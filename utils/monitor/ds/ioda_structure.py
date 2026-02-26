import logging
import hashlib
from typing import Optional
import netCDF4 as nc
from sqlalchemy.orm import Session

from .ioda_structure_orm import (
    IodaStructureORM, 
    IodaNodeORM, 
    IodaStructureAttributeORM, 
    ioda_variable_dimensions
)

logger = logging.getLogger(__name__)


class IodaStructure:
    def __init__(self, 
        nodes_info: list, 
        structure_hash: str, 
        id: Optional[int] = None
    ):
        # The internal state
        self.nodes_info = nodes_info
        self.structure_hash = structure_hash
        self.id = id

    def __repr__(self) -> str:
        return f"IodaStructure id = {self.id}, {self.structure_hash}"

    @classmethod
    def from_file(cls, file_path: str) -> Optional["IodaStructure"]:
        """
        Returns an instance of IodaStructure with the hash and nodes populated.
        Returns None if corrupted/unreadable.
        """
        try:
            with nc.Dataset(file_path, 'r') as ds:
                # Use your existing _scan_structure
                nodes_info = cls._scan_structure(ds)
                
                if nodes_info is None:
                    return None
                    
                structure_hash = cls._generate_hash(nodes_info)
                
                this_ioda_structure = cls(nodes_info=nodes_info, structure_hash=structure_hash)
                # logger.debug(f"constructed {this_ioda_structure} from {file_path}")
                return this_ioda_structure
        except Exception as e:
            logger.debug(f"Failed to read structure from {file_path}: {e}")
            return None

    @staticmethod
    def _scan_structure(ds):
        """
        Walks the file to extract the skeleton and attribute NAMES.
        Returns a list of node dictionaries or None if the scan fails.
        """
        nodes_dict = {}

        try:
            # 1. Dimensions
            for name in ds.dimensions:
                nodes_dict[name] = {
                    'path': name,
                    'node_type': 'DIMENSION',
                    'dtype': None,
                    'dims': [],
                    'attr_names': []
                }

            # 2. Recursive Walk
            def walk(group, prefix=""):
                # Global/Group Attributes
                group_path = prefix if prefix else "/"
                if group_path not in nodes_dict:
                    nodes_dict[group_path] = {
                        'path': group_path,
                        'node_type': 'GROUP',
                        'dtype': None,
                        'dims': [],
                        'attr_names': group.ncattrs()
                    }

                # Variables
                for name, var in group.variables.items():
                    full_path = f"{prefix}{name}"
                    nodes_dict[full_path] = {
                        'path': full_path,
                        'node_type': 'VARIABLE',
                        'dtype': str(var.dtype),
                        'dims': list(var.dimensions),
                        'attr_names': var.ncattrs()
                    }

                for name, grp in group.groups.items():
                    walk(grp, f"{prefix}{name}/")

            walk(ds)
            return list(nodes_dict.values())
            
        except Exception as e:
            logger.debug(f"Internal structure scan failed: {e}")
            return None

    @staticmethod
    def _generate_hash(nodes_info):
        """Hash includes paths, types, dtypes, dims, and ATTRIBUTE NAMES."""
        sorted_nodes = sorted(nodes_info, key=lambda x: x['path'])
        components = []
        for n in sorted_nodes:
            # Sort attribute names to ensure deterministic hashing
            attr_str = ",".join(sorted(n['attr_names']))
            dim_str = ",".join(n['dims'])
            components.append(f"{n['path']}|{n['node_type']}|{n['dtype']}|{dim_str}|{attr_str}")
            
        return hashlib.sha256("::".join(components).encode()).hexdigest()


    # to be deprecated?
    @classmethod
    def get_or_create_id(cls, file_path, session):
        struct_obj = cls.from_file(file_path)
        if struct_obj is None:
            return None
        return struct_obj.to_db(session)


    # PERSISTANCE

    def to_db(self, session: Session) -> int:
        # Check if hash exists
        existing = session.query(IodaStructureORM.id).filter_by(
            structure_hash=self.structure_hash
        ).first()
        
        if existing:
            self.id = existing[0]
            return self.id

        self.id = self._to_db_structure(self.nodes_info, self.structure_hash, session)
        # logger.debug(f"to_db {self}")
        return self.id

    @classmethod
    def _to_db_structure(cls, nodes_info, structure_hash, session):
        # 1. Create Skeleton
        struct = IodaStructureORM(structure_hash=structure_hash)
        session.add(struct)
        session.flush()

        node_map = {}
        # 2. Create Nodes and their Attribute Definitions
        for info in nodes_info:
            node = IodaNodeORM(
                structure_id=struct.id,
                full_path=info['path'],
                node_type=info['node_type'],
                dtype=info['dtype']
            )
            session.add(node)
            session.flush()
            node_map[info['path']] = node

            # Register the EXISTENCE of attributes for this node
            for attr_name in info['attr_names']:
                session.add(IodaStructureAttributeORM(
                    node_id=node.id, 
                    attr_name=attr_name
                ))

        # 3. Link Dimensions (Manual insertion as before)
        for info in nodes_info:
            if info['node_type'] == 'VARIABLE' and info['dims']:
                var_node = node_map[info['path']]
                for idx, dim_name in enumerate(info['dims']):
                    dim_node = node_map.get(dim_name)
                    if dim_node:
                        stmt = ioda_variable_dimensions.insert().values(
                            variable_node_id=var_node.id,
                            dimension_node_id=dim_node.id,
                            dim_index=idx
                        )
                        session.execute(stmt)
        
        session.commit()
        return struct.id
