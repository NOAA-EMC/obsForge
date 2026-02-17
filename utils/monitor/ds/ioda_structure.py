import hashlib
import netCDF4 as nc
from .ioda_structure_orm import (
    IodaStructureORM, 
    IodaNodeORM, 
    IodaStructureAttributeORM, 
    ioda_variable_dimensions
)

class IodaStructure:
    @classmethod
    def get_or_create_id(cls, file_path, session):
        with nc.Dataset(file_path, 'r') as ds:
            # We now only care about the NAMES of attributes for the blueprint
            nodes_info = cls._scan_structure(ds)
            fingerprint = cls._generate_hash(nodes_info)

        existing = session.query(IodaStructureORM.id).filter_by(structure_hash=fingerprint).first()
        if existing:
            return existing[0]

        return cls._register_new_structure(nodes_info, fingerprint, session)

    @staticmethod
    def _scan_structure(ds):
        """Walks the file to extract the skeleton and attribute NAMES."""
        nodes_dict = {}

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
                    'attr_names': var.ncattrs() # Just the keys
                }

            for name, grp in group.groups.items():
                walk(grp, f"{prefix}{name}/")

        walk(ds)
        return list(nodes_dict.values())

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

    @classmethod
    def _register_new_structure(cls, nodes_info, fingerprint, session):
        # 1. Create Skeleton
        struct = IodaStructureORM(structure_hash=fingerprint)
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
