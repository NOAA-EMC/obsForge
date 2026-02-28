import logging
import hashlib
from typing import Optional
import netCDF4 as nc
from sqlalchemy.orm import Session

from .netcdf_structure_orm import (
    NetcdfStructureORM, 
    NetcdfNodeORM, 
    NetcdfStructureAttributeORM, 
    netcdf_variable_dimensions
)

logger = logging.getLogger(__name__)


class NetcdfStructureNode:
    """
    Domain-level representation of a node (variable, dimension, group)
    in a NetCDF structure.
    """
    def __init__(self, path: str, node_type: str, dtype: str = None,
                 dims: list[str] = None, attr_names: list[str] = None):
        self.path = path
        self.node_type = node_type  # 'GROUP', 'VARIABLE', 'DIMENSION'
        self.dtype = dtype
        self.dims = dims or []
        self.attr_names = attr_names or []

    def __repr__(self):
        return f"<NetcdfStructureNode(path={self.path}, type={self.node_type})>"


    def to_db(self, session: Session, structure_id: int):
        node_orm = NetcdfNodeORM(
            structure_id=structure_id,
            full_path=self.path,
            node_type=self.node_type,
            dtype=self.dtype
        )

        session.add(node_orm)
        session.flush()

        self.id = node_orm.id

        # Persist attribute definitions (names only)
        for attr_name in self.attr_names:
            session.add(
                NetcdfStructureAttributeORM(
                    node_id=node_orm.id,
                    attr_name=attr_name
                )
            )

        return node_orm



class NetcdfStructure:
    def __init__(
        self, nodes: list["NetcdfStructureNode"], 
        structure_hash: str, 
        id: Optional[int] = None
    ):
        self.nodes = nodes  # list of NetcdfStructureNode
        self.structure_hash = structure_hash
        self.id = id

    def __repr__(self):
        return f"<NetcdfStructure id={self.id}, hash={self.structure_hash[:10]}...>"

    @classmethod
    def from_file(cls, file_path: str) -> Optional["NetcdfStructure"]:
        try:
            import netCDF4 as nc
            with nc.Dataset(file_path, 'r') as ds:
                nodes = cls._scan_structure(ds)
                if not nodes:
                    return None
                structure_hash = cls._generate_hash(nodes)
                return cls(nodes=nodes, structure_hash=structure_hash)
        except Exception as e:
            logger.debug(f"Failed to read structure from {file_path}: {e}")
            return None

    @staticmethod
    def _scan_structure(ds):

        nodes_dict = {}

        # Dimensions
        for name in ds.dimensions:
            nodes_dict[name] = NetcdfStructureNode(
                path=name,
                node_type="DIMENSION"
            )

        def walk(group, prefix=""):
            group_path = prefix if prefix else "/"

            if group_path not in nodes_dict:
                nodes_dict[group_path] = NetcdfStructureNode(
                    path=group_path,
                    node_type="GROUP",
                    attr_names=group.ncattrs()
                )

            for name, var in group.variables.items():
                full_path = f"{prefix}{name}"
                nodes_dict[full_path] = NetcdfStructureNode(
                    path=full_path,
                    node_type="VARIABLE",
                    dtype=str(var.dtype),
                    dims=list(var.dimensions),
                    attr_names=var.ncattrs()
                )

            for name, grp in group.groups.items():
                walk(grp, f"{prefix}{name}/")

        walk(ds)

        return list(nodes_dict.values())


    @staticmethod
    def bad_scan_structure(ds) -> list["NetcdfStructureNode"]:
        nodes = []

        # 1. Dimensions
        for name in ds.dimensions:
            nodes.append(
                NetcdfStructureNode(
                    path=name,
                    node_type='DIMENSION'
                )
            )

        # 2. Recursive walk for groups & variables
        def walk(group, prefix=""):
            group_path = prefix if prefix else "/"
            # Group node
            nodes.append(
                NetcdfStructureNode(
                    path=group_path,
                    node_type='GROUP',
                    attr_names=group.ncattrs()
                )
            )

            # Variables
            for name, var in group.variables.items():
                full_path = f"{prefix}{name}"
                nodes.append(
                    NetcdfStructureNode(
                        path=full_path,
                        node_type='VARIABLE',
                        dtype=str(var.dtype),
                        dims=list(var.dimensions),
                        attr_names=var.ncattrs()
                    )
                )

            # Recurse into sub-groups
            for name, grp in group.groups.items():
                walk(grp, f"{prefix}{name}/")

        walk(ds)
        return nodes

    @staticmethod
    def _generate_hash(nodes: list["NetcdfStructureNode"]) -> str:
        # deterministic sorting
        sorted_nodes = sorted(nodes, key=lambda n: n.path)
        components = []
        for n in sorted_nodes:
            attr_str = ",".join(sorted(n.attr_names))
            dim_str = ",".join(n.dims)
            components.append(f"{n.path}|{n.node_type}|{n.dtype}|{dim_str}|{attr_str}")
        import hashlib
        return hashlib.sha256("::".join(components).encode()).hexdigest()

    def to_db(self, session: Session) -> NetcdfStructureORM:
        """
        Persist this structure blueprint if not already present.
        Returns the ORM object.
        Idempotent.
        """

        # Already persisted in this session
        if self.id is not None:
            existing = session.get(NetcdfStructureORM, self.id)
            if existing:
                return existing

        # Check by hash
        existing = session.query(NetcdfStructureORM).filter_by(
            structure_hash=self.structure_hash
        ).first()

        if existing:
            self.id = existing.id
            return existing

        # Create new structure row
        struct = NetcdfStructureORM(structure_hash=self.structure_hash)
        session.add(struct)
        session.flush()

        self.id = struct.id

        # Persist nodes
        node_map = {}

        for node in self.nodes:
            node_orm = node.to_db(session, structure_id=self.id)
            node_map[node.path] = node_orm

        # Link variable → dimensions
        for node in self.nodes:
            if node.node_type == "VARIABLE" and node.dims:
                var_node = node_map[node.path]

                for idx, dim_name in enumerate(node.dims):
                    dim_node = node_map.get(dim_name)
                    if dim_node:
                        stmt = netcdf_variable_dimensions.insert().values(
                            variable_node_id=var_node.id,
                            dimension_node_id=dim_node.id,
                            dim_index=idx
                        )
                        session.execute(stmt)

        session.flush()
        return struct
