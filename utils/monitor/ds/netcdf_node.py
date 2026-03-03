import logging
from typing import Optional, List, Dict, Any
import netCDF4
import numpy as np

from sqlalchemy.orm import Session
from .netcdf_structure_orm import NetcdfNodeORM, NetcdfStructureAttributeORM

logger = logging.getLogger(__name__)


class NetcdfNode:
    """
    Represents a node in a NetCDF file or structure.
    Can be used for:
      - Structure skeleton: no attribute values
      - File instance: with attribute values
    """

    def __init__(
        self,
        path: str,
        node_type: str,
        dtype: Optional[str] = None,
        dims: Optional[List[str]] = None,
        attr_names: Optional[List[str]] = None,
        attr_values: Optional[Dict[str, Any]] = None,
        structure: Optional["NetcdfStructure"] = None,
        id: Optional[int] = None,
    ):
        self.path = path
        self.node_type = node_type
        self.dtype = dtype
        self.dims = dims or []
        self.attr_names = attr_names or []
        self.attr_values = attr_values or {}  # only for file instances
        self.structure = structure
        self.id = id

    def __repr__(self):
        return (
            f"<NetcdfNode(path='{self.path}', type={self.node_type}, "
            f"dtype={self.dtype}, attrs={len(self.attr_names)})>"
        )

    def to_db(self, session: Session) -> NetcdfNodeORM:
        """
        Persist this node to the DB.
        Idempotent: inserts if missing, otherwise updates type/dtype.
        Also persists attribute names (no values).
        Returns the Node ORM object.
        """
        if self.structure is None or self.structure.id is None:
            raise ValueError("Node must belong to a persisted structure to persist itself.")

        # Check if node already exists in DB
        node_orm = session.query(NetcdfNodeORM).filter_by(
            structure_id=self.structure.id,
            full_path=self.path,
            node_type=self.node_type
        ).first()

        if not node_orm:
            logger.debug(f"[NODE] Inserting new node: {self.path}")
            node_orm = NetcdfNodeORM(
                structure_id=self.structure.id,
                full_path=self.path,
                node_type=self.node_type,
                dtype=self.dtype
            )
            session.add(node_orm)
            session.flush()
        else:
            logger.debug(f"[NODE] Node already exists in DB: {self.path} (id={node_orm.id})")

        self.id = node_orm.id

        # Persist attribute definitions (names only)
        for attr_name in self.attr_names:
            attr_orm = session.query(NetcdfStructureAttributeORM).filter_by(
                node_id=node_orm.id,
                attr_name=attr_name
            ).first()
            if not attr_orm:
                session.add(
                    NetcdfStructureAttributeORM(
                        node_id=node_orm.id,
                        attr_name=attr_name
                    )
                )

        session.flush()
        return node_orm


def json_safe(value):
    """Convert value to JSON-safe Python type."""
    if isinstance(value, (np.integer,)):
        return int(value)
    elif isinstance(value, (np.floating,)):
        return float(value)
    elif isinstance(value, (np.bool_, bool)):
        return bool(value)
    elif isinstance(value, np.ndarray):
        return value.tolist()
    elif value is None:
        return None
    else:
        return value


def read_netcdf_nodes(file_path: str, read_values: bool = False) -> List[NetcdfNode]:
    """
    Scan a NetCDF file and produce a list of NetcdfNode objects.
    
    Args:
        file_path: Path to the NetCDF file.
        read_values: If True, also read attribute values (for file).
                     If False, only read metadata (for structure).
    
    Returns:
        List of NetcdfNode objects.
    """
    nodes: List[NetcdfNode] = []

    with netCDF4.Dataset(file_path, "r") as ds:

        # ---- Global / root node ----
        global_attrs = ds.ncattrs()
        attr_values = {}
        if read_values:
            for attr_name in global_attrs:
                attr_values[attr_name] = json_safe(ds.getncattr(attr_name))

        nodes.append(
            NetcdfNode(
                path="/",
                node_type="GROUP",
                dtype=None,
                dims=[],
                attr_names=list(global_attrs),
                attr_values=attr_values,
            )
        )

        # ---- Variables ----
        for var_name, var in ds.variables.items():
            attr_names = var.ncattrs()
            attr_values = {}
            if read_values:
                for attr_name in attr_names:
                    attr_values[attr_name] = json_safe(var.getncattr(attr_name))

            nodes.append(
                NetcdfNode(
                    path=var_name,
                    node_type="VARIABLE",
                    dtype=str(var.dtype),
                    dims=list(var.dimensions),
                    attr_names=list(attr_names),
                    attr_values=attr_values,
                )
            )

        # ---- Dimensions ----
        for dim_name, dim in ds.dimensions.items():
            nodes.append(
                NetcdfNode(
                    path=dim_name,
                    node_type="DIMENSION",
                    dtype=None,
                    dims=[],
                    attr_names=[],
                    attr_values={} if read_values else {},
                )
            )

    return nodes


def get_dim_node_for_var(
    var_node: NetcdfNode, 
    dim_name: str, 
    all_nodes: List[NetcdfNode]
) -> Optional[NetcdfNode]:
    """
    Return the NetcdfNode that corresponds to a dimension of a variable node by name.
    """
    for node in all_nodes:
        if node.node_type == "DIMENSION" and node.path == dim_name:
            return node
    return None
