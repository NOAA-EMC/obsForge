import logging
import hashlib
from typing import Optional, List, Dict
from sqlalchemy.orm import Session
from .netcdf_structure_orm import (
    NetcdfStructureORM,
    NetcdfNodeORM,
    NetcdfStructureAttributeORM,
    NetcdfVariableDimensionORM,
)
from .netcdf_node import NetcdfNode, read_netcdf_nodes

import netCDF4

logger = logging.getLogger(__name__)


class NetcdfStructure:
    """
    Domain-level representation of a NetCDF structure (skeleton).
    """

    def __init__(
        self,
        nodes: List[NetcdfNode],
        structure_hash: str,
        id: Optional[int] = None
    ):
        self.nodes = nodes
        self.structure_hash = structure_hash
        self.id = id

        # Assign back-reference for each node
        for n in nodes:
            n.structure = self

    def __repr__(self):
        return (
            f"<NetcdfStructure id={self.id}, "
            f"{len(self.nodes)} nodes, "
            f"hash={self.structure_hash[:10]}..>"
        )

    @classmethod
    def from_file(cls, file_path: str) -> "NetcdfStructure":
        """
        Reads a NetCDF file and returns a NetcdfStructure object.
        - Uses read_netcdf_nodes to build nodes in memory.
        - Computes a deterministic hash.
        - Assigns back-reference of structure to each node.
        """
        nodes = read_netcdf_nodes(file_path, read_values=False)
        if not nodes:
            logger.debug(f"No nodes found in {file_path}")
            # logger.error(f"Failed to read NetCDF structure from {file_path}")
            return None

        structure_hash = cls._generate_hash(nodes)

        structure = cls(nodes=nodes, structure_hash=structure_hash)

        return structure

    @staticmethod
    def _generate_hash(nodes: List[NetcdfNode]) -> str:
        sorted_nodes = sorted(nodes, key=lambda n: n.path)
        components = []
        for n in sorted_nodes:
            attr_str = ",".join(sorted(n.attr_names))
            dim_str = ",".join(n.dims)
            components.append(f"{n.path}|{n.node_type}|{n.dtype}|{dim_str}|{attr_str}")
        return hashlib.sha256("::".join(components).encode()).hexdigest()

    def to_db(self, session: Session) -> NetcdfStructureORM:
        """
        Persist this structure to the DB. Idempotent.
        Delegates node persistence to NetcdfNode.to_db.
        """
        # Check if structure already exists
        existing = session.query(NetcdfStructureORM).filter_by(
            structure_hash=self.structure_hash
        ).first()

        if existing:
            self.id = existing.id
            logger.debug(f"Structure exists in DB (id={self.id})")
            # Update all nodes with DB state
            for node in self.nodes:
                node.to_db(session)
            return existing

        # Create new structure row
        struct_orm = NetcdfStructureORM(structure_hash=self.structure_hash)
        session.add(struct_orm)
        session.flush()
        self.id = struct_orm.id

        # Persist nodes using their own to_db
        for node in self.nodes:
            node.to_db(session)

        return struct_orm
