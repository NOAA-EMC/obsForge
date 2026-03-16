import logging
from typing import List, Dict, Optional, Any, TYPE_CHECKING
import netCDF4
import numpy as np

from sqlalchemy.orm import Session
# from .netcdf_structure_orm import NetcdfNodeORM, NetcdfStructureAttributeORM
from .netcdf_structure_orm import NetcdfNodeORM

# Prevents circular imports for type hinting
if TYPE_CHECKING:
    from .netcdf_structure import NetcdfStructure

logger = logging.getLogger(__name__)



class NetcdfNode:
    """
    Domain object representing a Group, Variable, or Dimension in a NetCDF.
    """

    def __init__(
        self, 
        full_path: str, 
        node_type: str, 
        dtype: Optional[str] = None
    ):
        # Identity
        self.full_path = full_path
        self.node_type = node_type.upper()  # 'GROUP', 'VARIABLE', 'DIMENSION'
        self.dtype = dtype
        self.id: Optional[int] = None  # Populated after to_db()

        # Hierarchy & Pointers
        self.structure: Optional["NetcdfStructure"] = None
        self.parent_node: Optional["NetcdfNode"] = None
        self.children: List["NetcdfNode"] = []

        # Data
        self.attributes: Dict[str, Any] = {}
        self.dim_names: List[str] = []  # Names of dimensions (for VARIABLE nodes)
        # logger.info(f"--> created {self}")

    def add_child(self, child: "NetcdfNode") -> None:
        """Establishes tree hierarchy."""
        child.parent_node = self
        self.children.append(child)

    def get_structural_identity(self) -> Dict[str, Any]:
        """
        Returns the core characteristics that define this node's structure.
        Used for hashing the NetcdfStructure.
        """
        return {
            "path": self.full_path,
            "type": self.node_type,
            "dtype": self.dtype,
            "dims": self.dim_names,
            "attr_names": sorted(self.attributes.keys())
        }

    def to_db(self, session: Session) -> NetcdfNodeORM:
        """
        Syncs the in-memory node with the database.
        If it exists, populates self.id. If not, inserts it.
        Also persists attribute definitions for this node.
        """

        from .netcdf_structure_orm import NetcdfStructureAttributeORM

        if not self.structure or self.structure.id is None:
            logger.error(
                f"Cannot persist node '{self.full_path}' without a persisted NetcdfStructure."
            )
            return None

        # -------------------------------------------------
        # 1. Look for existing node within this structure
        # -------------------------------------------------
        node_orm = (
            session.query(NetcdfNodeORM)
            .filter_by(
                structure_id=self.structure.id,
                full_path=self.full_path
            )
            .first()
        )

        # -------------------------------------------------
        # 2. Insert if missing
        # -------------------------------------------------
        if not node_orm:
            node_orm = NetcdfNodeORM(
                structure_id=self.structure.id,
                full_path=self.full_path,
                node_type=self.node_type,
                dtype=self.dtype,
            )
            session.add(node_orm)
            session.flush()  # ensure id is generated

        # -------------------------------------------------
        # 3. Sync ID back to domain object
        # -------------------------------------------------
        self.id = node_orm.id

        # -------------------------------------------------
        # 4. Persist attribute definitions
        # -------------------------------------------------
        if self.attributes:

            for attr_name in self.attributes.keys():

                exists = (
                    session.query(NetcdfStructureAttributeORM)
                    .filter_by(
                        node_id=self.id,
                        attr_name=attr_name
                    )
                    .first()
                )

                if not exists:
                    session.add(
                        NetcdfStructureAttributeORM(
                            node_id=self.id,
                            attr_name=attr_name
                        )
                    )

        return node_orm




    def old_to_db(self, session: Session) -> NetcdfNodeORM:
        """
        Syncs the in-memory node with the database. 
        If it exists, populates self.id. If not, inserts it.
        """
        if not self.structure or self.structure.id is None:
            # raise ValueError(
            logger.error(
                f"Cannot persist node '{self.full_path}' without a persisted NetcdfStructure."
            )
            return None

        # 1. Look for existing node within this specific structure
        node_orm = (
            session.query(NetcdfNodeORM)
            .filter_by(
                structure_id=self.structure.id, 
                full_path=self.full_path
            )
            .first()
        )

        # 2. Insert if it doesn't exist
        if not node_orm:
            node_orm = NetcdfNodeORM(
                structure_id=self.structure.id,
                full_path=self.full_path,
                node_type=self.node_type,
                dtype=self.dtype,
            )
            session.add(node_orm)
            # Flush so the DB generates an ID for us immediately
            session.flush()

        # 3. Sync the ID back to the domain object
        self.id = node_orm.id
        
        return node_orm

    @property
    def name(self) -> str:
        """Leaf name of the node (e.g., /Group/Temp -> Temp)."""
        if self.full_path == "/":
            return "root"
        return self.full_path.rstrip("/").split("/")[-1]

    def __repr__(self) -> str:
        return f"<NetcdfNode({self.node_type}): {self.full_path} (id={self.id})>"

    def to_dict(self) -> Dict[str, Any]:
        """Recursive serialization of the node and its subtree."""
        data = {
            "path": self.full_path,
            "type": self.node_type,
            "dtype": self.dtype,
            "attributes": sorted(self.attributes.keys()),
        }
        
        if self.node_type == "VARIABLE":
            data["dimensions"] = self.dim_names
            
        if self.children:
            # Sort children by path to keep the JSON output deterministic
            data["children"] = [
                child.to_dict() 
                for child in sorted(self.children, key=lambda x: x.full_path)
            ]
            
        return data
