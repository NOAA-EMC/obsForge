import logging
from typing import Dict, Any, Optional
import netCDF4
import numpy as np
from sqlalchemy.orm import Session

from .netcdf_structure_orm import NetcdfStructureAttributeORM
from .netcdf_file_orm import (
    NetcdfFileAttributeORM,
    NetcdfFileDerivedAttributeORM,
)

from .netcdf_node import NetcdfNode, read_netcdf_nodes

logger = logging.getLogger(__name__)


class DerivedAttributeRegistry:
    """Registry of derived numeric attributes for variables."""

    def __init__(self):
        self._attributes = {}

    def register(self, name, func):
        self._attributes[name] = func

    def compute_attributes(self, array):
        return {name: func(array) for name, func in self._attributes.items()}

    @classmethod
    def default(cls):
        registry = cls()
        registry.register("min", lambda x: float(np.min(x)))
        registry.register("max", lambda x: float(np.max(x)))
        registry.register("mean", lambda x: float(np.mean(x)))
        registry.register("std_dev", lambda x: float(np.std(x)))
        registry.register("median", lambda x: float(np.median(x)))
        registry.register("nobs", lambda x: int(x.size))
        return registry

class DerivedAttribute:
    def __init__(self, name: str, value: float):
        self.name = name
        self.value = value

class NetcdfFile:
    """
    Domain object representing a physical NetCDF file.
    Reads attributes and computes derived numeric attributes.
    """

    def __init__(
        self,
        file,
        structure: Optional["NetcdfStructure"] = None,
        derived_attribute_registry: Optional[DerivedAttributeRegistry] = None,
    ):
        self.file = file
        self.structure = structure
        self.derived_attribute_registry = (
            derived_attribute_registry or DerivedAttributeRegistry.default()
        )

        self.nodes: List[NetcdfNode] = []
        # for each node store a list of derived attributes
        self.derived_attributes: List[List[DerivedAttribute]]

    def read_attributes(self) -> None:
        self.nodes = read_netcdf_nodes(self.file.path, read_values=True)
        for node in self.nodes:
            node.structure = self.structure

    def to_db_attributes(self, session: Session) -> None:
        """
        Persist file attribute values into netcdf_file_attributes.

        Assumes:
        - self.file.id is already set
        - self.nodes were created with read_values=True
        - structure and nodes are already persisted or will be created here
        """

        if not self.nodes:
            return

        # Remove existing file attributes (idempotent behavior)
        session.query(NetcdfFileAttributeORM).filter(
            NetcdfFileAttributeORM.file_id == self.file.id
        ).delete()

        for node in self.nodes:
            # Ensure node + structure attributes exist
            node.to_db(session)

            if not node.attr_values:
                continue

            for attr_name, value in node.attr_values.items():

                # Resolve structure attribute id (node_id + attr_name)
                struct_attr = session.query(NetcdfStructureAttributeORM).filter_by(
                    node_id=node.id,
                    attr_name=attr_name
                ).first()

                session.add(
                    NetcdfFileAttributeORM(
                        file_id=self.file.id,
                        struct_attr_id=struct_attr.id,
                        attr_value=value,
                    )
                )

        session.flush()

    def compute_derived_attributes(self) -> None:
        """
        Compute numeric derived attributes for all VARIABLE nodes.
        """
        self.derived_attributes = []

        with netCDF4.Dataset(self.file.path, "r") as ds:

            for node in self.nodes:

                # Only VARIABLE nodes can have numeric data
                if node.node_type != "VARIABLE":
                    self.derived_attributes.append([])
                    continue

                var = ds.variables.get(node.path)

                data = var[:]

                if not np.issubdtype(data.dtype, np.number):
                    self.derived_attributes.append([])
                    continue

                if isinstance(data, np.ma.MaskedArray):
                    clean = data.compressed()
                    n_missing = int(data.mask.sum())
                else:
                    clean = data.ravel()
                    n_missing = 0

                if clean.size == 0:
                    self.derived_attributes.append([])
                    continue

                computed = self.derived_attribute_registry.compute_attributes(clean)
                computed["n_missing"] = n_missing

                node_attrs = [
                    DerivedAttribute(name, float(value))
                    for name, value in computed.items()
                ]

                self.derived_attributes.append(node_attrs)

    def to_db_derived_attributes(self, session: Session) -> None:
        """
        Persist computed derived attributes for each node in self.nodes.
        Assumes the file and nodes are already persisted (have IDs).
        """
        if not self.derived_attributes:
            logger.debug(f"No derived attributes to persist for {self.file.path}")
            return

        # Remove any existing derived attributes for this file
        session.query(NetcdfFileDerivedAttributeORM).filter(
            NetcdfFileDerivedAttributeORM.file_id == self.file.id
        ).delete()

        # Persist derived attributes node by node
        for node, derived_list in zip(self.nodes, self.derived_attributes):
            if node.id is None:
                logger.warning(f"Skipping derived attributes for unpersisted node {node.path}")
                continue

            for derived_attr in derived_list:
                session.add(
                    NetcdfFileDerivedAttributeORM(
                        file_id=self.file.id,
                        netcdf_node_id=node.id,
                        name=derived_attr.name,
                        value=float(derived_attr.value),
                    )
                )

        session.flush()


    def to_db(self, session: Session) -> None:
        self.to_db_attributes(session)
        self.to_db_derived_attributes(session)
