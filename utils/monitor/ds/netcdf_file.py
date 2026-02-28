import logging
import numpy as np

import netCDF4
import numpy as np
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session

from .netcdf_file_orm import (
    NetcdfFileAttributeORM,
    NetcdfFileDerivedAttributeORM,
)

logger = logging.getLogger(__name__)


class DerivedAttributeRegistry:
    """
    Registry of derived numeric attributes computed on numeric arrays.
    Easily extensible.
    """

    def __init__(self):
        self._attributes = {}

    def register(self, name, func):
        self._attributes[name] = func

    def compute_attributes(self, array):
        return {
            name: func(array)
            for name, func in self._attributes.items()
        }

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


class NetcdfFile:
    """
    Domain object representing a physical NetCDF file.

    Responsibilities:
        - Read static file attributes
        - Compute derived numeric attributes
        - Persist both when requested

    Computation does NOT require DB persistence.
    """

    def __init__(
        self,
        file,
        structure=None,
        derived_attribute_registry: Optional[DerivedAttributeRegistry] = None,
    ):
        self.file = file
        self.structure = structure
        self.derived_attribute_registry = (
            derived_attribute_registry or DerivedAttributeRegistry.default()
        )

        # In-memory state
        self.attributes: Dict[Any, Any] = {}
        self.derived_attributes: Dict[Any, Dict[str, float]] = {}

    def to_db(self, session: Session) -> None:
        self.to_db_attributes(session)
        self.to_db_derived_attributes(session)

    # ==========================================================
    # 1️⃣ READ ATTRIBUTES (metadata)
    # ==========================================================

    def read_attributes(self) -> None:
        """
        Read global + variable attributes into self.attributes.

        Keys:
            - If structure exists → struct_attr_id
            - Else → (scope, attr_name) tuples
        """

        self.attributes = {}

        with netCDF4.Dataset(self.file.path, "r") as ds:

            # ---- Global attributes ----
            for attr_name in ds.ncattrs():
                value = ds.getncattr(attr_name)

                if self.structure:
                    struct_attr = next(
                        (
                            a for a in self.structure.structure_attributes
                            if a.node_id is None and a.attr_name == attr_name
                        ),
                        None,
                    )
                    if struct_attr:
                        self.attributes[struct_attr.id] = value
                else:
                    self.attributes[("GLOBAL", attr_name)] = value

            # ---- Variable attributes ----
            for var_name, var in ds.variables.items():

                node_id = None
                if self.structure:
                    node = next(
                        (
                            n for n in self.structure.nodes
                            if n.full_path == var_name
                        ),
                        None,
                    )
                    if node:
                        node_id = node.id

                for attr_name in var.ncattrs():
                    value = var.getncattr(attr_name)

                    if self.structure and node_id:
                        struct_attr = next(
                            (
                                a for a in self.structure.structure_attributes
                                if a.node_id == node_id
                                and a.attr_name == attr_name
                            ),
                            None,
                        )
                        if struct_attr:
                            self.attributes[struct_attr.id] = value
                    else:
                        self.attributes[(var_name, attr_name)] = value

    # ==========================================================
    # 2️⃣ PERSIST ATTRIBUTES
    # ==========================================================

    def to_db_attributes(self, session: Session) -> None:
        """
        Persist file attributes into netcdf_file_attributes table.
        """

        # if not self.structure:
            # raise ValueError("Structure required for DB persistence.")

        # if self.file.id is None:
            # raise ValueError("File must be persisted before saving attributes.")

        if not self.attributes:
            logger.debug(f"to_db_attributes: empty for {self.file.path}")
            return

        session.query(NetcdfFileAttributeORM).filter(
            NetcdfFileAttributeORM.file_id == self.file.id
        ).delete()

        for struct_attr_id, value in self.attributes.items():
            session.add(
                NetcdfFileAttributeORM(
                    file_id=self.file.id,
                    struct_attr_id=struct_attr_id,
                    attr_value=value,
                )
            )

        session.flush()

    # ==========================================================
    # 3️⃣ COMPUTE DERIVED ATTRIBUTES
    # ==========================================================

    def compute_derived_attributes(self) -> None:
        """
        Compute numeric derived attributes for all numeric variables.
        Results stored in self.derived_attributes.

        Keys:
            - If structure exists → node_id
            - Else → variable name
        """

        self.derived_attributes = {}

        with netCDF4.Dataset(self.file.path, "r") as ds:

            for var_name, var in ds.variables.items():

                data = var[:]

                if not np.issubdtype(data.dtype, np.number):
                    continue

                if isinstance(data, np.ma.MaskedArray):
                    clean = data.compressed()
                    n_missing = int(data.mask.sum())
                else:
                    clean = data.ravel()
                    n_missing = 0

                if clean.size == 0:
                    continue

                attributes = self.derived_attribute_registry.compute_attributes(clean)
                attributes["n_missing"] = n_missing

                if self.structure:
                    node = next(
                        (
                            n for n in self.structure.nodes
                            if n.full_path == var_name
                        ),
                        None,
                    )
                    if not node:
                        continue
                    self.derived_attributes[node.id] = attributes
                else:
                    self.derived_attributes[var_name] = attributes

    # ==========================================================
    # 4️⃣ PERSIST DERIVED ATTRIBUTES
    # ==========================================================

    def to_db_derived_attributes(self, session: Session) -> None:
        """
        Persist derived attributes into netcdf_file_derived_attributes.
        """

        # if not self.structure:
            # raise ValueError("Structure required for DB persistence.")

        # if self.file.id is None:
            # raise ValueError(
                # "File must be persisted before saving derived attributes."
            # )

        if not self.derived_attributes:
            logger.debug(f"to_db_derived_attributes: empty for {self.file.path}")
            return

        session.query(NetcdfFileDerivedAttributeORM).filter(
            NetcdfFileDerivedAttributeORM.file_id == self.file.id
        ).delete()

        for node_id, attributes in self.derived_attributes.items():
            for name, value in attributes.items():
                session.add(
                    NetcdfFileDerivedAttributeORM(
                        file_id=self.file.id,
                        netcdf_node_id=node_id,
                        name=name,
                        value=float(value),
                    )
                )

        session.flush()
