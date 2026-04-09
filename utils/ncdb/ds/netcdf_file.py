import logging
import netCDF4
import numpy as np
from typing import Optional, Dict, Any, List
from sqlalchemy.orm import Session

from .file_orm import FileORM
from .netcdf_structure_orm import (
    NetcdfStructureAttributeORM,
    NetcdfNodeORM
)
from .netcdf_file_orm import (
    NetcdfFileAttributeORM,
    NetcdfFileDerivedAttributeORM,
)

from .netcdf_scanner import NetcdfScanner
from .netcdf_structure import NetcdfStructure
from .derived_attributes import DerivedAttributeRegistry
from .file import File

logger = logging.getLogger(__name__)


class NetcdfFile:
    def __init__(
        self,
        file: "File",
        structure: NetcdfStructure,
        attribute_values: Optional[Dict[str, Dict[str, Any]]] = None,
        derived_values: Optional[Dict[str, Dict[str, float]]] = None,
        registry: Optional[DerivedAttributeRegistry] = None,
    ):
        self.file = file
        self.structure = structure
        self.registry = registry or DerivedAttributeRegistry.default()
        
        # State is passed in or initialized as empty
        # path -> {attr_name: value}
        self.attribute_values = attribute_values or {}
        # path -> {metric_name: value}
        self.derived_values = derived_values or {}

    @classmethod
    def from_file(
        cls, 
        file_obj: "File", 
        structure: NetcdfStructure
    ) -> "NetcdfFile":
        instance = cls(file=file_obj, structure=structure)
        
        instance.read_attributes()
        instance.compute_derived_attributes()
        
        return instance

    @classmethod
    def from_orm(
        cls, 
        session: Session, 
        file_orm: FileORM, 
        structure: NetcdfStructure
    ) -> "NetcdfFile":
        # 1. Base identity
        file_domain = File.from_orm(file_orm)
        instance = cls(file=file_domain, structure=structure)

        # 2. Modular hydration
        instance.from_orm_attributes(session)
        instance.from_orm_derived_attributes(session)

        return instance

    def from_orm_attributes(self, session: Session) -> None:
        """Hydrates attribute_values from netcdf_file_attributes."""
        attr_data = (
            session.query(
                NetcdfNodeORM.full_path, 
                NetcdfStructureAttributeORM.attr_name, 
                NetcdfFileAttributeORM.attr_value
            )
            .join(NetcdfStructureAttributeORM, NetcdfFileAttributeORM.struct_attr_id == NetcdfStructureAttributeORM.id)
            .join(NetcdfNodeORM, NetcdfStructureAttributeORM.node_id == NetcdfNodeORM.id)
            .filter(NetcdfFileAttributeORM.file_id == self.file.id)
            .all()
        )
        
        for path, name, val in attr_data:
            self.attribute_values.setdefault(path, {})[name] = val

    def from_orm_derived_attributes(self, session: Session) -> None:
        """Hydrates derived_values from netcdf_file_derived_attributes."""
        derived_data = (
            session.query(
                NetcdfNodeORM.full_path, 
                NetcdfFileDerivedAttributeORM.name, 
                NetcdfFileDerivedAttributeORM.value
            )
            .join(NetcdfNodeORM, NetcdfFileDerivedAttributeORM.netcdf_node_id == NetcdfNodeORM.id)
            .filter(NetcdfFileDerivedAttributeORM.file_id == self.file.id)
            .all()
        )

        for path, name, val in derived_data:
            self.derived_values.setdefault(path, {})[name] = val

    def read_attributes(self) -> None:
        """
        Scans the file for actual attribute values using the Scanner.
        """
        if not self.file or not self.file.path:
            logger.error("NetcdfFile cannot read attributes: No file path provided.")
            return

        try:
            # Call scanner in 'read_values' mode
            _, registry = NetcdfScanner.scan(self.file.path, read_values=True)
            
            for path, node in registry.items():
                if node.attributes:
                    self.attribute_values[path] = node.attributes
            # logger.info(f"Read attributes for {len(self.attribute_values)} nodes in {self.file.path}")
        except Exception as e:
            logger.error(f"Error reading attributes from {self.file.path}: {e}")

    def compute_derived_attributes(self) -> None:
        if not self.structure:
            logger.error(
                f"Cannot compute derived attributes for {self.file.path}: No structure assigned."
            )
            return

        computed = 0
        try:
            with netCDF4.Dataset(self.file.path, "r") as ds:
                for path, node in self.structure.nodes_by_path.items():
                    if node.node_type != "VARIABLE":
                        continue

                    try:
                        # Remove leading slash
                        clean = path.lstrip("/")
                        parts = clean.split("/")
                        var_name = parts[-1]
                        group = ds

                        # Navigate groups if necessary
                        for g in parts[:-1]:
                            group = group.groups.get(g)
                            if group is None:
                                raise KeyError(f"group '{g}' not found")
                                # logger.error(f"group '{g}' not found")
                                # continue

                        var_obj = group.variables.get(var_name)
                        if var_obj is None:
                            logger.debug(f"Variable missing in file: {path}")
                            continue

                        data = var_obj[:]
                        if data is None:
                            logger.debug(f"Skipping {path} (not found in file)")

                        stats = self.registry.compute_for_array(data)
                        if stats:
                            self.derived_values[path] = stats
                            computed += 1
                            # logger.info(f"DERIVED for {path} in {self.file.path}")

                    except Exception as e:
                        logger.debug(f"Skipping {path}: {e}")

            # logger.info(
                # f"Computed derived stats for {computed} variables in {self.file.path}"
            # )

        except Exception as e:
            logger.error(
                f"Error computing derived stats for {self.file.path}: {e}"
            )

    def to_db(self, session: Session) -> None:
        # the file and the structure mustah ve been already
        # persisted before persisting the file
        self.structure.to_db(session) 
        self.file.to_db(session)
        
        if not self.file.id:
            logger.error(f"to_db failed: File ID missing for {self.file.path}")
            return

        session.flush() # Ensure IDs are populated from the DB for the next steps
        self.to_db_attributes(session)
        self.to_db_derived_attributes(session)

    def to_db_attributes(self, session: Session) -> None:
        """Persists metadata values to netcdf_file_attributes."""
        for path, attrs in self.attribute_values.items():
            node = self.structure.find_node(path)
            if not node or not node.id:
                logger.debug(f"Skipping attributes for {path}: Node not found in database.")
                continue

            # Map current node's structural attribute definitions to their IDs
            struct_attrs = session.query(NetcdfStructureAttributeORM).filter_by(node_id=node.id).all()
            attr_id_lookup = {sa.attr_name: sa.id for sa in struct_attrs}

            for name, val in attrs.items():
                try:
                    struct_attr_id = attr_id_lookup.get(name)
                    if struct_attr_id:
                        # Sync logic: check if value exists for this file + attribute definition
                        exists = session.query(NetcdfFileAttributeORM).filter_by(
                            file_id=self.file.id,
                            struct_attr_id=struct_attr_id
                        ).first()

                        if not exists:
                            session.add(NetcdfFileAttributeORM(
                                file_id=self.file.id,
                                struct_attr_id=struct_attr_id,
                                attr_value=val
                            ))
                except Exception as e:
                    logger.error(f"Failed to persist attribute '{name}' for node '{path}': {e}")

    def to_db_derived_attributes(self, session: Session) -> None:
        """Persists computed stats to netcdf_file_derived_attributes."""
        for path, stats in self.derived_values.items():
            node = self.structure.find_node(path)
            if not node or not node.id:
                logger.debug(f"Skipping derived stats for {path}: Node not found in database.")
                continue

            for name, val in stats.items():
                if val is None: continue # Skip failed computations
                try:
                    exists = session.query(NetcdfFileDerivedAttributeORM).filter_by(
                        file_id=self.file.id,
                        netcdf_node_id=node.id,
                        name=name
                    ).first()

                    if not exists:
                        session.add(NetcdfFileDerivedAttributeORM(
                            file_id=self.file.id,
                            netcdf_node_id=node.id,
                            name=name,
                            value=val
                        ))
                except Exception as e:
                    logger.error(f"Failed to persist derived stat '{name}' for node '{path}': {e}")

    def new_get_variable(self, path: str) -> np.ndarray:
        """
        Retrieves raw numeric data for a variable at the given path.
        """
        node = self.structure.find_node(path)
        if not node or node.node_type != "VARIABLE":
            raise ValueError(f"Path '{path}' is not a valid VARIABLE node")

        try:
            with netCDF4.Dataset(self.file.path, 'r') as ds:
                values = ds[path][:]
                return values

        except Exception as e:
            raise RuntimeError(
                f"Failed to read variable '{path}' from {self.file.path}: {e}"
            )

    def get_variable(self, path: str, filter_out_masked: bool = True) -> Optional[np.ndarray]:
        """
        Retrieves the numeric data for a variable at the given path.
        
        Parameters:
        - path: The path of the variable in the NetCDF file.
        - filter_out_masked: If True, mask out invalid values (NaNs or specific invalid values).
        
        Returns:
        - The variable data as a numpy array, or None if the variable cannot be found or read.
        """
        node = self.structure.find_node(path)
        if not node or node.node_type != "VARIABLE":
            logger.error(f"Cannot get variable: Path '{path}' is not a variable node.")
            return None

        try:
            with netCDF4.Dataset(self.file.path, 'r') as ds:
                clean_path = path.lstrip('/')

                if clean_path in ds.variables:
                    # Top-level variable access
                    values = ds.variables[clean_path][:]
                else:
                    # For nested variables, we try direct path indexing
                    try:
                        var_obj = ds[path]
                        values = var_obj[:]
                    except KeyError:
                        logger.error(f"Variable or Group hierarchy not found in file: {path}")
                        return None

                # If filter_out_masked is True, mask NaN or invalid values
                if filter_out_masked:
                    # Mask NaN values, or replace -9999 or any invalid values with a mask
                    values = np.ma.masked_where(np.isnan(values), values)  # Mask NaN values
                    # Optionally, mask invalid values like -9999 if your data has such placeholders
                    # values = np.ma.masked_equal(values, -9999)  # Mask -9999 values, if required

                return values

        except Exception as e:
            logger.error(f"Failed to read data for {path}: {e}")
            return None

    def get_node_attribute(self, path: str, attr_name: str) -> Optional[Any]:
        """
        Retrieves the value of a specific attribute for a node path.
        Looks in the in-memory cache first, then defaults to None.
        """
        # 1. Get all attributes for this path from our scanned cache
        node_attrs = self.attribute_values.get(path)
        if not node_attrs:
            logger.debug(f"No attributes found for node: {path}")
            return None

        # 2. Return the specific attribute value
        return node_attrs.get(attr_name)



    def has_derived(self, path: str, name: str) -> bool:
        return name in self.derived_values.get(path, {})

    # def get_derived(self, path: str, name: str) -> Optional[float]:
        # return self.derived_values.get(path, {}).get(name)

    # def list_derived(self, path: str) -> List[str]:
        # return list(self.derived_values.get(path, {}).keys())
