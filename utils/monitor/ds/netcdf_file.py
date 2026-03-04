import logging
import netCDF4
import numpy as np
from typing import Optional, Dict, Any, List
from sqlalchemy.orm import Session

from .netcdf_scanner import NetcdfScanner
from .netcdf_structure import NetcdfStructure
from .derived_attributes import DerivedAttributeRegistry

from .netcdf_structure_orm import NetcdfStructureAttributeORM
from .netcdf_file_orm import (
    NetcdfFileAttributeORM,
    NetcdfFileDerivedAttributeORM,
)

logger = logging.getLogger(__name__)

class NetcdfFile:
    """
    Domain object representing a physical NetCDF file instance.
    Handles reading actual data values and computing statistics.
    """

    def __init__(
        self,
        file: "File", # Your generic domain File object
        structure: Optional[NetcdfStructure] = None,
        registry: Optional[DerivedAttributeRegistry] = None,
    ):
        self.file = file 
        self.structure = structure
        self.registry = registry or DerivedAttributeRegistry.default()
        
        # Instance-specific data storage: path -> {attr_name: value}
        self.attribute_values: Dict[str, Dict[str, Any]] = {} 
        # path -> {metric_name: value}
        self.derived_values: Dict[str, Dict[str, float]] = {} 

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
            logger.info(f"Read attributes for {len(self.attribute_values)} nodes in {self.file.path}")
        except Exception as e:
            logger.error(f"Error reading attributes from {self.file.path}: {e}")

    def compute_derived_attributes(self) -> None:
        """
        Calculates numeric metrics for all variables using the Registry.
        """
        if not self.structure:
            logger.error(f"Cannot compute derived attributes for {self.file.path}: No structure assigned.")
            return

        try:
            with netCDF4.Dataset(self.file.path, 'r') as ds:
                # We only iterate over variables defined in our structure
                for path, node in self.structure.nodes_by_path.items():
                    if node.node_type == "VARIABLE":
                        # NetCDF4 variables are accessed by their leaf name
                        var_obj = ds.variables.get(node.name)
                        if var_obj is not None:
                            # Load data and compute via registry
                            data = var_obj[:] 
                            self.derived_values[path] = self.registry.compute_for_array(data)
            logger.info(f"Computed derived stats for {len(self.derived_values)} variables in {self.file.path}")
        except Exception as e:
            logger.error(f"Error computing derived stats for {self.file.path}: {e}")

    def to_db(self, session: Session) -> None:
        """
        Recursive entry point to persist file-specific metadata and stats.
        Assumes self.file.id and self.structure nodes are already persisted.
        """
        if not self.file.id:
            logger.error(f"to_db failed for {self.file.path}: File ID is missing. Persist the File object first.")
            return

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


    def get_variable(self, path: str) -> Optional[np.ndarray]:
        """
        Retrieves the numeric data for a variable at the given path.
        """
        node = self.structure.find_node(path)
        if not node or node.node_type != "VARIABLE":
            logger.error(f"Cannot get variable: Path '{path}' is not a variable node.")
            return None

        try:
            with netCDF4.Dataset(self.file.path, 'r') as ds:
                # netCDF4-python allows direct dictionary-like access using the full path
                # as long as the path is relative to the root or explicitly defined.
                # We ensure the path doesn't have a leading slash if ds is the root.
                clean_path = path.lstrip('/')
                
                if clean_path in ds.variables:
                    # Top-level variable access
                    return ds.variables[clean_path][:]
                
                # For nested variables, we can access them directly via the dataset object
                # if the library version supports it, or navigate cleanly:
                try:
                    var_obj = ds[path] # Most versions support direct path indexing
                    return var_obj[:]
                except KeyError:
                    logger.error(f"Variable or Group hierarchy not found in file: {path}")
                    return None
                    
        except Exception as e:
            logger.error(f"Failed to read data for {path}: {e}")
            return None
