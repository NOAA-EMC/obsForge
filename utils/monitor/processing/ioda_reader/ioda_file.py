import netCDF4 as nc
import numpy as np
import os
import logging

import netCDF4 as nc
from .ioda_structure import IodaStructure

logger = logging.getLogger(__name__)


class IodaFile:
    """
    File-level IODA services.
    Provides access to content and structure of a single IODA file.
    """

    def __init__(self, file_path):
        if not os.path.exists(file_path):
            logger.error(f"IODA file not found: {file_path}")
            raise FileNotFoundError(file_path)
        self.file_path = file_path

    # --- Temporary content methods (copied exactly) ---

    def get_surface_data(self):
        """Extracts lat, lon, and the primary observation value from a NetCDF file."""
        if not os.path.exists(self.file_path):
            logger.error(f"File not found: {self.file_path}")
            return None

        try:
            with nc.Dataset(self.file_path, 'r') as ds:
                lats = ds.groups['MetaData'].variables['latitude'][:]
                lons = ds.groups['MetaData'].variables['longitude'][:]

                obs_group = ds.groups['ObsValue']
                var_names = list(obs_group.variables.keys())
                
                if not var_names:
                    logger.warning(f"No variables found in ObsValue group for {self.file_path}")
                    return None
                
                var_name = var_names[0]
                values = obs_group.variables[var_name][:]

                units = obs_group.variables[var_name].getncattr('units') if 'units' in obs_group.variables[var_name].ncattrs() else "N/A"

                return {
                    'lats': lats,
                    'lons': lons,
                    'values': values,
                    'var_name': var_name,
                    'units': units
                }
        except Exception as e:
            logger.error(f"Failed to read NetCDF {self.file_path}: {e}")
            return None

    def get_effective_dim(self):
        """
        Returns:
            2: If the file contains a plottable surface field (lat, lon, value).
            3+: If the file contains profiles, channels, or multi-level data (Default/Safe).
        """
        try:
            with nc.Dataset(self.file_path) as ds:
                data_group = ds.groups.get("ObsValue", ds)
                vars_to_check = list(data_group.variables.keys())
                if not vars_to_check:
                    return 3

                sample_var = data_group.variables[vars_to_check[0]]
                if len(sample_var.dimensions) > 1:
                    return 3 

                md = ds.groups.get("MetaData", None)
                if not md:
                    return 2

                blockers = [
                    "depth", "pressure", "air_pressure", "height", 
                    "altitude", "level", "channel", "sensor_chan", 
                    "wavelength", "frequency"
                ]

                for b in blockers:
                    if b in md.variables:
                        v = md.variables[b]
                        if v.size > 1:
                            data_sample = v[:]
                            if not np.all(data_sample == data_sample[0]):
                                return 3

                return 2
        except Exception as e:
            return 3


    def extract_structure(self):
        """
        Extract a complete IODA structure.
        This is drop-in compatible with ObsSpaceIodaStructure.read_ioda().
        """

        logger = logging.getLogger(__name__)

        structure = {
            "global_attributes": {},
            "dimensions": {},
            "groups": {}
        }

        try:
            with nc.Dataset(self.file_path, "r") as ds:
                # --- Global attributes ---
                structure["global_attributes"] = {
                    attr: ds.getncattr(attr) for attr in ds.ncattrs()
                }

                # --- Global dimensions (EXACTLY like old code) ---
                for dim_name, dim_obj in ds.dimensions.items():
                    structure["dimensions"][dim_name] = {
                        "size": len(dim_obj),
                        "isunlimited": dim_obj.isunlimited()
                    }

                # --- Groups and variables ---
                for group_name, group_obj in ds.groups.items():
                    structure["groups"][group_name] = {}

                    for var_name, var_obj in group_obj.variables.items():
                        # Variable attributes
                        attrs = {}
                        for attr in var_obj.ncattrs():
                            val = var_obj.getncattr(attr)
                            if isinstance(val, np.generic):
                                val = val.item()
                            attrs[attr] = val

                        # Storage info (compression + chunking)
                        filters = var_obj.filters()
                        chunking = var_obj.chunking()

                        structure["groups"][group_name][var_name] = {
                            "dtype": str(var_obj.dtype),
                            "dimensions": var_obj.dimensions,
                            "attributes": attrs,
                            "storage": {
                                "filters": filters,
                                "chunks": chunking
                            }
                        }

        except Exception as e:
            logger.error(
                f"Failed to extract structure from {self.file_path}: {e}"
            )
            return None

        ioda_struct = IodaStructure()
        ioda_struct.load_from_dict(structure)
        return ioda_struct

