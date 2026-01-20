import netCDF4 as nc
import numpy as np
import os
import logging

logger = logging.getLogger(__name__)


class ObsSpaceReader:
    """Reads physical data arrays from IODA/NetCDF observation files."""
    
    def get_surface_data(self, file_path):
        """
        Extracts lat, lon, and the primary observation value from a NetCDF file.
        """
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None

        try:
            with nc.Dataset(file_path, 'r') as ds:
                # 1. Extract Coordinates from MetaData group
                # Standard IODA format uses MetaData/latitude and MetaData/longitude
                lats = ds.groups['MetaData'].variables['latitude'][:]
                lons = ds.groups['MetaData'].variables['longitude'][:]

                # 2. Extract Observation Value
                # We look for the first variable in the ObsValue group
                obs_group = ds.groups['ObsValue']
                var_names = list(obs_group.variables.keys())
                
                if not var_names:
                    logger.warning(f"No variables found in ObsValue group for {file_path}")
                    return None
                
                # Pick the first available variable (e.g., air_temperature)
                var_name = var_names[0]
                values = obs_group.variables[var_name][:]
                
                # Get units for the legend
                units = obs_group.variables[var_name].getncattr('units') if 'units' in obs_group.variables[var_name].ncattrs() else "N/A"

                return {
                    'lats': lats,
                    'lons': lons,
                    'values': values,
                    'var_name': var_name,
                    'units': units
                }
        except Exception as e:
            logger.error(f"Failed to read NetCDF {file_path}: {e}")
            return None
