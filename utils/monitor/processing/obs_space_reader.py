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

    def get_obsvalue_dim(self, file_path):
        """
        Return the dimensionality (ndim) of the primary ObsValue variable.

        Returns:
            int  : number of dimensions (e.g. 1, 2, 3)
            None : if file or ObsValue is missing / unreadable
        """
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None

        try:
            with nc.Dataset(file_path, "r") as ds:
                if "ObsValue" not in ds.groups:
                    logger.warning(f"No ObsValue group in {file_path}")
                    return None

                obs_group = ds.groups["ObsValue"]
                var_names = list(obs_group.variables.keys())

                if not var_names:
                    logger.warning(f"ObsValue group empty in {file_path}")
                    return None

                var = obs_group.variables[var_names[0]]
                return var.ndim

        except Exception as e:
            logger.error(f"Failed reading ObsValue dim from {file_path}: {e}")
            return None

    def get_effective_dim(self, file_path):
        """
        Returns an approximate physical dimensionality:
          2 = surface
          3 = profile / channel / level
          4 = profile + channel
        """
        try:
            with nc.Dataset(file_path) as ds:
                md = ds.groups.get("MetaData", None)
                if not md:
                    return 2

                dim = 2  # lat/lon baseline

                # candidates that increase dimension if they vary
                vertical_like = [
                    "depth",
                    "pressure",
                    "air_pressure",
                    "height",
                    "altitude"
                ]

                spectral_like = [
                    "channel",
                    "wavelength",
                    "frequency"
                ]

                def varies(var):
                    vals = var[:]
                    vals = vals[np.isfinite(vals)]
                    return len(np.unique(vals)) > 1

                # vertical dimension
                for v in vertical_like:
                    if v in md.variables and varies(md.variables[v]):
                        dim += 1
                        break

                # spectral dimension
                for v in spectral_like:
                    if v in md.variables and varies(md.variables[v]):
                        dim += 1
                        break

                return dim

        except Exception as e:
            logger.error(f"Failed to infer dim for {file_path}: {e}")
            return 2

