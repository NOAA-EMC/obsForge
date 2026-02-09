import netCDF4 as nc
import numpy as np
import os
import logging

logger = logging.getLogger(__name__)


class IodaReader:
    def __init__(self, file_path):
        self.file_path = file_path
        self.ds = None
        if not os.path.exists(file_path):
            logger.error(f"IODA file not found: {file_path}")
            raise FileNotFoundError(file_path)

    def __enter__(self):
        self.ds = nc.Dataset(self.file_path, 'r')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.ds is not None:
            self.ds.close()

    def get_surface_data(self):

        """
        Extracts lat, lon, and the primary observation value from a NetCDF file.
        """
        if not os.path.exists(self.file_path):
            logger.error(f"File not found: {self.file_path}")
            return None

        try:
            with nc.Dataset(self.file_path, 'r') as ds:
                # 1. Extract Coordinates from MetaData group
                # Standard IODA format uses MetaData/latitude and MetaData/longitude
                lats = ds.groups['MetaData'].variables['latitude'][:]
                lons = ds.groups['MetaData'].variables['longitude'][:]

                # 2. Extract Observation Value
                # We look for the first variable in the ObsValue group
                obs_group = ds.groups['ObsValue']
                var_names = list(obs_group.variables.keys())
                
                if not var_names:
                    logger.warning(f"No variables found in ObsValue group for {self.file_path}")
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
            logger.error(f"Failed to read NetCDF {self.file_path}: {e}")
            return None

    def get_obsvalue_dim(self):
        """
        Return the dimensionality (ndim) of the primary ObsValue variable.

        Returns:
            int  : number of dimensions (e.g. 1, 2, 3)
            None : if file or ObsValue is missing / unreadable
        """
        if not os.path.exists(self.file_path):
            logger.error(f"File not found: {self.file_path}")
            return None

        try:
            with nc.Dataset(self.file_path, "r") as ds:
                if "ObsValue" not in ds.groups:
                    logger.warning(f"No ObsValue group in {self.file_path}")
                    return None

                obs_group = ds.groups["ObsValue"]
                var_names = list(obs_group.variables.keys())

                if not var_names:
                    logger.warning(f"ObsValue group empty in {self.file_path}")
                    return None

                var = obs_group.variables[var_names[0]]
                return var.ndim

        except Exception as e:
            logger.error(f"Failed reading ObsValue dim from {self.file_path}: {e}")
            return None

    def get_effective_dim(self):
        """
        Returns:
            2: If the file contains a plottable surface field (lat, lon, value).
            3+: If the file contains profiles, channels, or multi-level data (Default/Safe).
        """
        try:
            with nc.Dataset(self.file_path) as ds:
                # 1. Look for the actual data variables (usually in ObsValue)
                # We check the shape of the data itself, not just metadata.
                data_group = ds.groups.get("ObsValue", ds)
                
                # Get the first available data variable to inspect its "nature"
                vars_to_check = list(data_group.variables.keys())
                if not vars_to_check:
                    return 3 # No data? Don't plot.

                sample_var = data_group.variables[vars_to_check[0]]
                
                # BLOCKER A: Multi-dimensionality
                # If a variable is (nlocs, nchans) or (nlocs, nlevels), rank is 2.
                # A plottable surface field MUST have rank 1 (nlocs,).
                if len(sample_var.dimensions) > 1:
                    return 3 

                # 2. Inspect Metadata for hidden vertical/spectral variance
                md = ds.groups.get("MetaData", None)
                if not md:
                    return 2 # No metadata to contradict the surface assumption

                # Dimensions that turn a map into a profile or spectral set
                blockers = [
                    "depth", "pressure", "air_pressure", "height", 
                    "altitude", "level", "channel", "sensor_chan", 
                    "wavelength", "frequency"
                ]

                for b in blockers:
                    if b in md.variables:
                        v = md.variables[b]
                        # If the coordinate has more than one value...
                        if v.size > 1:
                            # ...and those values aren't all identical
                            # (Using a small sample check for performance)
                            data_sample = v[:]
                            if not np.all(data_sample == data_sample[0]):
                                return 3 # It's a profile or multi-channel file

                return 2 # If we got here, it's a flat surface field.

        except Exception as e:
            # On error, return 3 to prevent potentially broken/messy plots
            return 3

    def old_get_effective_dim(self):
        """
        Returns an approximate physical dimensionality:
          2 = surface
          3 = profile / channel / level
          4 = profile + channel
        """
        try:
            with nc.Dataset(self.file_path) as ds:
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
            logger.error(f"Failed to infer dim for {self.file_path}: {e}")
            return 2

    def get_structure(self):
        """
        Return a complete, JSON-serializable description of the IODA file structure.
        No data arrays are read.
        """
        structure = {
            "dimensions": {},
            "global_attributes": {},
            "groups": {}
        }

        with nc.Dataset(self.file_path, "r") as ds:
            # --- global dimensions ---
            for name, dim in ds.dimensions.items():
                structure["dimensions"][name] = len(dim)

            # --- global attributes ---
            for attr in ds.ncattrs():
                structure["global_attributes"][attr] = ds.getncattr(attr)

            # --- groups ---
            for gname, group in ds.groups.items():
                ginfo = {
                    "dimensions": {},
                    "variables": {},
                    "attributes": {}
                }

                # group dimensions
                for dname, dim in group.dimensions.items():
                    ginfo["dimensions"][dname] = len(dim)

                # group attributes
                for attr in group.ncattrs():
                    ginfo["attributes"][attr] = group.getncattr(attr)

                # variables
                for vname, var in group.variables.items():
                    vinfo = {
                        "dtype": str(var.dtype),
                        "dimensions": list(var.dimensions),
                        "attributes": {}
                    }

                    for attr in var.ncattrs():
                        val = var.getncattr(attr)
                        # ensure JSON-safe scalars
                        if isinstance(val, np.generic):
                            val = val.item()
                        vinfo["attributes"][attr] = val

                    ginfo["variables"][vname] = vinfo

                structure["groups"][gname] = ginfo

        return structure

