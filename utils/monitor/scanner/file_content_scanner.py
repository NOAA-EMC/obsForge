import os
import logging
from typing import List

from scanner.models import CycleData, TaskRunData, FileInventoryData
from scanner.persistence import Registrar

logger = logging.getLogger("FileContentScanner")


class FileContentScanner:
    """
    Scans FileInventoryData objects, handling both filesystem
    and content inspection (content will later move to Inspector).
    """

    def __init__(self, data_root, known_cycles: set = None):
        self.data_root = data_root
        self.known_cycles = known_cycles or {}

    # -----------------------------
    # Public entry points
    # -----------------------------
    # temporary
    # def scan_cycles(self, cycles: List[CycleData], limit: int = None) -> list:
        # if limit and limit > 0:
            # logger.info(f"scanning limited to {limit} cycles")
            # cycles = cycles[-limit:]
        # for cycle in cycles:
            # logger.info(f"cycle {cycle.date} {cycle.cycle:02d}")
            # for task in cycle.tasks:
                # self.inspect_task_files(task)

    # to be deprecated
    # def scan_cycles(self, known_cycles: set = None, limit: int = None) -> list:
    def inspect_cycles(self, cycles: List[CycleData], limit: int = None) -> list:
        for cycle in cycles:
            logger.info(f"Inspecting cycle {cycle.date} {cycle.cycle:02d}")
            for task in cycle.tasks:
                self._inspect_file_content(task.files)
        return cycles

    def _inspect_file_content(self, file_inventory: list):
        """
        For files that exist and are non-empty, inspect contents:
          - Calls _check_content_integrity
          - Updates obs_count, stats, domain, properties, integrity
        """
        for f in file_inventory:
            if f.integrity not in ("OK_PENDING",):
                # Skip missing/empty/error files
                continue

            full_path = os.path.join(self.data_root, f.rel_path)

            try:
                integrity, meta, stats, domain, anomalies = self._check_content_integrity(full_path)
                f.integrity = integrity
                f.obs_count = meta.get("obs", 0)
                f.stats = stats
                f.domain = domain
                f.properties = meta
                if anomalies:
                    f.properties['outliers'] = anomalies
            except Exception as e:
                f.integrity = "CORRUPT"
                f.error_msg = str(e)


    def _clean_obs_space_name(self, filename):
        prefixes = "|".join(self.VALID_PREFIXES)
        pattern = rf"^(?:{prefixes})\.t[0-9]{{2}}z\.(.+)\.(?:nc|bufr)$"
        m = re.match(pattern, filename, re.IGNORECASE)
        if m:
            return m.group(1)
        return os.path.splitext(filename)[0]

    def _check_content_integrity(self, filepath):
        if not filepath.endswith(".nc"):
            return "OK", {"obs": 0}, [], None, []
        try:
            with Dataset(filepath, 'r') as ds:
                meta = {}
                n = 0
                if "Location" in ds.dimensions:
                    n = len(ds.dimensions["Location"])
                elif "nlocs" in ds.dimensions:
                    n = len(ds.dimensions["nlocs"])
                if n == 0 and 'ObsValue' in ds.groups:
                    grp = ds.groups['ObsValue']
                    for var_name in grp.variables:
                        if grp.variables[var_name].dimensions:
                            dim_name = grp.variables[var_name].dimensions[0]
                            n = len(ds.dimensions[dim_name])
                            break
                meta['obs'] = n
                for attr in ds.ncattrs():
                    try:
                        meta[attr] = str(getattr(ds, attr))
                    except Exception:
                        pass

                meta['schema'] = self._extract_full_schema(ds)
                self._infer_dimensionality(meta['schema'])
                
                # --- CALL THE FIXED DOMAIN EXTRACTION ---
                domain = self._extract_domain(ds)
                
                stats, anomalies = self._calculate_statistics(ds)
                return "OK", meta, stats, domain, anomalies
        except Exception as e:
            return "CORRUPT", {"err": str(e)}, [], None, []

    def _extract_full_schema(self, ds_or_group, parent_path=""):
        """
        Recursively extracts variable metadata using actual NetCDF dimensions.
        """
        schema = {}
        
        # Process variables in the current group
        for var_name, var_obj in ds_or_group.variables.items():
            full_name = f"{parent_path}/{var_name}" if parent_path else var_name
            
            # Use the actual dimensions defined in the NetCDF file
            actual_dims = var_obj.dimensions
            
            schema[full_name] = {
                'type': str(var_obj.dtype),
                'dims': ",".join(actual_dims),
                'ndim': len(actual_dims)  # Use actual rank (e.g., 2 for [nlocs, nchans])
            }

        # Recursive step: Process nested groups
        for group_name, group_obj in ds_or_group.groups.items():
            new_path = f"{parent_path}/{group_name}" if parent_path else group_name
            schema.update(self._extract_full_schema(group_obj, new_path))
            
        return schema

    def _infer_dimensionality(self, schema):
        """
        Refines dimensionality for Database storage.
        Logic: 
        1 = Metadata (non-spatial)
        2 = Surface/Spatial (tied to nlocs only)
        3 = Profile/Spectral (nlocs + vertical/channel dimension)
        """
        # Create a set of all variable paths to check for vertical/spectral indicators
        all_paths = set(schema.keys())

        for path, meta in schema.items():
            dims_list = meta['dims'].split(',') if meta['dims'] else []
            
            # 1. MetaData group is always auxiliary (1D)
            if path.startswith("MetaData/"):
                meta['ndim'] = 1
                continue

            # 2. Hard-wired 3D: Any variable with more than one dimension
            # e.g., (nlocs, nchans) or (nlocs, nlevels)
            if len(dims_list) > 1:
                meta['ndim'] = 3
                continue

            # 3. Handle 1D variables in Value groups (ObsValue, ObsError, etc.)
            if path.startswith(("ObsValue/", "ObsError/", "PreQC/")):
                # Look for indicators of vertical (Atmos/Marine) or spectral (Radiance) data
                vertical_indicators = [
                    'MetaData/depth', 'MetaData/pressure', 'MetaData/height', 
                    'MetaData/air_pressure', 'MetaData/sensor_channel', 
                    'MetaData/level', 'MetaData/channelIndex'
                ]
                
                is_profile = any(f"{ind}" in all_paths for ind in vertical_indicators)
                
                if 'nlocs' in dims_list:
                    # If there is vertical info elsewhere, this 1D variable is likely 
                    # a slice or coordinate of a 3D system. Otherwise, it's 2D surface.
                    meta['ndim'] = 3 if is_profile else 2
                else:
                    meta['ndim'] = 1
            else:
                # Everything else (e.g., GeoVaLs or other groups)
                meta['ndim'] = 1



    def _extract_domain(self, ds):
        """
        Extracts start/end time.
        FIXED: Uses date2num to safely handle 'cftime' objects and various Epochs (1858, 1981).
        """
        d = {}
        try:
            t_var = None
            
            # 1. Search for Time Variable (Check Root AND MetaData)
            candidates = [
                ('MetaData', 'dateTime'),
                ('MetaData', 'time'),
                (None, 'time'),       # Check root/time (for SST files)
                (None, 'date')
            ]
            
            for group, var_name in candidates:
                if group:
                    if group in ds.groups and var_name in ds.groups[group].variables:
                        t_var = ds.groups[group].variables[var_name]
                        break
                else:
                    if var_name in ds.variables:
                        t_var = ds.variables[var_name]
                        break

            if t_var is not None:
                # Get raw values
                times = t_var[:]
                if np.ma.is_masked(times):
                    times = times.compressed()

                if len(times) > 0:
                    min_t, max_t = np.min(times), np.max(times)

                    # --- CRITICAL FIX: Safe Unit Conversion ---
                    if hasattr(t_var, 'units'):
                        try:
                            # Step A: Convert number to Date Object (might be cftime)
                            dt_start = num2date(
                                min_t, 
                                units=t_var.units, 
                                calendar=getattr(t_var, 'calendar', 'standard')
                            )
                            dt_end = num2date(
                                max_t, 
                                units=t_var.units, 
                                calendar=getattr(t_var, 'calendar', 'standard')
                            )
                            
                            # Step B: Convert Date Object to Unix Timestamp (seconds since 1970)
                            # We use date2num to handle cftime objects safely (avoids .timestamp() crash)
                            unix_units = "seconds since 1970-01-01 00:00:00"
                            d['start'] = int(date2num(dt_start, units=unix_units))
                            d['end'] = int(date2num(dt_end, units=unix_units))
                        except Exception:
                            # Fallback
                            d['start'], d['end'] = int(min_t), int(max_t)
                    else:
                        d['start'], d['end'] = int(min_t), int(max_t)

            # 2. Extract Spatial Domain (Unchanged)
            # (Note: We look in MetaData for lat/lon as per IODA spec)
            if 'MetaData' in ds.groups:
                md = ds.groups['MetaData']
                axes = [
                    ('latitude', 'min_lat', 'max_lat'),
                    ('longitude', 'min_lon', 'max_lon')
                ]
                for ax, k_min, k_max in axes:
                    if ax in md.variables:
                        vals = md.variables[ax][:]
                        if np.ma.is_masked(vals):
                            vals = vals.compressed()
                        vals = vals[np.abs(vals) < 1000]
                        if len(vals) > 0:
                            d[k_min] = float(np.min(vals))
                            d[k_max] = float(np.max(vals))
        except Exception:
            pass
        return d

    def _calculate_statistics(self, ds):
        stats = []
        anomalies = []
        THRESHOLD = 1.5e9
        groups = ['ObsValue', 'MetaData']
        
        for g in groups:
            if g not in ds.groups:
                continue
            grp = ds.groups[g]
            for v_name, v_obj in grp.variables.items():
                # Skip strings/objects
                if v_obj.dtype == np.str_ or v_obj.dtype == np.object_:
                    continue
                # Skip time variables
                if 'dateTime' in v_name or 'time' in v_name.lower():
                    continue

                try:
                    # --- CRITICAL CHANGE: Force Raw Data Access ---
                    # 1. Turn off auto-masking for this read to see the "garbage"
                    v_obj.set_auto_mask(False) 
                    raw_data = v_obj[:] 

                    # 2. Check for Garbage (Huge Numbers)
                    # Now 'raw_data' contains the 9.99e36 values if they exist
                    if np.any(np.abs(raw_data) > THRESHOLD):
                        anomalies.append(f"Contains Fill Values in {v_name}")

                    # 3. For Statistics (Min/Max/Mean), we DO want to filter them out
                    # otherwise the Mean will be 1e36.
                    clean_data = raw_data[np.abs(raw_data) < THRESHOLD]

                    if len(clean_data) > 0:
                        stats.append({
                            'name': f"{g}/{v_name}",
                            'min': float(np.min(clean_data)),
                            'max': float(np.max(clean_data)),
                            'mean': float(np.mean(clean_data)),
                            'std': float(np.std(clean_data))
                        })
                    else:
                        # If the file is 100% garbage
                        anomalies.append(f"Variable {v_name} is 100% Fill Values")

                except Exception:
                    pass
                    
        return stats, anomalies

