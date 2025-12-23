import os
import glob
import re
import logging
import numpy as np
from netCDF4 import Dataset

from .log_file_parser import parse_master_log, parse_output_files_from_log
from .models import CycleData, TaskRunData, FileInventoryData

logger = logging.getLogger("InventoryScanner")

class InventoryScanner:
    
    VALID_PREFIXES = {'gdas', 'gfs', 'gcdas'}

    def __init__(self, data_root, known_mtimes: dict = None):
        self.data_root = os.path.abspath(data_root)
        self.known_mtimes = known_mtimes or {}
        logger.debug(f"INIT: InventoryScanner root={self.data_root}")

    def scan_filesystem(self, known_cycles: set = None) -> list:
        logs_root = os.path.join(self.data_root, "logs")
        if not os.path.isdir(logs_root):
            logger.error(f"Logs directory not found: {logs_root}")
            return []

        pattern = os.path.join(logs_root, "[0-9]*.log")
        master_logs = sorted(glob.glob(pattern))
        
        for m_log_path in master_logs:
            filename = os.path.basename(m_log_path)
            m = re.match(r"(\d{8})(\d{2})\.log", filename)
            if not m: continue
            
            date_str, cycle_int = m.group(1), int(m.group(2))
            logger.info(f"Scanning Cycle via Master Log: {filename}")
            cycle_obj = self._process_cycle(date_str, cycle_int, m_log_path)
            
            if cycle_obj.tasks:
                yield cycle_obj

    def _process_cycle(self, date_str, cycle_int, master_log_path):
        cycle_obj = CycleData(date=date_str, cycle=cycle_int)
        raw_tasks = parse_master_log(master_log_path)
        
        unique_tasks = {}
        for t in raw_tasks: unique_tasks[t['task_name']] = t
        final_tasks = list(unique_tasks.values())
        
        cycle_log_dir = os.path.join(self.data_root, "logs", f"{date_str}{cycle_int:02d}")

        for t in final_tasks:
            raw_name = t['task_name']
            run_type, task_name = self._normalize_task_name(raw_name)

            task_data = TaskRunData(
                task_name=task_name,
                run_type=run_type,
                logfile="missing",
                job_id=t['job_id'], status=t['status'], exit_code=t['exit_code'],
                attempt=t['attempt'], host=t['host'], runtime_sec=t['duration'],
                start_time=t.get('start_time'), end_time=t.get('end_time')
            )

            candidates = [f"{raw_name}_prep.log", f"{raw_name}.log"]
            task_log_path = None
            for c in candidates:
                p = os.path.join(cycle_log_dir, c)
                if os.path.exists(p):
                    task_log_path = p
                    break
            
            if task_log_path:
                task_data.logfile = task_log_path
                claimed_files = parse_output_files_from_log(task_log_path, self.data_root)
                expanded_files = self._expand_directories(claimed_files)
                task_data.files = self.validate_file_inventory(expanded_files)

            cycle_obj.tasks.append(task_data)

        return cycle_obj

    def _normalize_task_name(self, raw_name):
        parts = raw_name.split('_')
        prefix = parts[0]
        if prefix in self.VALID_PREFIXES:
            return prefix, raw_name[len(prefix)+1:]
        return 'unknown', raw_name

    def _expand_directories(self, rel_paths):
        expanded = set()
        for rel in rel_paths:
            full = os.path.join(self.data_root, rel)
            if os.path.isdir(full):
                for root, _, files in os.walk(full):
                    for f in files:
                        if f.endswith(('.nc', '.bufr')): 
                            expanded.add(os.path.relpath(os.path.join(root, f), self.data_root))
            else:
                expanded.add(rel)
        return list(expanded)

    def validate_file_inventory(self, rel_paths):
        inventory_list = []
        for rel in rel_paths:
            full_path = os.path.join(self.data_root, rel)
            path_parts = rel.split(os.sep)
            category = path_parts[-2] if len(path_parts) > 1 else "unknown"
            filename = path_parts[-1]
            obs_space = self._clean_obs_space_name(filename)

            integrity = "UNKNOWN"
            size = 0
            obs_count = 0
            mtime = 0
            error = None
            props = {}
            stats = []
            domain = None

            if not os.path.exists(full_path):
                integrity = "MISSING"
            else:
                try:
                    stat_info = os.stat(full_path)
                    size = stat_info.st_size
                    mtime = int(stat_info.st_mtime)
                    
                    if size == 0: 
                        integrity = "EMPTY"
                    else:
                        prev_mtime = self.known_mtimes.get(rel, 0)
                        if mtime > prev_mtime:
                            integrity, meta, deep_stats, deep_domain = self._check_content_integrity(full_path)
                            obs_count = meta.get('obs', 0)
                            error = meta.get('err')
                            props = meta 
                            stats = deep_stats
                            domain = deep_domain
                        else:
                            integrity = "OK"
                except OSError as e:
                    integrity = "ERR_ACC"
                    error = str(e)
            
            inventory_list.append(FileInventoryData(
                rel_path=rel, category=category, obs_space_name=obs_space,
                integrity=integrity, size_bytes=size, mtime=mtime,
                obs_count=obs_count, error_msg=error, 
                properties=props, stats=stats, domain=domain
            ))
            
        return inventory_list

    def _clean_obs_space_name(self, filename):
        prefixes = "|".join(self.VALID_PREFIXES)
        pattern = rf"^(?:{prefixes})\.t[0-9]{{2}}z\.(.+)\.(?:nc|bufr)$"
        m = re.match(pattern, filename, re.IGNORECASE)
        if m: return m.group(1)
        name, _ = os.path.splitext(filename)
        return name

    def _check_content_integrity(self, filepath):
        if not filepath.endswith(".nc"):
            return "OK", {"obs": 0}, [], None

        try:
            meta = {}
            stats = []
            domain = {}
            
            with Dataset(filepath, 'r') as ds:
                n = 0
                if "Location" in ds.dimensions: n = len(ds.dimensions["Location"])
                elif "nlocs" in ds.dimensions: n = len(ds.dimensions["nlocs"])
                meta['obs'] = n
                
                # Global Attributes
                for attr in ds.ncattrs():
                    try:
                        val = getattr(ds, attr)
                        meta[attr] = str(val)
                    except: pass
                
                # Schema Extraction
                meta['schema'] = self._extract_full_schema(ds)
                
                # --- INFER SEMANTIC DIMENSIONS (Added Logic) ---
                self._infer_dimensionality(meta['schema'])
                # -----------------------------------------------
                
                domain = self._extract_domain(ds)
                stats = self._calculate_statistics(ds)

                return "OK", meta, stats, domain
        except Exception as e: 
            return "CORRUPT", {"err": str(e)}, [], None

    def _extract_full_schema(self, ds_or_group, parent_path=""):
        schema = {}
        for var_name, var_obj in ds_or_group.variables.items():
            full_name = f"{parent_path}/{var_name}" if parent_path else var_name
            schema[full_name] = {
                'type': str(var_obj.dtype),
                'dims': ",".join(var_obj.dimensions),
                'ndim': 1 # Default, will be updated by inference
            }
        for group_name, group_obj in ds_or_group.groups.items():
            new_path = f"{parent_path}/{group_name}" if parent_path else group_name
            schema.update(self._extract_full_schema(group_obj, new_path))
        return schema

    def _infer_dimensionality(self, schema):
        """
        Updates 'ndim' based on semantic rules.
        Priority 1: Name contains 'Surface' -> 2D
        Priority 2: File has Depth/Pressure -> 3D
        Priority 3: Default -> 2D
        """
        vertical_coords = {'MetaData/depth', 'MetaData/air_pressure', 'MetaData/pressure', 'MetaData/height'}
        
        # Heuristic 1: Does the file support vertical profiles?
        has_vertical = any(v in schema for v in vertical_coords)
        default_dim = 3 if has_vertical else 2
        
        for path, meta in schema.items():
            var_name = path.split('/')[-1] if '/' in path else path
            
            # Heuristic 2: Force Metadata to 1D
            if path.startswith("MetaData/"):
                meta['ndim'] = 1
                
            # Heuristic 3: Force 'Surface' variables to 2D (Overrides file structure)
            elif "Surface" in var_name:
                meta['ndim'] = 2
                
            # Heuristic 4: Standard Variables use file default
            elif path.startswith("ObsValue/") or path.startswith("ObsError/") or path.startswith("PreQC/"):
                meta['ndim'] = default_dim
                
            else:
                meta['ndim'] = 1

    def _extract_domain(self, ds):
        d = {}
        try:
            if 'MetaData' in ds.groups and 'dateTime' in ds.groups['MetaData'].variables:
                times = ds.groups['MetaData'].variables['dateTime'][:]
                if np.ma.is_masked(times): times = times.compressed()
                if len(times) > 0:
                    d['start'] = int(np.min(times))
                    d['end'] = int(np.max(times))
            
            if 'MetaData' in ds.groups:
                md = ds.groups['MetaData']
                if 'latitude' in md.variables:
                    lats = md.variables['latitude'][:]
                    if np.ma.is_masked(lats): lats = lats.compressed()
                    if len(lats) > 0:
                        d['min_lat'] = float(np.min(lats))
                        d['max_lat'] = float(np.max(lats))
                
                if 'longitude' in md.variables:
                    lons = md.variables['longitude'][:]
                    if np.ma.is_masked(lons): lons = lons.compressed()
                    if len(lons) > 0:
                        d['min_lon'] = float(np.min(lons))
                        d['max_lon'] = float(np.max(lons))
        except: pass
        return d

    def _calculate_statistics(self, ds):
        stats = []
        if 'ObsValue' not in ds.groups:
            return stats
            
        grp = ds.groups['ObsValue']
        for var_name, var_obj in grp.variables.items():
            if var_obj.dtype == np.str_: continue
            
            try:
                data = var_obj[:]
                if np.ma.is_masked(data): data = data.compressed()
                
                if len(data) > 0:
                    s = {
                        'name': f"ObsValue/{var_name}",
                        'min': float(np.min(data)),
                        'max': float(np.max(data)),
                        'mean': float(np.mean(data)),
                        'std': float(np.std(data))
                    }
                    stats.append(s)
            except: pass
        return stats
