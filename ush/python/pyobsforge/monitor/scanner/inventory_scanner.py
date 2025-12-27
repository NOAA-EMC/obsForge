import os
import glob
import re
import logging
import numpy as np
from netCDF4 import Dataset, num2date

from .log_file_parser import parse_master_log, parse_output_files_from_log
from .models import CycleData, TaskRunData, FileInventoryData

logger = logging.getLogger("InventoryScanner")

class InventoryScanner:
    """
    Scans the filesystem for tasks and files.
    Extracts Metadata, Physical Statistics (Sanitized), and Quality Flags.
    """
    
    VALID_PREFIXES = {'gdas', 'gfs', 'gcdas'}

    def __init__(self, data_root, known_state: dict = None):
        self.data_root = os.path.abspath(data_root)
        self.known_state = known_state or {}
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
        unique_tasks = {t['task_name']: t for t in raw_tasks}
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
                        history = self.known_state.get(rel)
                        prev_mtime = history['mtime'] if history else 0
                        
                        if mtime > prev_mtime:
                            # File Changed: Deep Scan
                            integrity, meta, deep_stats, deep_domain, anomalies = self._check_content_integrity(full_path)
                            obs_count = meta.get('obs', 0)
                            error = meta.get('err')
                            props = meta 
                            
                            # Inject anomaly flags into properties for the Inspector
                            if anomalies:
                                props['outliers'] = anomalies
                                
                            stats = deep_stats
                            domain = deep_domain
                        else:
                            # File Unchanged: Restore Metadata
                            integrity = history.get('integrity', 'OK')
                            obs_count = history.get('obs_count', 0)
                            
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
        """
        Returns: (status, properties, stats_list, domain_dict, anomalies_list)
        """
        if not filepath.endswith(".nc"):
            return "OK", {"obs": 0}, [], None, []

        try:
            meta = {}
            with Dataset(filepath, 'r') as ds:
                # 1. Obs Count
                n = 0
                if "Location" in ds.dimensions: n = len(ds.dimensions["Location"])
                elif "nlocs" in ds.dimensions: n = len(ds.dimensions["nlocs"])
                
                if n == 0 and 'ObsValue' in ds.groups:
                    grp = ds.groups['ObsValue']
                    for var_name in grp.variables:
                        if grp.variables[var_name].dimensions:
                            dim_name = grp.variables[var_name].dimensions[0]
                            if dim_name in ds.dimensions:
                                n = len(ds.dimensions[dim_name])
                                break
                
                meta['obs'] = n
                
                # 2. Attributes & Schema
                for attr in ds.ncattrs():
                    try: meta[attr] = str(getattr(ds, attr))
                    except: pass
                
                meta['schema'] = self._extract_full_schema(ds)
                self._infer_dimensionality(meta['schema'])
                
                # 3. Domain & Stats (with Anomaly Detection)
                domain = self._extract_domain(ds)
                stats, anomalies = self._calculate_statistics(ds)

                return "OK", meta, stats, domain, anomalies
        except Exception as e: 
            return "CORRUPT", {"err": str(e)}, [], None, []

    def _extract_full_schema(self, ds_or_group, parent_path=""):
        schema = {}
        for var_name, var_obj in ds_or_group.variables.items():
            full_name = f"{parent_path}/{var_name}" if parent_path else var_name
            schema[full_name] = {'type': str(var_obj.dtype), 'dims': ",".join(var_obj.dimensions), 'ndim': 1}
        for group_name, group_obj in ds_or_group.groups.items():
            new_path = f"{parent_path}/{group_name}" if parent_path else group_name
            schema.update(self._extract_full_schema(group_obj, new_path))
        return schema

    def _infer_dimensionality(self, schema):
        vertical_coords = {'MetaData/depth', 'MetaData/air_pressure', 'MetaData/pressure', 'MetaData/height'}
        has_vertical = any(v in schema for v in vertical_coords)
        default_dim = 3 if has_vertical else 2
        for path, meta in schema.items():
            if path.startswith("MetaData/"): meta['ndim'] = 1
            elif "Surface" in path: meta['ndim'] = 2
            elif path.startswith(("ObsValue/", "ObsError/", "PreQC/")): meta['ndim'] = default_dim
            else: meta['ndim'] = 1

    def _extract_domain(self, ds):
        d = {}
        try:
            if 'MetaData' in ds.groups:
                md = ds.groups['MetaData']
                # Time
                if 'dateTime' in md.variables:
                    t_var = md.variables['dateTime']
                    times = t_var[:]
                    if np.ma.is_masked(times): times = times.compressed()
                    if len(times) > 0:
                        min_t, max_t = np.min(times), np.max(times)
                        if hasattr(t_var, 'units'):
                            try:
                                d['start'] = int(num2date(min_t, units=t_var.units, calendar=getattr(t_var, 'calendar', 'standard')).timestamp())
                                d['end'] = int(num2date(max_t, units=t_var.units, calendar=getattr(t_var, 'calendar', 'standard')).timestamp())
                            except: d['start'], d['end'] = int(min_t), int(max_t)
                        else: d['start'], d['end'] = int(min_t), int(max_t)
                
                # Spatial
                for ax, key_min, key_max in [('latitude', 'min_lat', 'max_lat'), ('longitude', 'min_lon', 'max_lon')]:
                    if ax in md.variables:
                        data = md.variables[ax][:]
                        if np.ma.is_masked(data): data = data.compressed()
                        data = data[np.abs(data) < 1000] # Simple outlier filter for coords
                        if len(data) > 0:
                            d[key_min], d[key_max] = float(np.min(data)), float(np.max(data))
        except: pass
        return d

    def _calculate_statistics(self, ds):
        """
        Calculates statistics. 
        Filters out Unmasked Fill Values (> 1.5e9) for DB stats.
        Returns (stats_list, anomalies_list).
        """
        stats = []
        anomalies = []
        target_groups = ['ObsValue', 'MetaData']
        
        # 1.5e9 threshold catches 2^31 (2.14e9) but keeps Epoch Seconds (1.7e9)
        # Note: We skip Time variables for stats to avoid flagging valid epochs as outliers
        GENERIC_THRESHOLD = 1.5e9 
        
        for g in target_groups:
            if g not in ds.groups: continue
            grp = ds.groups[g]
            for v_name, v_obj in grp.variables.items():
                if v_obj.dtype == np.str_ or v_obj.dtype == np.object_: continue
                
                # Skip Time variables (handled in Domain)
                if 'dateTime' in v_name or 'time' in v_name.lower(): continue

                try:
                    d = v_obj[:]
                    if np.ma.is_masked(d): d = d.compressed()
                    
                    # 1. Detect Anomalies (Dirty Check)
                    if np.any(np.abs(d) > GENERIC_THRESHOLD):
                        anomalies.append(f"Unmasked Fill Value in {v_name}")
                    
                    # 2. Filter for Stats (Clean Stats)
                    d = d[np.abs(d) < GENERIC_THRESHOLD]
                    
                    if len(d) > 0:
                        stats.append({
                            'name': f"{g}/{v_name}", 
                            'min': float(np.min(d)), 'max': float(np.max(d)), 
                            'mean': float(np.mean(d)), 'std': float(np.std(d))
                        })
                except: pass
                
        return stats, anomalies
