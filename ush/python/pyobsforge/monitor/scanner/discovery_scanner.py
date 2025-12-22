import os
import glob
import re
import logging
from collections import defaultdict
from netCDF4 import Dataset

from .log_file_parser import parse_master_log, parse_output_files_from_log
from .scanner import CycleData, TaskRunData, FileInventoryData

logger = logging.getLogger("DiscoveryScanner")

class DiscoveryScanner:
    # Centralized list of known workflow prefixes
    VALID_PREFIXES = {'gdas', 'gfs', 'gcdas'}

    def __init__(self, data_root, task_configs=None):
        self.data_root = os.path.abspath(data_root)
        logger.debug(f"INIT: DiscoveryScanner root={self.data_root}")

    def scan_filesystem(self, known_cycles: set = None) -> list:
        logs_root = os.path.join(self.data_root, "logs")
        if not os.path.isdir(logs_root):
            logger.error(f"Logs directory not found: {logs_root}")
            return []

        pattern = os.path.join(logs_root, "[0-9]*.log")
        master_logs = sorted(glob.glob(pattern))
        
        logger.debug(f"SCAN: Found {len(master_logs)} master log files in {logs_root}")
        
        for m_log_path in master_logs:
            filename = os.path.basename(m_log_path)
            m = re.match(r"(\d{8})(\d{2})\.log", filename)
            if not m: 
                logger.debug(f"SKIP: '{filename}' does not match timestamp format.")
                continue
            
            date_str = m.group(1)
            cycle_int = int(m.group(2))
            
            # --- OPTIMIZATION FIX ---
            # We cannot skip a file based on one run_type, because a single log 
            # might contain NEW run_types (e.g. log has GFS and GDAS, but DB only has GDAS).
            # We scan everything. The DB layer handles deduplication (Upsert).
            # ------------------------

            logger.info(f"Scanning Cycle via Master Log: {filename}")
            cycle_obj = self._process_cycle(date_str, cycle_int, m_log_path)
            
            if cycle_obj.tasks:
                logger.debug(f"YIELD: Cycle {date_str} {cycle_int} has {len(cycle_obj.tasks)} tasks.")
                yield cycle_obj
            else:
                logger.debug(f"EMPTY: Cycle {date_str} {cycle_int} produced no valid tasks.")

    def _process_cycle(self, date_str, cycle_int, master_log_path):
        cycle_obj = CycleData(date=date_str, cycle=cycle_int)
        raw_tasks = parse_master_log(master_log_path)
        
        logger.debug(f"  LOG: Found {len(raw_tasks)} raw lines in {os.path.basename(master_log_path)}")
        
        unique_tasks = {}
        for t in raw_tasks: unique_tasks[t['task_name']] = t
        final_tasks = list(unique_tasks.values())
        
        logger.debug(f"  DEDUP: {len(final_tasks)} unique tasks to process.")

        cycle_log_dir = os.path.join(self.data_root, "logs", f"{date_str}{cycle_int:02d}")

        for t in final_tasks:
            raw_name = t['task_name']
            
            # --- DYNAMIC DISCOVERY LOGIC ---
            parts = raw_name.split('_')
            prefix = parts[0]
            
            if prefix in self.VALID_PREFIXES:
                run_type = prefix
                # Strip prefix ("gfs_") to get "marine_dump"
                # len(prefix)+1 accounts for the underscore
                task_name = raw_name[len(prefix)+1:]
            else:
                # If prefix is not known (e.g. "post_processing"), 
                # we treat the whole name as the task and type as unknown
                run_type = 'unknown'
                task_name = raw_name
            # -------------------------------

            task_data = TaskRunData(
                task_name=task_name,
                run_type=run_type,
                logfile="missing",
                job_id=t['job_id'], status=t['status'], exit_code=t['exit_code'],
                attempt=t['attempt'], host=t['host'], runtime_sec=t['duration']
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
                
                logger.debug(f"    TASK: {task_name} ({run_type}) | Log found | Files: {len(expanded_files)}")
                
                task_data.files = self.validate_file_inventory(expanded_files)
            else:
                logger.debug(f"    TASK: {task_name} ({run_type}) | No individual log found.")

            cycle_obj.tasks.append(task_data)

        return cycle_obj

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
            
            # Use dynamic prefix logic to clean names
            obs_space = self._clean_obs_space_name(filename)

            integrity = "UNKNOWN"
            size = 0
            obs_count = 0
            error = None
            props = {}

            if not os.path.exists(full_path):
                integrity = "MISSING"
            else:
                try:
                    size = os.path.getsize(full_path)
                    if size == 0: 
                        integrity = "EMPTY"
                    else: 
                        integrity, meta = self._check_content_integrity(full_path)
                        obs_count = meta.get('obs', 0)
                        error = meta.get('err')
                        
                        if 'platform' in meta: props['platform'] = meta['platform']
                        if 'sensor' in meta: props['sensor'] = meta['sensor']
                        if 'variables' in meta: props['variables'] = meta['variables']
                except OSError as e:
                    integrity = "ERR_ACC"
                    error = str(e)
            
            inventory_list.append(FileInventoryData(
                rel_path=rel, category=category, obs_space_name=obs_space,
                integrity=integrity, size_bytes=size, obs_count=obs_count,
                error_msg=error, properties=props
            ))
        return inventory_list

    def _clean_obs_space_name(self, filename):
        """
        Extracts canonical name from filename dynamically using VALID_PREFIXES.
        e.g., 'gdas.t00z.insitu_temp.nc' -> 'insitu_temp'
        """
        # Dynamic regex based on class constant
        prefixes = "|".join(self.VALID_PREFIXES)
        pattern = rf"^(?:{prefixes})\.t[0-9]{{2}}z\.(.+)\.(?:nc|bufr)$"
        
        m = re.match(pattern, filename, re.IGNORECASE)
        if m: 
            return m.group(1)
        
        name, _ = os.path.splitext(filename)
        return name

    def _check_content_integrity(self, filepath):
        if filepath.endswith(".nc"):
            try:
                meta = {}
                with Dataset(filepath, 'r') as ds:
                    n = 0
                    if "Location" in ds.dimensions: n = len(ds.dimensions["Location"])
                    elif "nlocs" in ds.dimensions: n = len(ds.dimensions["nlocs"])
                    meta['obs'] = n
                    
                    for attr in ['platform', 'sensor', 'source', 'experiment']:
                        if hasattr(ds, attr): meta[attr] = str(getattr(ds, attr))
                    
                    vars_found = []
                    if "ObsValue" in ds.groups:
                        vars_found = list(ds.groups["ObsValue"].variables.keys())
                    else:
                        dims = set(ds.dimensions.keys())
                        vars_found = [v for v in ds.variables if v not in dims]
                    
                    if vars_found:
                        if len(vars_found) > 10: meta['variables'] = ",".join(vars_found[:10]) + ",..."
                        else: meta['variables'] = ",".join(vars_found)

                    return "OK", meta
            except Exception as e: 
                return "CORRUPT", {"err": str(e)}
        return "OK", {"obs": 0}
