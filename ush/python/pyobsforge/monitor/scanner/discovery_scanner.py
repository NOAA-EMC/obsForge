import os
import glob
import re
import logging
from collections import defaultdict
from netCDF4 import Dataset

# Import parsers
from .log_file_parser import parse_master_log, parse_output_files_from_log

# Import Data Classes to maintain compatibility with monitor_update.py
from .scanner import CycleData, TaskRunData

logger = logging.getLogger("DiscoveryScanner")

class DiscoveryScanner:
    def __init__(self, data_root, task_configs=None):
        self.data_root = os.path.abspath(data_root)
        logger.debug(f"Initialized DiscoveryScanner with root: {self.data_root}")

    def scan_filesystem(self, known_cycles: set = None) -> list:
        """
        Main entry point for monitor_update.py.
        """
        logs_root = os.path.join(self.data_root, "logs")
        if not os.path.isdir(logs_root):
            logger.error(f"Logs directory not found: {logs_root}")
            return []

        # Find all Master Logs
        pattern = os.path.join(logs_root, "[0-9]*.log")
        logger.debug(f"Searching for Master Logs with pattern: {pattern}")
        master_logs = sorted(glob.glob(pattern))
        logger.debug(f"Found {len(master_logs)} master log files.")
        
        for m_log_path in master_logs:
            filename = os.path.basename(m_log_path)
            
            # Parse Date/Cycle from filename
            m = re.match(r"(\d{8})(\d{2})\.log", filename)
            if not m: 
                logger.debug(f"Skipping non-conforming log file: {filename}")
                continue
            
            date_str = m.group(1)
            cycle_int = int(m.group(2))
            
            # Optimization check (optional)
            if known_cycles:
                if ('gdas', date_str, cycle_int) in known_cycles:
                    logger.debug(f"Skipping known cycle: {date_str} {cycle_int}")
                    # continue 

            logger.info(f"Scanning Cycle via Master Log: {filename}")
            
            cycle_obj = self._process_cycle(date_str, cycle_int, m_log_path)
            if cycle_obj.tasks:
                yield cycle_obj

    def _process_cycle(self, date_str, cycle_int, master_log_path):
        cycle_obj = CycleData(date=date_str, cycle=cycle_int)
        
        logger.debug(f"Parsing Master Log: {master_log_path}")
        raw_tasks = parse_master_log(master_log_path)
        logger.debug(f"Found {len(raw_tasks)} raw task entries.")

        # --- POLITENESS FILTER (New) ---
        # Keep only the LATEST attempt for each task.
        # Since the log is parsed sequentially, overwriting the dict key
        # naturally keeps the last one found.
        unique_tasks = {}
        for t in raw_tasks:
            unique_tasks[t['task_name']] = t
        
        final_tasks = list(unique_tasks.values())
        logger.debug(f"Filtered to {len(final_tasks)} unique tasks (latest attempts only).")
        # -------------------------------

        cycle_log_dir = os.path.join(self.data_root, "logs", f"{date_str}{cycle_int:02d}")
        logger.debug(f"Looking for individual task logs in: {cycle_log_dir}")

        for t in final_tasks:
            t_name = t['task_name']
            
            parts = t_name.split('_')
            run_type = parts[0] if parts[0] in ['gdas', 'gfs', 'gcdas'] else 'unknown'

            task_data = TaskRunData(
                task_name=t_name,
                run_type=run_type,
                logfile="missing",
                runtime_sec=t['duration'],
                notes=f"JobId:{t['job_id']} Stat:{t['status']}"
            )

            # Find Individual Task Log
            candidates = [f"{t_name}_prep.log", f"{t_name}.log"]
            task_log_path = None
            
            for c in candidates:
                p = os.path.join(cycle_log_dir, c)
                if os.path.exists(p):
                    task_log_path = p
                    break
            
            if task_log_path:
                logger.debug(f"  [{t_name}] Log found: {os.path.basename(task_log_path)}")
                task_data.logfile = task_log_path
                
                # A. Parse Log for paths
                claimed_files = parse_output_files_from_log(task_log_path, self.data_root)
                logger.debug(f"  [{t_name}] Log claims {len(claimed_files)} output paths.")
                
                # B. Expand Directories
                expanded_files = self._expand_directories(claimed_files)
                logger.debug(f"  [{t_name}] Expanded to {len(expanded_files)} total files.")
                
                # C. Validate
                validated_files = self.validate_file_inventory(expanded_files)
                
                # D. Map
                self._map_inventory_to_task_data(task_data, validated_files)
            else:
                logger.debug(f"  [{t_name}] No individual log found. Candidates: {candidates}")

            cycle_obj.tasks.append(task_data)

        return cycle_obj

    def _map_inventory_to_task_data(self, task_data, validated_files):
        for f in validated_files:
            rel_path = f['path']
            status = f['status']
            
            logger.debug(f"    File: {rel_path} -> {status} ({f['meta']})")

            if status != "OK":
                continue

            path_parts = rel_path.split(os.sep)
            if len(path_parts) < 2: continue
            
            category = path_parts[-2]
            filename = path_parts[-1]
            obs_space = filename 

            n_obs = f['meta'].get('obs', 0)
            
            if category not in task_data.detailed_counts:
                task_data.detailed_counts[category] = {}
            
            task_data.detailed_counts[category][obs_space] = n_obs
            task_data.obs_counts[category] = task_data.obs_counts.get(category, 0) + n_obs

    def _expand_directories(self, rel_paths):
        expanded = set()
        for rel in rel_paths:
            full = os.path.join(self.data_root, rel)
            if os.path.isdir(full):
                logger.debug(f"    Expanding directory: {rel}")
                count = 0
                for root, _, files in os.walk(full):
                    for f in files:
                        if f.endswith(('.nc', '.bufr')): 
                            expanded.add(os.path.relpath(os.path.join(root, f), self.data_root))
                            count += 1
                logger.debug(f"      -> Found {count} files inside.")
            else:
                expanded.add(rel)
        return list(expanded)

    def validate_file_inventory(self, rel_paths):
        results = []
        for rel in rel_paths:
            full_path = os.path.join(self.data_root, rel)
            
            status = "UNKNOWN"
            meta = {}

            if not os.path.exists(full_path):
                status = "MISSING"
                meta = {}
            else:
                try:
                    size = os.path.getsize(full_path)
                    if size == 0: 
                        status, meta = "EMPTY", {"size": 0}
                    else: 
                        status, meta = self._check_content_integrity(full_path)
                        meta['size'] = size
                except OSError as e:
                    status, meta = "ERR_ACC", {"err": str(e)}
            
            results.append({"path": rel, "status": status, "meta": meta})
        return results

    def _check_content_integrity(self, filepath):
        if filepath.endswith(".nc"):
            try:
                with Dataset(filepath, 'r') as ds:
                    n = 0
                    if "Location" in ds.dimensions: n = len(ds.dimensions["Location"])
                    elif "nlocs" in ds.dimensions: n = len(ds.dimensions["nlocs"])
                    return "OK", {"obs": n}
            except Exception as e: 
                return "CORRUPT", {"err": str(e)}
        return "OK", {"obs": 0}
