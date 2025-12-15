import os
import glob
import re
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Iterator, Dict
from datetime import datetime, timedelta
from netCDF4 import Dataset

# Import the legacy parser
from pyobsforge.monitor.log_file_parser import parse_job_log

logger = logging.getLogger("ObsForgeScanner")

@dataclass
class TaskRunData:
    task_name: str
    run_type: str
    logfile: str
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    runtime_sec: float = 0.0
    notes: Optional[str] = None
    obs_counts: Dict[str, int] = field(default_factory=dict)
    detailed_counts: Dict[str, Dict[str, int]] = field(default_factory=dict)
    files_found: Dict[str, List[str]] = field(default_factory=dict)

@dataclass
class CycleData:
    date: str
    cycle: int
    tasks: List[TaskRunData] = field(default_factory=list)

class ObsForgeScanner:
    def __init__(self, data_root: str, task_configs: dict):
        self.data_root = data_root
        self.task_configs = task_configs

    def scan_filesystem(self, known_cycles: set = None) -> Iterator[CycleData]:
        pattern = os.path.join(self.data_root, "*.[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]")
        logger.debug(f"Root Pattern: {pattern}")
        found_dirs = sorted(glob.glob(pattern))

        for date_dir in found_dirs:
            folder_name = os.path.basename(date_dir)
            match = re.match(r"^(.+)\.(\d{8})$", folder_name)
            if not match: continue
            
            run_type_hint = match.group(1)
            date_str = match.group(2)
            
            cycle_dirs = sorted(glob.glob(os.path.join(date_dir, "[0-2][0-9]")))
            for c_dir in cycle_dirs:
                try:
                    cycle_int = int(os.path.basename(c_dir))
                except ValueError: continue

                if known_cycles and (run_type_hint, date_str, cycle_int) in known_cycles:
                    continue

                logger.debug(f"--> Cycle Found: {run_type_hint}.{date_str} {cycle_int:02d}")
                cycle_obj = self._build_cycle_data(date_str, cycle_int, run_type_hint)
                
                if cycle_obj.tasks:
                    yield cycle_obj
                else:
                    logger.debug(f"    [EMPTY] Cycle {run_type_hint}.{date_str} {cycle_int:02d} has no valid tasks/logs.")

    def _build_cycle_data(self, date: str, cycle: int, run_type_hint: str) -> CycleData:
        cycle_obj = CycleData(date=date, cycle=cycle)
        
        for task_name, cfg in self.task_configs.items():
            log_rel = cfg['logfile_template'].format(date=date, cycle=f"{cycle:02d}", run_type=run_type_hint)
            log_path = os.path.join(self.data_root, log_rel)
            
            if not os.path.exists(log_path):
                logger.debug(f"      [SKIP] Missing log for {task_name}: {log_rel}")
                continue

            # Simply call the legacy parser
            task_data = self._parse_task_log(task_name, log_path, run_type_hint)

            obs_template = cfg.get('obs_path_template')
            if obs_template:
                effective_run_type = task_data.run_type if task_data.run_type else run_type_hint
                obs_rel = obs_template.format(date=date, cycle=f"{cycle:02d}", run_type=effective_run_type)
                obs_root = os.path.join(self.data_root, obs_rel)
                
                if os.path.exists(obs_root):
                    self._scan_obs_directories(task_data, obs_root, cfg.get('categories'))

            cycle_obj.tasks.append(task_data)
            
        return cycle_obj

    def _parse_task_log(self, task_name: str, log_path: str, hint: str) -> TaskRunData:
        """
        Uses the legacy parser. Passes raw task_name because parser adds .sh automatically.
        """
        parsed = parse_job_log(log_path, task_name)
        
        data = TaskRunData(task_name=task_name, run_type=hint, logfile=log_path)
        
        if parsed:
            # FIX: Check if 'elapsed_time' is not None before converting
            elapsed = parsed.get("elapsed_time")
            
            if elapsed is not None:
                if isinstance(elapsed, timedelta):
                    data.runtime_sec = elapsed.total_seconds()
                else:
                    try:
                        data.runtime_sec = float(elapsed)
                    except (ValueError, TypeError):
                        data.runtime_sec = 0.0
            
            # Handle timestamps
            if data.runtime_sec > 0:
                data.run_type = hint
                data.start_time = str(parsed.get("start_date"))
                
                if parsed.get("end_date"):
                    data.end_time = str(parsed.get("end_date"))
                elif data.start_time:
                    try:
                        dt_start = datetime.strptime(data.start_time, "%Y-%m-%d %H:%M:%S")
                        data.end_time = (dt_start + timedelta(seconds=data.runtime_sec)).strftime("%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        pass
        else:
            logger.debug(f"      [PARSE FAIL] {task_name} (Legacy parser returned None)")

        return data

    def _scan_obs_directories(self, task_data: TaskRunData, obs_root: str, categories_cfg: dict):
        filename_pattern = re.compile(r"^(gdas|gfs|gcdas)\.t([0-9]{2})z\.([A-Za-z0-9_\-]+)\.nc.*$", re.IGNORECASE)
        categories = categories_cfg or self._auto_detect_categories(obs_root)

        for cat_key, cat_pattern in categories.items():
            cat_dir = os.path.join(obs_root, cat_pattern)
            if not os.path.isdir(cat_dir):
                logger.debug(f"      [CAT SKIP] {cat_key}: Dir not found: {cat_dir}")
                continue

            obs_map = {}
            for fname in sorted(os.listdir(cat_dir)):
                m = filename_pattern.match(fname)
                if m: 
                    obs_map[m.group(3)] = fname
                else:
                    logger.debug(f"        [IGNORE] {fname} (Regex mismatch)")

            total_obs = 0
            valid_files_count = 0
            
            if cat_key not in task_data.detailed_counts:
                task_data.detailed_counts[cat_key] = {}

            for obs_name, filename in obs_map.items():
                fullpath = os.path.join(cat_dir, filename)
                n_obs = -1
                
                try: n_obs = self._read_ioda_count(fullpath)
                except Exception: pass

                try: self._check_ioda_structure(fullpath)
                except Exception as e:
                    if n_obs >= 0: logger.debug(f"        [INVALID STRUCT] {filename}: {e}")
                    else: logger.debug(f"        [FILE ERROR] {filename}: {e}")

                task_data.detailed_counts[cat_key][obs_name] = n_obs
                
                if n_obs > 0:
                    total_obs += n_obs
                if n_obs >= 0:
                    valid_files_count += 1
            
            task_data.obs_counts[cat_key] = total_obs
            
            if obs_map:
                logger.debug(f"      [CAT] {cat_key:<8} -> Scanned {valid_files_count}/{len(obs_map)} files. Total Obs: {total_obs}")

    def _read_ioda_count(self, ncfile):
        with Dataset(ncfile, "r") as ds:
            if "Location" in ds.dimensions: return len(ds.dimensions["Location"])
            if "nlocs" in ds.dimensions: return len(ds.dimensions["nlocs"])
            raise KeyError("No Location/nlocs dim")

    def _check_ioda_structure(self, ncfile):
        required_groups = ["MetaData", "ObsValue", "ObsError", "PreQC"]
        with Dataset(ncfile, "r") as ds:
            for grp in required_groups:
                if grp not in ds.groups: raise KeyError(f"Missing group '{grp}'")
            if "MetaData" in ds.groups:
                if "latitude" not in ds.groups["MetaData"].variables:
                    raise KeyError("Missing 'latitude' in MetaData")

    def _auto_detect_categories(self, obs_root: str) -> dict:
        if not os.path.isdir(obs_root): return {}
        cats = {}
        for name in sorted(os.listdir(obs_root)):
            if os.path.isdir(os.path.join(obs_root, name)):
                cats[name] = name
        return cats
