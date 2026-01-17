import glob
import logging
import os
import re

from .log_file_parser import parse_master_log, parse_output_files_from_log
from .models import CycleData, FileInventoryData, TaskRunData
from .persistence import ScannerStateReader
from .ioda_structure import IODAStructureReader


logger = logging.getLogger("InventoryScanner")

class InventoryScanner:
    """
    Scans the filesystem.
    REFACTOR STATUS: Phase 2 (Stripped).
    - Uses IODAStructureReader for metadata.
    - DOES NOT calculate stats or domain (Handled by analyze_data.py).
    - Fast execution.
    """

    VALID_PREFIXES = {'gdas', 'gfs', 'gcdas'}

    def __init__(self, data_root, known_state: dict = None):
        self.data_root = os.path.abspath(data_root)
        self.known_state = known_state or {}
        # Instantiate the new reader once
        self.structure_reader = IODAStructureReader()
        logger.debug(f"INIT: InventoryScanner root={self.data_root}")

    def scan_filesystem(self, known_cycles: set = None, limit: int = None) -> list:
        logs_root = os.path.join(self.data_root, "logs")
        if not os.path.isdir(logs_root):
            logger.error(f"Logs directory not found: {logs_root}")
            return []

        pattern = os.path.join(logs_root, "[0-9]*.log")
        master_logs = sorted(glob.glob(pattern))

        if limit and limit > 0:
            logger.info(f"Limiting scan to the last {limit} cycles.")
            master_logs = master_logs[-limit:]

        for m_log_path in master_logs:
            filename = os.path.basename(m_log_path)
            m = re.match(r"(\d{8})(\d{2})\.log", filename)
            if not m:
                continue

            date_str, cycle_int = m.group(1), int(m.group(2))
            
            cycle_obj = self._process_cycle(date_str, cycle_int, m_log_path)
            if cycle_obj.tasks:
                yield cycle_obj

    def _process_cycle(self, date_str, cycle_int, master_log_path):
        cycle_obj = CycleData(date=date_str, cycle=cycle_int)
        raw_tasks = parse_master_log(master_log_path)
        unique_tasks = {t['task_name']: t for t in raw_tasks}

        cycle_log_dir = os.path.join(
            self.data_root, "logs", f"{date_str}{cycle_int:02d}"
        )

        for t in unique_tasks.values():
            raw_name = t['task_name']
            run_type, task_name = self._normalize_task_name(raw_name)

            task_data = TaskRunData(
                task_name=task_name,
                run_type=run_type,
                logfile="missing",
                job_id=t['job_id'],
                status=t['status'],
                exit_code=t['exit_code'],
                attempt=t['attempt'],
                host=t['host'],
                runtime_sec=t['duration'],
                start_time=t.get('start_time'),
                end_time=t.get('end_time')
            )

            for c in [f"{raw_name}_prep.log", f"{raw_name}.log"]:
                p = os.path.join(cycle_log_dir, c)
                if os.path.exists(p):
                    task_data.logfile = p
                    files = parse_output_files_from_log(p, self.data_root)
                    task_data.files = self.validate_file_inventory(
                        self._expand_directories(files)
                    )
                    break

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
                            expanded.add(
                                os.path.relpath(
                                    os.path.join(root, f), self.data_root
                                )
                            )
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

                        # If file changed, we scan metadata (fast)
                        if mtime > prev_mtime:
                            res = self._check_content_integrity(full_path)
                            integrity, meta, stats, domain, anomalies = res
                            obs_count = meta.get('obs', 0)
                            error = meta.get('err')
                            props = meta
                            if anomalies:
                                props['outliers'] = anomalies
                        else:
                            integrity = "OK_SKIPPED"

                except OSError as e:
                    integrity = "ERR_ACC"
                    error = str(e)

            inventory_list.append(FileInventoryData(
                rel_path=rel,
                category=category,
                obs_space_name=obs_space,
                integrity=integrity,
                size_bytes=size,
                mtime=mtime,
                obs_count=obs_count,
                error_msg=error,
                properties=props,
                stats=stats,  # Will be empty now
                domain=domain # Will be None now
            ))
        return inventory_list

    def _clean_obs_space_name(self, filename):
        prefixes = "|".join(self.VALID_PREFIXES)
        pattern = rf"^(?:{prefixes})\.t[0-9]{{2}}z\.(.+)\.(?:nc|bufr)$"
        m = re.match(pattern, filename, re.IGNORECASE)
        if m:
            return m.group(1)
        return os.path.splitext(filename)[0]

    def _check_content_integrity(self, filepath):
        """
        Phase 2 Logic:
        1. Scan Header (IODA Structure) -> Fast.
        2. Return Empty Stats/Domain -> Defer to Analyzer.
        """
        if not filepath.endswith(".nc"):
            return "OK", {"obs": 0}, [], None, []

        # 1. Delegate Structure Scan (Fast, no data reading)
        struct_info = self.structure_reader.scan_structure(filepath)
        
        if not struct_info.get("valid_ioda"):
             return "CORRUPT", {"err": struct_info.get("error", "Unknown")}, [], None, []

        # 2. Map structure to legacy metadata format
        meta = {}
        meta.update(struct_info.get("global_attrs", {}))
        
        # Convert schema list -> dict for DB compatibility
        schema_dict = {}
        for var in struct_info.get("variables", []):
            path = var['full_path']
            dims_str = ",".join(var['dims']) if isinstance(var['dims'], list) else str(var['dims'])
            schema_dict[path] = {
                'type': var['dtype'],
                'dims': dims_str,
                'ndim': len(var['dims'])
            }
        meta['schema'] = schema_dict

        # 3. Simple Heuristics for Obs Count (Optional, metadata based)
        # We can try to guess obs count from 'nlocs' dimension size if available in vars
        # But for now, returning 0 is safer than opening the file again.
        # The Analyzer can update this if needed, or we rely on 'Location' being in dims.
        n = 0 
        
        # NOTE: We return EMPTY stats and domain.
        # The 'analyze_data.py' tool will pick this file up and fill these in.
        return "OK", meta, [], None, []
