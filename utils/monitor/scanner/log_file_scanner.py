import glob
import logging
import os
import re
from datetime import datetime

import numpy as np
from netCDF4 import Dataset, num2date, date2num  # Added date2num

from .log_file_parser import parse_master_log, parse_output_files_from_log
from .models import CycleData, FileInventoryData, TaskRunData
from .persistence import ScannerStateReader

logger = logging.getLogger("LogFileScanner")


class LogFileScanner:
    """
    Scans the filesystem.
    Incremental Logic: Only opens NetCDF files if mtime > known_state.
    """

    VALID_PREFIXES = {'gdas', 'gfs', 'gcdas'}

    def __init__(self, data_root, known_state: dict = None):
        self.data_root = os.path.abspath(data_root)
        self.known_state = known_state or {}
        logger.debug(f"INIT: InventoryScanner root={self.data_root}")

    def scan_cycles(self, known_cycles: set = None, limit: int = None) -> list:
        """
        Scans for cycles logged in master logs, parses tasks, and 
        populates task file inventory.
        """
        cycles = []
        
        # Step 1: Discover all logged cycles
        logged_cycles = self._discover_logged_cycles(limit=limit)
        
        for date_str, cycle_int, master_log_path in logged_cycles:
            # Step 2: Parse tasks from master log only
            cycle_obj = self._process_cycle_logs(date_str, cycle_int, master_log_path)
            
            if not cycle_obj.tasks:
                continue
            
            # Step 3: Populate file inventory for tasks
            self._populate_task_file_inventory(cycle_obj)
            
            cycles.append(cycle_obj)
        
        return cycles

    def _discover_logged_cycles(self, limit: int = None):
        """
        Returns a list of (date_str, cycle_int, master_log_path)
        """
        logs_root = os.path.join(self.data_root, "logs")
        if not os.path.isdir(logs_root):
            logger.error(f"Logs directory not found: {logs_root}")
            return []

        pattern = os.path.join(logs_root, "[0-9]*.log")
        master_logs = sorted(glob.glob(pattern))

        if limit and limit > 0:
            master_logs = master_logs[-limit:]

        cycles = []
        for m_log_path in master_logs:
            filename = os.path.basename(m_log_path)
            m = re.match(r"(\d{8})(\d{2})\.log", filename)
            if not m:
                continue

            date_str, cycle_int = m.group(1), int(m.group(2))
            cycles.append((date_str, cycle_int, m_log_path))

        return cycles

    # to be renamed to _process_cycle
    def _process_cycle_logs(self, date_str, cycle_int, master_log_path):
        """
        Returns a CycleData object with tasks and logfiles only.
        CycleData object should not be used here
        """
        cycle_obj = CycleData(date=date_str, cycle=cycle_int)

        logger.info(f"Processing cycle {cycle_obj.date} {cycle_obj.cycle:02d}")

        raw_tasks = parse_master_log(master_log_path)
        unique_tasks = {t['task_name']: t for t in raw_tasks}

        tasks = self._create_task_run_data(unique_tasks)

        cycle_log_dir = os.path.join(
            self.data_root, 
            "logs", 
            f"{date_str}{cycle_int:02d}"
        )

        for task_data in tasks:
            log_path = self._find_task_logfile(task_data, cycle_log_dir)
            if log_path:
                task_data.logfile = log_path
            cycle_obj.tasks.append(task_data)

        return cycle_obj

    def _create_task_run_data(self, task_list):
        tasks = []
        for t in task_list.values():
            raw_name = t['task_name']
            run_type, task_name = self._normalize_task_name(raw_name)

            task_data = TaskRunData(
                task_name=task_name,
                raw_task_name=raw_name,
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
            tasks.append(task_data)

        return tasks

    def _find_task_logfile(self, task_data, cycle_log_dir):
        raw_name = task_data.raw_task_name

        for c in (f"{raw_name}_prep.log", f"{raw_name}.log"):
            p = os.path.join(cycle_log_dir, c)
            if os.path.exists(p):
                return p

        return None

    def _get_task_file_list_from_logs(self, log_path):
        """
        Reads a task log, extracts paths (files or dirs), and returns a flat list of file paths.
        """
        files_from_logs = parse_output_files_from_log(log_path, self.data_root)
        flat_files = self._expand_directories(files_from_logs)
        return flat_files

    def _populate_task_file_inventory(self, cycle_obj):
        for task_data in cycle_obj.tasks:
            if not task_data.logfile or not os.path.exists(task_data.logfile):
                continue

            files = self._get_task_file_list_from_logs(task_data.logfile)
            task_data.files = self._create_file_inventory_data(files)
            # self._inspect_file_system_info(task_data.files)
            # self._inspect_file_content(task_data.files)


    def _normalize_task_name(self, raw_name):
        parts = raw_name.split('_')
        prefix = parts[0]
        if prefix in self.VALID_PREFIXES:
            return prefix, raw_name[len(prefix)+1:]
        return 'unknown', raw_name


    def _expand_directories(self, rel_paths):
        """
        Converts a list of relative paths into a full set of file paths.
        Expands directories recursively for .nc and .bufr files.
        Returns a list of relative paths.
        """
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

    def _create_file_inventory_data(self, rel_paths):
        """
        Converts a list of relative paths into FileInventoryData objects.
        Does not inspect file contents.
        Only sets basic metadata (category, obs_space_name, rel_path).
        """
        inventory = []
        for rel in rel_paths:
            path_parts = rel.split(os.sep)
            category = path_parts[-2] if len(path_parts) > 1 else "unknown"
            obs_space = self._clean_obs_space_name(path_parts[-1])

            inventory.append(FileInventoryData(
                rel_path=rel,
                category=category,
                obs_space_name=obs_space,
                integrity="DECLARED",
                size_bytes=0,
                mtime=0,
                obs_count=0,
                error_msg=None,
                properties={},
                stats=[],
                domain=None
            ))
        return inventory


    def _clean_obs_space_name(self, filename):
        prefixes = "|".join(self.VALID_PREFIXES)
        pattern = rf"^(?:{prefixes})\.t[0-9]{{2}}z\.(.+)\.(?:nc|bufr)$"
        m = re.match(pattern, filename, re.IGNORECASE)
        if m:
            return m.group(1)
        return os.path.splitext(filename)[0]
