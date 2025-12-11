import os
import glob
import re
import logging

from pyobsforge.monitor.monitor_db import MonitorDB
import pyobsforge.monitor.timeutil as timeutil
from pyobsforge.monitor.cycle_monitor import CycleMonitor

logger = logging.getLogger("ObsforgeMonitor")


class ObsforgeMonitor:
    """
    The Core Monitoring Application.
    Pure business logic. Independent of wxflow/Rocoto.
    """
    def __init__(self, config):
        self.config = config

        # Initialize Database
        self.db = MonitorDB(config['database'])

        # Initialize the Cycle Monitor (Delegates actual processing)
        self.cycle_monitor = CycleMonitor(
            data_root=config['data_root'],
            db=self.db,
            task_cfgs=config['tasks']
        )

    def scan_for_new_cycles(self, latest_db_cycle=None):
        """
        Scans filesystem for cycles missing from DB.
        VALIDATES that at least one log file exists before adding to queue.
        """
        data_root = self.config['data_root']
        cycles_to_process = set()

        # 1. Identify run types (gdas, gfs)
        target_run_types = set()
        for task_name, task_cfg in self.config['tasks'].items():
            if 'run_type' in task_cfg:
                target_run_types.add(task_cfg['run_type'])

        # 2. Scan for each run_type
        for rtype in target_run_types:
            # A. Get Checklist
            existing_in_db = self.db.get_existing_cycles(rtype)

            # B. Scan Disk
            pattern = os.path.join(data_root, f"{rtype}.2*")
            candidate_dirs = glob.glob(pattern)
            dir_regex = re.compile(rf"^{re.escape(rtype)}\.(\d{{8}})$")

            for d_path in candidate_dirs:
                folder_name = os.path.basename(d_path)
                match = dir_regex.match(folder_name)
                if not match:
                    continue

                date_str = match.group(1)

                cycle_dirs = glob.glob(os.path.join(d_path, "[0-2][0-9]"))

                for c_path in cycle_dirs:
                    cyc_str = os.path.basename(c_path)

                    # C. Gap Check
                    if (date_str, cyc_str) in existing_in_db:
                        continue

                    # D. NEW: Zombie Check
                    # Check if *any* task log exists for this cycle.
                    # If logs are missing, it's a "Zombie" cycle (data exists, logs scrubbed).
                    # We skip it so we don't spam the logs forever.
                    if self._has_valid_logs(date_str, cyc_str):
                        cycles_to_process.add((date_str, cyc_str))

        sorted_cycles = sorted(list(cycles_to_process))

        if sorted_cycles:
            first = f"{sorted_cycles[0][0]}.{sorted_cycles[0][1]}"
            last  = f"{sorted_cycles[-1][0]}.{sorted_cycles[-1][1]}"
            logger.info(
                f"Scan complete. Found {len(sorted_cycles)} valid missing cycles. "
                f"Processing range: {first} -> {last}"
            )
        else:
            logger.info("Scan complete. Database is synchronized (ignoring cycles with missing logs).")

        return sorted_cycles

    def _has_valid_logs(self, date, cycle):
        """
        Helper: Returns True if at least one task log file exists for this date/cycle.
        """
        for task_name, task_cfg in self.config['tasks'].items():
            # Resolve the log path template
            template = task_cfg.get("logfile_template", "")
            if not template:
                continue

            # Construct the full path
            # Note: We rely on the standard data_root join logic
            log_rel_path = template.format(date=date, cycle=cycle)
            full_log_path = os.path.join(self.config['data_root'], log_rel_path)

            if os.path.exists(full_log_path):
                return True

        return False

    def run(self):
        logger.info("=== Obsforge Monitor Starting ===")
        timestamps = []

        # PRIORITY 1: Explicit Time Range (Standalone specific)
        if "time_range" in self.config:
            start = self.config['time_range']['start']
            end = self.config['time_range']['end']
            timestamps = list(timeutil.iter_timestamps(start, end))
            logger.info(f"Mode: Explicit Range ({len(timestamps)} cycles defined in YAML)")

        # PRIORITY 2: Single Cycle (Workflow/Rocoto)
        # 'PDY' is injected by the workflow environment
        elif "PDY" in self.config:
            pdy_raw = self.config['PDY']
            cyc_raw = self.config['cyc']
            ts = timeutil.normalize_rocoto_timestamp(pdy_raw, cyc_raw)
            timestamps = [timeutil.parse_timestamp(ts)]
            logger.info(f"Mode: Workflow Single Cycle ({ts})")

        # PRIORITY 3: Auto-Update (Standalone Default)
        else:
            logger.info("Mode: Auto-Update (Scanning filesystem)")

            # Log current state
            oldest = self.db.get_oldest_cycle()
            latest = self.db.get_latest_cycle()

            if oldest and latest:
                old_str = f"{oldest[0]}.{oldest[1]}"
                lat_str = f"{latest[0]}.{latest[1]}"
                logger.info(f"Current DB Range: {old_str} to {lat_str}")
            else:
                logger.info("Current DB status: Empty")

            # Scan for work
            timestamps = self.scan_for_new_cycles(latest)

            if not timestamps:
                logger.info("No new cycles found on disk.")

        # Execute
        for date, cycle in timestamps:
            self.cycle_monitor.run_cycle(date, cycle)

