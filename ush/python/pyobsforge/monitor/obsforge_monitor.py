import os
import glob
import re
import logging
from pyobsforge.monitor.monitor_db import MonitorDB
import pyobsforge.monitor.timeutil as timeutil
from pyobsforge.monitor.cycle_monitor import CycleMonitor

logger = logging.getLogger("ObsforgeMonitor")

class ObsforgeMonitor:
    def __init__(self, config):
        self.config = config
        self.db = MonitorDB(config['database'])
        
        self.cycle_monitor = CycleMonitor(
            data_root=config['data_root'], 
            db=self.db, 
            task_cfgs=config['tasks']
        )

    def scan_for_new_cycles(self, latest_db_cycle):
        """
        Scans data_root for cycles newer than latest_db_cycle.
        Strictly matches directories based on 'run_type' found in config['tasks'].
        """
        data_root = self.config['data_root']
        new_cycles = set() # Use a set to avoid duplicates if gdas and gfs have same dates

        # 1. Identify all unique run_types we care about (e.g., 'gdas', 'gfs')
        target_run_types = set()
        for task_name, task_cfg in self.config['tasks'].items():
            if 'run_type' in task_cfg:
                target_run_types.add(task_cfg['run_type'])
        
        logger.debug(f"Scanning for run types: {target_run_types}")

        # 2. Determine cutoff timestamp
        if latest_db_cycle:
            cutoff_dt = timeutil.parse_timestamp(latest_db_cycle[0] + latest_db_cycle[1])
            logger.info(f"Searching for data newer than: {cutoff_dt}")
        else:
            logger.info("Database is empty. Scanning all available directories.")
            cutoff_dt = None

        # 3. Scan for each run_type specifically
        for rtype in target_run_types:
            # Pattern: data_root/gdas.YYYYMMDD
            # We look for gdas.2* to filter initially by filesystem
            pattern = os.path.join(data_root, f"{rtype}.2*")
            candidate_dirs = glob.glob(pattern)

            # Compile regex for strict validation: e.g. ^gdas\.(\d{8})$
            dir_regex = re.compile(rf"^{re.escape(rtype)}\.(\d{{8}})$")

            for d_path in candidate_dirs:
                folder_name = os.path.basename(d_path)
                
                # Strict Match: Check if folder matches "gdas.20251120" exactly
                match = dir_regex.match(folder_name)
                if not match:
                    continue
                
                date_str = match.group(1) # Extract 20251120

                # Check subdirectories for cycles (00, 06, 12, 18)
                cycle_dirs = glob.glob(os.path.join(d_path, "[0-2][0-9]"))
                
                for c_path in cycle_dirs:
                    cyc_str = os.path.basename(c_path) # e.g. "00"
                    
                    # Check timestamp
                    this_dt = timeutil.parse_timestamp(date_str + cyc_str)
                    
                    if cutoff_dt is None or this_dt > cutoff_dt:
                        new_cycles.add((date_str, cyc_str))

        # Convert set back to sorted list
        sorted_cycles = sorted(list(new_cycles))
        logger.info(f"Scan complete. Found {len(sorted_cycles)} new cycle(s) to process.")
        return sorted_cycles

    def run(self):
        logger.info("=== Obsforge Monitor Starting ===")
        timestamps = []

        # PRIORITY 1: Explicit Time Range (Standalone specific)
        if "time_range" in self.config:
            start = self.config.time_range.start
            end = self.config.time_range.end
            timestamps = list(timeutil.iter_timestamps(start, end))
            logger.info(f"Mode: Explicit Range ({len(timestamps)} cycles defined in YAML)")

        # PRIORITY 2: Single Cycle (Workflow/Rocoto)
        # 'PDY' is injected by the workflow environment, never present in standalone YAML
        elif "PDY" in self.config:
            pdy_raw = self.config.PDY
            cyc_raw = self.config.cyc
            ts = timeutil.normalize_rocoto_timestamp(pdy_raw, cyc_raw)
            timestamps = [timeutil.parse_timestamp(ts)]
            logger.info(f"Mode: Workflow Single Cycle ({ts})")

        # PRIORITY 3: Auto-Update (Standalone Default)
        # If no range is defined and no PDY exists, we assume we must find new work.
        else:
            logger.info("Mode: Auto-Update (No range or PDY found - scanning filesystem)")
            
            # 1. Get the last thing we successfully monitored
            last_processed = self.db.get_latest_cycle()
            
            # 2. Find everything on disk newer than that
            timestamps = self.scan_for_new_cycles(last_processed)
            
            if not timestamps:
                logger.info("No new cycles found on disk.")

        # Execute
        for date, cycle in timestamps:
            logger.info(f"Processing cycle: {date}.{cycle}")
            self.cycle_monitor.run_cycle(date, cycle)

