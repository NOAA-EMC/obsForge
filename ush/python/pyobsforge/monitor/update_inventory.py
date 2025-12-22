#!/usr/bin/env python3
import sys
import argparse
import logging
import json

from pyobsforge.monitor.database.monitor_db import MonitorDB
from pyobsforge.monitor.scanner.discovery_scanner import DiscoveryScanner


def configure_logging(debug_mode: bool):
    """
    Forcefully reconfigures logging to ensure DEBUG messages appear.
    """
    # 1. Determine Level
    level = logging.DEBUG if debug_mode else logging.INFO
    
    # 2. Get Root Logger
    root = logging.getLogger()
    root.setLevel(level)
    
    # 3. Nuke existing handlers (The critical fix)
    # If basicConfig was called anywhere else (even implicitly), this clears it.
    if root.handlers:
        for h in root.handlers:
            root.removeHandler(h)
            
    # 4. Create a fresh, clean Console Handler
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S")
    handler.setFormatter(formatter)
    root.addHandler(handler)
    
    # 5. Force Specific Loggers to inherit or explicitly set level
    # This overrides any module-level settings that might be stuck at INFO
    logging.getLogger("UpdateInventory").setLevel(level)
    logging.getLogger("DiscoveryScanner").setLevel(level)
    logging.getLogger("MonitorDB").setLevel(level)
    
    # 6. Silence noisy libraries (Optional but recommended)
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    logging.getLogger("PIL").setLevel(logging.WARNING)

    if debug_mode:
        # If you don't see this line, the function isn't running or debug_mode is False
        root.debug(">>> LOGGING RECONFIGURED: DEBUG MODE ENABLED <<<")

def main():
    parser = argparse.ArgumentParser(description="Step 1: Scan Filesystem & Record Physical Inventory")
    parser.add_argument("--db", required=True, help="Path to SQLite DB file")
    parser.add_argument("--data-root", required=True, help="Root directory of the data")
    parser.add_argument("--debug", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # CALL THIS FIRST
    configure_logging(args.debug)
    
    logger = logging.getLogger("UpdateInventory")
    logger.debug(f"Arguments parsed. DB: {args.db}")

    logger.info(f"Initializing DB: {args.db}")
    db = MonitorDB(args.db)
    
    logger.info(f"Scanning Root: {args.data_root}")
    scanner = DiscoveryScanner(args.data_root)

    new_count = 0
    # Scan every cycle found in logs
    for cycle_data in scanner.scan_filesystem(): 
        logger.info(f"Persisting Cycle: {cycle_data.date} {cycle_data.cycle:02d}")
        
        for task in cycle_data.tasks:
            t_id = db.get_or_create_task(task.task_name)
            
            # 1. Log Execution
            tr_id, action = db.log_task_run(
                task_id=t_id, date=cycle_data.date, cycle=cycle_data.cycle, 
                run_type=task.run_type, job_id=task.job_id, status=task.status, 
                exit_code=task.exit_code, attempt=task.attempt, host=task.host, 
                logfile=task.logfile, start_time=task.start_time, 
                end_time=task.end_time, runtime_sec=task.runtime_sec
            )
            
            # 2. Log Physical Inventory (Raw Evidence)
            for f in task.files:
                cat_id = db.get_or_create_category(f.category)
                s_id = db.get_or_create_obs_space(f.obs_space_name, cat_id)
                
                meta_json = json.dumps(f.properties) if f.properties else None
                
                db.log_file_inventory(
                    task_run_id=tr_id,
                    obs_space_id=s_id,
                    path=f.rel_path,
                    integrity=f.integrity,
                    size=f.size_bytes,
                    obs_count=f.obs_count,
                    error_msg=f.error_msg,
                    metadata_json=meta_json
                )
        new_count += 1

    logger.info(f"Scan Complete. Processed {new_count} cycles.")

if __name__ == "__main__":
    main()
