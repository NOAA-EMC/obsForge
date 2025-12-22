#!/usr/bin/env python3
import sys
import argparse
import logging
import json

from pyobsforge.monitor.database.monitor_db import MonitorDB
from pyobsforge.monitor.database.db_reader import DBReader
from pyobsforge.monitor.scanner.discovery_scanner import DiscoveryScanner

def configure_logging(debug_mode: bool):
    root = logging.getLogger()
    level = logging.DEBUG if debug_mode else logging.INFO
    root.setLevel(level)
    if root.handlers:
        for h in root.handlers: root.removeHandler(h)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S"))
    root.addHandler(handler)
    logging.getLogger("matplotlib").setLevel(logging.WARNING)

def main():
    parser = argparse.ArgumentParser(description="Step 1: Scan Filesystem & Record Physical Inventory")
    parser.add_argument("--db", required=True, help="Path to SQLite DB file")
    parser.add_argument("--data-root", required=True, help="Root directory of the data")
    parser.add_argument("--debug", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    configure_logging(args.debug)
    logger = logging.getLogger("UpdateInventory")

    logger.info(f"Initializing DB Connection: {args.db}")
    db_writer = MonitorDB(args.db)
    db_reader = DBReader(args.db) # New Read-Only connection
    
    logger.info(f"Scanning Root: {args.data_root}")
    scanner = DiscoveryScanner(args.data_root)
    
    # Optional Optimization: Pass existing cycles to skip parsing old logs
    # existing = db_reader.get_all_run_cycles_set()
    
    new_count = 0
    # Note: Pass known_cycles=existing if you want to enable skipping
    for cycle_data in scanner.scan_filesystem(known_cycles=None): 
        logger.info(f"Persisting Cycle: {cycle_data.date} {cycle_data.cycle:02d}")
        
        for task in cycle_data.tasks:
            t_id = db_writer.get_or_create_task(task.task_name)
            
            tr_id, action = db_writer.log_task_run(
                task_id=t_id, date=cycle_data.date, cycle=cycle_data.cycle, 
                run_type=task.run_type, job_id=task.job_id, status=task.status, 
                exit_code=task.exit_code, attempt=task.attempt, host=task.host, 
                logfile=task.logfile, start_time=task.start_time, 
                end_time=task.end_time, runtime_sec=task.runtime_sec
            )
            
            for f in task.files:
                cat_id = db_writer.get_or_create_category(f.category)
                s_id = db_writer.get_or_create_obs_space(f.obs_space_name, cat_id)
                
                meta_json = json.dumps(f.properties) if f.properties else None
                
                db_writer.log_file_inventory(
                    task_run_id=tr_id,
                    obs_space_id=s_id,
                    path=f.rel_path,
                    integrity=f.integrity,
                    size=f.size_bytes,
                    obs_count=f.obs_count,
                    error_msg=f.error_msg,
                    metadata_json=meta_json
                )
        
        # Batch Commit per cycle
        db_writer.commit()
        new_count += 1

    logger.info(f"Scan Complete. Processed {new_count} cycles.")

if __name__ == "__main__":
    main()
