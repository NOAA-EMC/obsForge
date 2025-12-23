#!/usr/bin/env python3
import sys
import argparse
import logging
import json

from pyobsforge.monitor.database.monitor_db import MonitorDB
from pyobsforge.monitor.scanner.persistence import ScannerStateReader
from pyobsforge.monitor.scanner.inventory_scanner import InventoryScanner

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
    parser = argparse.ArgumentParser(description="Step 1: Scan & Register Inventory")
    parser.add_argument("--db", required=True)
    parser.add_argument("--data-root", required=True)
    parser.add_argument("--debug", action="store_true")
    
    args = parser.parse_args()
    configure_logging(args.debug)
    logger = logging.getLogger("UpdateInventory")

    logger.info(f"DB: {args.db}")
    db_writer = MonitorDB(args.db)
    state_reader = ScannerStateReader(args.db)
    
    # 1. Fetch State (For Incremental Scanning)
    logger.info("Fetching state...")
    known_mtimes = state_reader.get_known_mtimes()
    
    # 2. Scan
    logger.info(f"Scanning: {args.data_root}")
    scanner = InventoryScanner(args.data_root, known_mtimes=known_mtimes)
    
    new_count = 0
    for cycle_data in scanner.scan_filesystem(): 
        logger.info(f"Persisting: {cycle_data.date} {cycle_data.cycle:02d}")
        
        for task in cycle_data.tasks:
            t_id = db_writer.get_or_create_task(task.task_name)
            tr_id, _ = db_writer.log_task_run(
                task_id=t_id, date=cycle_data.date, cycle=cycle_data.cycle, 
                run_type=task.run_type, job_id=task.job_id, status=task.status, 
                exit_code=task.exit_code, attempt=task.attempt, host=task.host, 
                logfile=task.logfile, start_time=task.start_time, 
                end_time=task.end_time, runtime_sec=task.runtime_sec
            )
            
            for f in task.files:
                cat_id = db_writer.get_or_create_category(f.category)
                s_id = db_writer.get_or_create_obs_space(f.obs_space_name, cat_id)
                
                # 3. Log Header & Lineage
                # Note: We pass properties so it can extract 'obs_source_files'
                file_id = db_writer.log_file_inventory(
                    task_run_id=tr_id,
                    obs_space_id=s_id,
                    path=f.rel_path,
                    integrity=f.integrity,
                    size=f.size_bytes,
                    mtime=f.mtime,
                    obs_count=f.obs_count,
                    error_msg=f.error_msg,
                    properties=f.properties 
                )
                
                # 4. INSTANT LEARNING (This is the missing link!)
                # Directly registers variables, units, and dims from this file
                if f.properties and 'schema' in f.properties:
                    db_writer.register_file_schema(s_id, f.properties['schema'])
                
                # 5. Log Metrics
                if f.domain:
                    db_writer.log_file_domain(
                        file_id, f.domain.get('start'), f.domain.get('end'),
                        f.domain.get('min_lat'), f.domain.get('max_lat'),
                        f.domain.get('min_lon'), f.domain.get('max_lon')
                    )
                if f.stats:
                    db_writer.log_variable_statistics(file_id, f.stats)
        
        db_writer.commit()
        new_count += 1

    logger.info(f"Done. Cycles: {new_count}")

if __name__ == "__main__":
    main()
