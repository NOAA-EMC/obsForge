#!/usr/bin/env python3
import sys
import os
import argparse
import logging
import yaml

# --- UPDATED IMPORTS ---
from pyobsforge.monitor.database.monitor_db import MonitorDB
# SWITCH: Use the new DiscoveryScanner
from pyobsforge.monitor.scanner.discovery_scanner import DiscoveryScanner

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("MonitorUpdate")

def load_config(config_path):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def main():
    parser = argparse.ArgumentParser(description="Update ObsForge Monitor DB from filesystem")
    parser.add_argument("--config", required=True, help="Path to monitor_config.yaml")
    parser.add_argument("--db", required=True, help="Path to SQLite DB file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logging.getLogger("DiscoveryScanner").setLevel(logging.DEBUG)

    config = load_config(args.config)
    monitor_cfg = config.get('obsforgemonitor', {})
    
    # 1. Initialize DB
    db = MonitorDB(args.db)
    
    # 2. Initialize Scanner (Discovery Mode)
    data_root = monitor_cfg.get('data_root')
    # Note: We pass 'tasks' from config, but DiscoveryScanner mostly ignores it now
    tasks_map = monitor_cfg.get('tasks', {})
    
    scanner = DiscoveryScanner(data_root, tasks_map)
    
    # 3. Get existing cycles
    existing_cycles = db.get_all_run_cycles()
    logger.info(f"Database contains {len(existing_cycles)} cycles. Scanning for updates...")

    # 4. Run Scan
    new_count = 0
    # scanner.scan_filesystem yields CycleData objects, just like the old one
    for cycle_data in scanner.scan_filesystem(known_cycles=existing_cycles):
        logger.info(f"Persisting Cycle: {cycle_data.date} {cycle_data.cycle:02d}")
        
        for task in cycle_data.tasks:
            # Register Metadata
            t_id = db.get_or_create_task(task.task_name)
            
            # Log Run
            tr_id, action = db.log_task_run(
                task_id=t_id,
                date=cycle_data.date,
                cycle=cycle_data.cycle,
                run_type=task.run_type,
                logfile=task.logfile,
                start_time=task.start_time,
                end_time=task.end_time,
                runtime_sec=task.runtime_sec,
                notes=task.notes
            )
            
            # Log Obs Counts
            # The DiscoveryScanner has already filtered for valid/OK files and summed counts here
            for cat_name, files_dict in task.detailed_counts.items():
                cat_id = db.get_or_create_category(cat_name)
                for space_name, obs_count in files_dict.items():
                    if obs_count >= 0:
                        s_id = db.get_or_create_obs_space(space_name, cat_id)
                        db.set_task_obs_space_mapping(t_id, s_id)
                        db.log_task_run_detail(tr_id, s_id, obs_count)

        new_count += 1

    logger.info(f"Update Complete. Processed {new_count} cycles.")

if __name__ == "__main__":
    main()
