#!/usr/bin/env python3
"""
Step 1: Scan & Register Inventory.

This script scans the filesystem for log files and output files,
and registers them in the ObsForge database.
"""

import argparse
import logging
import sys

from database.monitor_db import MonitorDB
from scanner.inventory_scanner import InventoryScanner
from scanner.persistence import ScannerStateReader


def configure_logging(debug_mode: bool):
    """Configures the root logger."""
    root = logging.getLogger()
    level = logging.DEBUG if debug_mode else logging.INFO
    root.setLevel(level)

    # Remove existing handlers to avoid duplicates
    if root.handlers:
        for h in root.handlers:
            root.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)

    # Silence noisy libraries
    logging.getLogger("matplotlib").setLevel(logging.WARNING)


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Step 1: Scan & Register Inventory"
    )
    parser.add_argument("--db", required=True, help="Path to SQLite DB")
    parser.add_argument("--data-root", required=True, help="Root scan dir")
    parser.add_argument("--debug", action="store_true", help="Debug logging")
    parser.add_argument(
        "--limit-cycles", 
        type=int, 
        default=None, 
        help="Only process the N most recent cycles"
    )

    args = parser.parse_args()
    configure_logging(args.debug)
    logger = logging.getLogger("UpdateInventory")

    logger.info(f"DB: {args.db}")
    
    db_writer = MonitorDB(args.db)

    # 1. Fetch State (For mtime comparison only)
    logger.info("Fetching previous state...")
    state_reader = ScannerStateReader(args.db)
    known_state = state_reader.get_known_state()

    logger.info(f"Scanning: {args.data_root}")
    scanner = InventoryScanner(args.data_root, known_state=known_state)

    cycles_found = 0
    total_files = 0
    total_new_updated = 0
    total_skipped = 0

    for cycle_data in scanner.scan_filesystem(limit=args.limit_cycles):
        cycles_found += 1
        logger.info(
            f"Processing: {cycle_data.date} {cycle_data.cycle:02d}"
        )
        
        for task in cycle_data.tasks:
            t_id = db_writer.get_or_create_task(task.task_name)
            
            # Log Task Run
            tr_id, _ = db_writer.log_task_run(
                task_id=t_id,
                date=cycle_data.date,
                cycle=cycle_data.cycle,
                run_type=task.run_type,
                job_id=task.job_id,
                status=task.status,
                exit_code=task.exit_code,
                attempt=task.attempt,
                host=task.host,
                logfile=task.logfile,
                start_time=task.start_time,
                end_time=task.end_time,
                runtime_sec=task.runtime_sec
            )

            for f in task.files:
                total_files += 1
                cat_id = db_writer.get_or_create_category(f.category)
                s_id = db_writer.get_or_create_obs_space(
                    f.obs_space_name, cat_id
                )

                # Log file
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

                if file_id:
                    total_new_updated += 1

                    if f.properties and 'schema' in f.properties:
                        db_writer.register_file_schema(
                            s_id, f.properties['schema']
                        )

                    if f.domain:
                        db_writer.log_file_domain(
                            file_id,
                            f.domain.get('start'),
                            f.domain.get('end'),
                            f.domain.get('min_lat'),
                            f.domain.get('max_lat'),
                            f.domain.get('min_lon'),
                            f.domain.get('max_lon')
                        )

                    if f.stats:
                        db_writer.log_variable_statistics(file_id, f.stats)
                else:
                    total_skipped += 1

        # Commit AFTER every cycle to release DB locks for Inspectors
        db_writer.commit()

    logger.info(
        f"Done. Cycles: {cycles_found} | Files Scanned: {total_files}"
    )
    logger.info(
        f"Summary: {total_new_updated} New/Updated, "
        f"{total_skipped} Unchanged/Skipped."
    )


if __name__ == "__main__":
    main()
