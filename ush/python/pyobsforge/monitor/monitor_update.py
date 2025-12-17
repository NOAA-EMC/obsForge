import argparse
import logging
import sys
import yaml
import os
from pyobsforge.monitor.database.monitor_db import MonitorDB
from pyobsforge.monitor.scanner.scanner import ObsForgeScanner

# Configure logging to show timestamps and levels clearly
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout
)
logger = logging.getLogger("MonitorUpdate")

def main():
    parser = argparse.ArgumentParser(description="Update ObsForge Monitor DB")
    parser.add_argument("-c", "--config", required=True, help="Path to monitor_config.yaml")
    parser.add_argument("--debug", action="store_true", help="Enable verbose debug logging")
    parser.add_argument("--force", action="store_true", help="Force re-scan of all cycles (ignore existing DB state)")
    args = parser.parse_args()

    # Set level based on flag
    log_level = logging.DEBUG if args.debug else logging.INFO
    logger.setLevel(log_level)
    logging.getLogger("ObsForgeScanner").setLevel(log_level)
    
    if not os.path.exists(args.config):
        logger.error(f"Config file not found: {args.config}")
        sys.exit(1)

    with open(args.config, 'r') as f:
        raw_config = yaml.safe_load(f)

    config = raw_config.get('obsforgemonitor', raw_config)
    db_path = config.get('database')
    data_root = config.get('data_root')
    tasks_config = config.get('tasks', {})

    if not db_path or not data_root:
        logger.error("Invalid Config: Missing 'database' or 'data_root'.")
        sys.exit(1)

    logger.info(f"Connecting to DB: {db_path}")
    db = MonitorDB(db_path)
    
    # 1. Prepare Incremental Filter
    existing_cycles = None
    if not args.force:
        try:
            existing_cycles = db.get_all_run_cycles()
            logger.info(f"Incremental Mode: Found {len(existing_cycles)} existing run-cycles in DB.")
        except AttributeError:
            logger.warning("DB missing 'get_all_run_cycles'. Performing full scan.")
    else:
        logger.info("Force Mode: Scanning ALL directories (ignoring existing DB state).")

    logger.info(f"Scanning Data Root: {data_root}")
    scanner = ObsForgeScanner(data_root, tasks_config)

    count_tasks = 0

    # 2. Run Scan
    for cycle_data in scanner.scan_filesystem(known_cycles=existing_cycles):
        logger.info(f"Processing Cycle: {cycle_data.date} {cycle_data.cycle:02d}")
        
        for task in cycle_data.tasks:
            try:
                # A. Log Task Run
                task_id = db.get_or_create_task(task.task_name)
                safe_run_type = task.run_type if task.run_type else "unknown"
                
                row_id, action = db.log_task_run(
                    task_id=task_id,
                    date=cycle_data.date,
                    cycle=cycle_data.cycle,
                    run_type=safe_run_type,
                    logfile=task.logfile,
                    start_time=task.start_time,
                    end_time=task.end_time,
                    runtime_sec=task.runtime_sec,
                    notes=task.notes
                )
                
                logger.info(f"  [{action}] Task Run: {task.task_name} ({safe_run_type}) {task.runtime_sec}s")

                # B. Log Detailed Obs Stats
                if task.detailed_counts:
                    for cat_name, obs_map in task.detailed_counts.items():
                        
                        # Category Resolution
                        cat_id = db.get_or_create_category(cat_name)
                        logger.debug(f"    [CAT] Resolved '{cat_name}' (ID: {cat_id})")
                        
                        for obs_name, count in obs_map.items():
                            # Obs Space Definition
                            obs_space_id = db.get_or_create_obs_space(obs_name, cat_id)
                            logger.debug(f"      [DEF] Obs Space '{obs_name}' -> ID {obs_space_id}")
                            
                            # Mapping (Task Definition -> Obs Space Definition)
                            # This ensures task_obs_space_map is populated
                            db.set_task_obs_space_mapping(task_id, obs_space_id)
                            logger.debug(f"      [MAP] Linked Task {task_id} -> ObsSpace {obs_space_id}")

                            # Statistics (Task Run Execution -> Obs Space)
                            # This populates task_run_details
                            db.log_task_run_detail(row_id, obs_space_id, count)
                            
                            # Log the payload at INFO so it's easy to see
                            logger.info(f"      [STATS] {obs_name:<25} : {count:>10} obs")

                count_tasks += 1
            except Exception as e:
                logger.error(f"Failed to log task {task.task_name}: {e}")

    logger.info(f"Update Complete. Tasks Processed: {count_tasks}")

if __name__ == "__main__":
    main()
