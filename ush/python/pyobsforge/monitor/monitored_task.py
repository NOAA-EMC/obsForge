import logging
import sqlite3
from typing import Dict

from pyobsforge.monitor.monitor_db import MonitorDB
import pyobsforge.monitor.log_file_parser as log_file_parser
import pyobsforge.monitor.monitor_util as monitor_util

logger = logging.getLogger("MonitoredTask")


class MonitoredTask:
    """
    A monitored workflow task.
    Parse its log -> create task_run
    Parse its obs directories -> create task_run_details
    """

    def __init__(self, name: str, logfile_template: str,
                 obs_path_template: str):
        self.name = name
        self.logfile_template = logfile_template
        self.obs_path_template = obs_path_template

    def log_task_run(self, db: MonitorDB, logfile: str,
                     logical_date: str, logical_cycle: str) -> int:

        try:
            info = log_file_parser.parse_job_log(logfile, self.name)
        except Exception as e:
            logger.error(f"[{self.name}] Error parsing log file {logfile}: {e}")
            raise

        # We trust the date/cycle that CycleMonitor gave us (from the directory structure)
        date = logical_date
        cycle = int(logical_cycle)

        runtime_sec = log_file_parser.elapsed_to_seconds(info["elapsed_time"])

        task_id = db.get_or_create_task(self.name)

        try:
            task_run_id = db.log_task_run(
                task_id=task_id,
                date=date,         # Inserting the LOGICAL date
                cycle=cycle,       # Inserting the LOGICAL cycle
                run_type=info["run_type"],
                logfile=logfile,
                start_time=info["start_date"].isoformat(), # We still keep actual start time for metadata
                end_time=info["end_date"].isoformat(),
                runtime_sec=runtime_sec,
            )
        except sqlite3.IntegrityError as e:
            logger.warning(
                f"[{self.name}] Skipping: Run already exists for {date}/{cycle}. Error: {e}"
            )
            raise

        logger.info(f"[{self.name}] Logged task_run id={task_run_id}")
        return task_run_id

    # ---------------------------------------------------------
    def log_task_run_details(self, db: MonitorDB, task_run_id: int,
                             category_map: Dict[str, str]):
        """
        For each category, parse obs-space dirs and insert task_run_details.
        """

        # --- NEW: Get task_id for mapping enforcement ---
        # We need the task_id to set the disjoint mapping constraint.
        task_id = db.get_or_create_task(self.name)

        for category_name, nc_dir in category_map.items():
            try:
                results = monitor_util.parse_obs_dir(category_name, nc_dir)
            except Exception as e:
                logger.error(f"[{self.name}] Failed to parse {nc_dir}: {e}")
                continue

            logger.info(f"[{self.name}] {category_name} -> {len(results)} obs-spaces")

            category_id = db.get_or_create_category(category_name)
            # logger.debug(f"[{self.name}] {category_name} -> id = {category_id}")

            for obs_space, info in results.items():
                obs_space_id = db.get_or_create_obs_space(obs_space, category_id)
                # logger.debug(f"[{self.name}] {obs_space} -> id = {obs_space_id}")

                try:
                    # 1. ENFORCE DISJOINT SETS: Map obs space to current task.
                    # If this obs space is already mapped to a different task, an IntegrityError occurs.
                    db.set_task_obs_space_mapping(task_id, obs_space_id)

                    # 2. LOG THE DETAIL: This enforces UNIQUE(task_run_id, obs_space_id)
                    db.log_task_run_detail(
                        task_run_id,
                        obs_space_id,
                        obs_count=info["n_obs"],
                        runtime_sec=0.0,
                    )
                except sqlite3.IntegrityError as e:
                    # Catch violation of UNIQUE(obs_space_id) in task_obs_space_map
                    # OR violation of UNIQUE(task_run_id, obs_space_id) in task_run_details
                    logger.error(
                        f"[{self.name}] Logging failed for obs space '{obs_space}': "
                        f"Integrity check failed. Possible violation: Obs space already owned by another task, "
                        f"or duplicate detail entry for this run. Error: {e}"
                    )
                    # We continue to the next obs_space detail rather than crashing the whole run
                    continue

