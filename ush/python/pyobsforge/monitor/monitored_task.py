# pyobsforge/monitor/monitored_task.py

import os
from typing import Dict, Any
from datetime import datetime

from logging import getLogger
logger = getLogger(__name__.split('.')[-1])

from pyobsforge.monitor.monitor_db import MonitorDB
# from pyobsforge.monitor import log_file_parser, monitor_util
from pyobsforge.monitor.log_file_parser import *
from pyobsforge.monitor.monitor_util import *


class MonitoredTask:
    """
    Represents a monitored workflow task. Its job:
    - parse the task log
    - insert a row into task_runs
    - parse all categories/obs-spaces
    - insert rows into task_run_details
    """

    def __init__(self, name: str, logfile: str, categories: Dict[str, str]):
        self.name = name
        self.logfile = logfile
        self.categories = categories  # category_name -> obs-space directory

    # ----------------------------------------------------------
    # High-level API used by ObsforgeMonitor
    # ----------------------------------------------------------

    def run_monitoring(self, db: MonitorDB) -> int:
        """
        Perform full monitoring:
        1. parse log -> insert into task_runs
        2. parse categories -> insert into task_run_details
        """
        task_run_id = self._log_task_run(db)
        self._log_task_run_details(db, task_run_id)
        return task_run_id

    # ----------------------------------------------------------
    # Step 1: Insert task_run
    # ----------------------------------------------------------

    def _log_task_run(self, db: MonitorDB) -> int:
        """
        Create a task_run in the DB.
        """
        # Parse job log
        try:
            job_info = parse_job_log(self.logfile, self.name)
        except Exception as e:
            logger.error(f"[{self.name}] Failed to parse log file: {self.logfile}: {e}")
            raise

        cycle = job_info.get("cycle")
        run_type = job_info.get("run_type")
        start_time = job_info.get("start_date")
        end_time = job_info.get("end_date")

        runtime_sec = elapsed_to_seconds(
            job_info.get("elapsed_time")
        )

        # Ensure task exists
        task_id = db.get_or_create_task(self.name)

        # Insert row
        task_run_id = db.log_task_run(
            task_id=task_id,
            date=datetime.utcnow().date().isoformat(),
            cycle=cycle,
            run_type=run_type,
            logfile=self.logfile,
            start_time=start_time.isoformat() if start_time else None,
            end_time=end_time.isoformat() if end_time else None,
            runtime_sec=runtime_sec,
            notes=None
        )

        logger.info(f"[{self.name}] Logged task_run id={task_run_id}")
        return task_run_id

    # ----------------------------------------------------------
    # Step 2: Insert task_run_details
    # ----------------------------------------------------------

    def _log_task_run_details(self, db: MonitorDB, task_run_id: int):
        """
        For each category: parse obs-space files and log results.
        """
        for category_name, nc_dir in self.categories.items():
            try:
                results = parse_obs_dir(category_name, nc_dir)
                # results = monitor_util.parse_obs_dir(category_name, nc_dir)
            except Exception as e:
                logger.error(f"[{self.name}] Failed to parse dir {nc_dir}: {e}")
                continue

            logger.info(f"[{self.name}] Category '{category_name}': {len(results)} obs-spaces")

            # Ensure category exists
            category_id = db.get_or_create_category(category_name)

            for obs_space_name, info in results.items():
                n_obs = info.get("n_obs", 0)

                # Pretty print
                print_obs_space_description(
                    # category_name, obs_space_name, '', n_obs
                    category_name, obs_space_name, info
                )

                # Ensure obs_space exists
                obs_space_id = db.get_or_create_obs_space(
                    obs_space_name, category_id
                )

                # Insert detail row
                db.log_task_run_detail(
                    task_run_id=task_run_id,
                    obs_space_id=obs_space_id,
                    obs_count=n_obs,
                    runtime_sec=0.0
                )

                logger.info(
                    f"[{self.name}] Logged obs-space '{obs_space_name}' with {n_obs} obs"
                )

