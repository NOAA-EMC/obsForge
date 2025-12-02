import os
from datetime import datetime
from typing import Dict

from logging import getLogger
logger = getLogger("MonitoredTask")

from pyobsforge.monitor.monitor_db import MonitorDB
import pyobsforge.monitor.log_file_parser as log_file_parser
import pyobsforge.monitor.monitor_util as monitor_util


class MonitoredTask:
    """
    A monitored workflow task.
    Parse its log → create task_run
    Parse its obs directories → create task_run_details
    """

    def __init__(self, name: str, logfile_template: str,
                 obs_path_template: str):
        self.name = name
        self.logfile_template = logfile_template
        self.obs_path_template = obs_path_template

    # ---------------------------------------------------------
    def log_task_run(self, db: MonitorDB, logfile: str) -> int:
        """
        Parse the log file and insert task_run.
        """

        try:
            info = log_file_parser.parse_job_log(logfile, self.name)
        except Exception as e:
            logger.error(f"[{self.name}] Error parsing log file {logfile}: {e}")
            raise

        date = info["start_date"].strftime("%Y%m%d")
        cycle = int(info["cycle"])
        runtime_sec = log_file_parser.elapsed_to_seconds(info["elapsed_time"])

        task_id = db.get_or_create_task(self.name)

        task_run_id = db.log_task_run(
            task_id=task_id,
            date=date,
            cycle=cycle,
            run_type=info["run_type"],
            logfile=logfile,
            start_time=info["start_date"].isoformat(),
            end_time=info["end_date"].isoformat(),
            runtime_sec=runtime_sec,
        )

        logger.info(f"[{self.name}] Logged task_run id={task_run_id}")
        return task_run_id

    # ---------------------------------------------------------
    def log_task_run_details(self, db: MonitorDB, task_run_id: int,
                             category_map: Dict[str, str]):
        """
        For each category, parse obs-space dirs and insert task_run_details.
        """

        for category_name, nc_dir in category_map.items():
            try:
                results = monitor_util.parse_obs_dir(category_name, nc_dir)
            except Exception as e:
                logger.error(f"[{self.name}] Failed to parse {nc_dir}: {e}")
                continue

            logger.info(f"[{self.name}] {category_name} → {len(results)} obs-spaces")

            category_id = db.get_or_create_category(category_name)

            for obs_space, info in results.items():
                obs_space_id = db.get_or_create_obs_space(obs_space, category_id)
                db.log_task_run_detail(
                    task_run_id,
                    obs_space_id,
                    obs_count=info["n_obs"],
                    runtime_sec=0.0,
                )

