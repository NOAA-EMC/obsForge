import os
from datetime import datetime
# from .monitor_util import parse_job_log, elapsed_to_seconds
# from pyobsforge.task.monitor_util import *
from pyobsforge.task.log_file_parser import *


class MonitoredTask:
    """
    Base class for monitored tasks in the ObsForge monitoring system.
    """

    def __init__(self, name, logfile, nc_dir, dump_task_config=None, logger=None):
        self.name = name
        self.logfile = logfile
        self.nc_dir = nc_dir
        self.dump_task_config = dump_task_config
        self.logger = logger
        self.task_db_id = 3  # TODO: set it from db

    # --------------------------
    # Logging helper
    # --------------------------

    def info(self, msg):
        if self.logger:
            self.logger.info(msg)
        else:
            print(msg)

    # --------------------------
    # Core job-run logging logic
    # --------------------------

    def log_task_run(self, db):
        """
        Parse the task's log file and insert a task-run record into the database.

        db: database object with method log_task_run(...)
        """

        log_path = self.logfile
        job_info = parse_job_log(log_path, f"{self.name}.sh")

        self.info("===========================================")
        self.info(f"log_path ==>>>>> {log_path}")
        self.info(job_info["start_date"])
        self.info(job_info["end_date"])
        self.info(job_info["error_code"])
        self.info(job_info["elapsed_time"])
        self.info(f'Elapsed time: {elapsed_to_seconds(job_info["elapsed_time"])} seconds')
        self.info(f'Parsed job_info = {job_info}')
        self.info("===========================================")

        # task_id = self.dump_task_config.get("task_id", self.task_db_id)

        today = datetime.utcnow().date()
        cycle = job_info["cycle"]
        run_type = job_info["run_type"]
        start_date = job_info["start_date"]
        end_date = job_info["end_date"]

        # Write to DB
        task_run_id = db.log_task_run(
            task_id=self.task_db_id,
            date=today.isoformat(),
            cycle=cycle,
            run_type=run_type,
            start_time=start_date.isoformat(),
            end_time=end_date.isoformat(),
            notes=None
        )

        self.info(f"logged task run id ==>>>>> {task_run_id}")
        self.info("===========================================")

        return task_run_id

    def log_task_run_details(self, db, task_run_id, obs_type):
        """
        Parse all obs-space files in the task's nc_dir and log 
        detailed observation statistics to the database.

        db: database handle
        task_run_id: integer returned from log_task_run()
        obs_type: e.g. "marine", "sst", etc.
        """

        from pyobsforge.task.monitor_util import parse_obs_dir, print_obs_space_description

        # ---------------------------------------------------------
        # Process the observation directory for obs files
        # ---------------------------------------------------------
        results = parse_obs_dir(obs_type, self.nc_dir)

        self.info(f"Processed obs type: {obs_type}")
        self.info(f"Found {len(results)} obs-spaces in directory: {self.nc_dir}")

        for obs_space, obs_space_info in results.items():

            filename = obs_space_info["filename"]
            n_obs = obs_space_info["n_obs"]

            # Pretty-print for humans
            print_obs_space_description(
                obs_type,
                obs_space,
                filename,
                n_obs,
            )

            # ---------------------------------------------------------
            # TODO: Replace hardcoded obs_space_id with lookup table
            # ---------------------------------------------------------
            obs_space_id = 7

            # ---------------------------------------------------------
            # Log to DB
            # ---------------------------------------------------------
            db.log_task_run_detail(
                task_run_id=task_run_id,
                obs_space_id=obs_space_id,
                obs_count=n_obs,
                runtime_sec=0.0,   # TODO: eventually compute/parse runtime
            )

            self.info(
                f"Logged number of obs for file '{filename}': {n_obs}"
            )

    # --------------------------
    # Pretty-print
    # --------------------------

    def __repr__(self):
        return (
            f"<MonitoredTask name={self.name}, "
            f"logfile={self.logfile}, "
            f"nc_dir={self.nc_dir}>"
        )

