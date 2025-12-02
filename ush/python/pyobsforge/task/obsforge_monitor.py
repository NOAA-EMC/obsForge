#!/usr/bin/env python3
import os
from logging import getLogger

from wxflow import Task

from pyobsforge.monitor.monitor_db import MonitorDB
from pyobsforge.monitor.monitored_task import MonitoredTask
import pyobsforge.monitor.timeutil as timeutil
from pyobsforge.monitor import monitor_util


logger = getLogger("ObsforgeMonitor")


class ObsforgeMonitor(Task):
    """
    Master monitoring task (wxflow Task).
    Supports:
      • Standalone batch monitoring (time_range)
      • Rocoto (single timestamp pdy/cyc)
    """

    def __init__(self, config):
        super().__init__(config)

        self.config = config
        self.db = MonitorDB(config.database)
        self.tasks = self._load_tasks(config.tasks)

    # ---------------------------------------------------------
    def _load_tasks(self, tasks_cfg):
        tasks = {}
        for name, info in tasks_cfg.items():
            tasks[name] = MonitoredTask(
                name=name,
                logfile_template=info["logfile_template"],
                obs_path_template=info["obs_path_template"],
            )
        return tasks

    # ---------------------------------------------------------
    def _resolve_logfile(self, task_cfg, date, cycle):
        template = task_cfg["logfile_template"]
        return os.path.join(
            self.config.data_root,
            template.format(date=date, cycle=cycle)
        )

    def _resolve_obs_path(self, task_cfg, date, cycle, run_type):
        template = task_cfg["obs_path_template"]
        return os.path.join(
            self.config.data_root,
            template.format(date=date, cycle=cycle, run_type=run_type)
        )

    # ---------------------------------------------------------
    def run(self):
        logger.info("=== Obsforge Monitor Starting ===")

        # Determine timestamps
        if "time_range" in self.config:
            start = self.config.time_range["start"]
            end = self.config.time_range["end"]
            timestamps = list(timeutil.iter_timestamps(start, end))
            logger.info(f"Standalone mode: {len(timestamps)} timestamps")
        else:
            ts = f"{self.config.pdy}{self.config.cyc}"
            timestamps = [timeutil.parse_timestamp(ts)]
            logger.info(f"Rocoto mode: timestamp {ts}")

        # Process timestamps
        for date, cycle in timestamps:
            logger.info(f"→ BEGIN timestamp date={date} cycle={cycle}")

            for task_name, task_cfg in self.config.tasks.items():
                self._inspect_task(task_name, task_cfg, date, cycle)

    # ---------------------------------------------------------
    def _inspect_task(self, name, cfg, date, cycle):
        task = self.tasks[name]

        logfile = self._resolve_logfile(cfg, date, cycle)

        try:
            task_run_id = task.log_task_run(self.db, logfile)
        except Exception:
            logger.error(f"[{name}] Could not log task run")
            return

        run_type = cfg.get("run_type", "gdas")

        # Handle auto categories
        if cfg.get("categories") == "auto":
            obs_root = self._resolve_obs_path(cfg, date, cycle, run_type)
            category_map = monitor_util.detect_categories(obs_root)
        else:
            category_map = cfg["categories"]

        task.log_task_run_details(self.db, task_run_id, category_map)

        logger.info(f"[{name}] Completed monitoring")

