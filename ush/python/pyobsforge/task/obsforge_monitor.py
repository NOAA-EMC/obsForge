#!/usr/bin/env python3

from wxflow import Task
from typing import Dict, Any

from logging import getLogger
logger = getLogger(__name__.split('.')[-1])

from pyobsforge.monitor.monitor_db import MonitorDB
from pyobsforge.monitor.monitored_task import MonitoredTask


class ObsforgeMonitor(Task):
    """
    Wxflow Task that runs after dump tasks and logs monitoring info.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)   # required by wxflow

        self.config = config

        # These come from the flattened monitor_config.yaml
        self.db_path = config["database"]
        self.tasks_cfg = config.get("tasks", {})

        # Access to dump task configs (not used yet)
        self.dump_tasks = config.get("dump_tasks", {})

        # Build DB and monitored-task objects
        self.db = MonitorDB(self.db_path)
        self.monitored_tasks = self._load_monitored_tasks()

    # ------------------------------------------------------------------
    # Build MonitoredTask instances
    # ------------------------------------------------------------------
    def _load_monitored_tasks(self) -> Dict[str, MonitoredTask]:
        tasks = {}
        for name, info in self.tasks_cfg.items():
            logfile = info.get("logfile", "")
            categories = info.get("categories", {})

            task = MonitoredTask(
                name=name,
                logfile=logfile,
                categories=categories
            )
            tasks[name] = task

        return tasks

    # ------------------------------------------------------------------
    # Wxflow entrypoint
    # ------------------------------------------------------------------
    def run(self):
        """
        Called by wxflow / Rocoto.
        Perform monitoring for each configured task.
        """

        logger.info("Starting ObsforgeMonitor...")

        for task_name, task in self.monitored_tasks.items():
            logger.info(f"Monitoring task '{task_name}'")
            try:
                run_id = task.run_monitoring(self.db)
                logger.info(f"→ Completed monitoring for '{task_name}', run_id={run_id}")
            except Exception as e:
                logger.error(f"Monitoring failed for {task_name}: {e}")
                raise

        logger.info("ObsforgeMonitor complete.")

