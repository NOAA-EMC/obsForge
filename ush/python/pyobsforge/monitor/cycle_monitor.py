import os
from logging import getLogger
# Assuming MonitorDB and MonitoredTask are in a location accessible by pyobsforge
from pyobsforge.monitor.monitor_db import MonitorDB
from pyobsforge.monitor.monitored_task import MonitoredTask
from pyobsforge.monitor import monitor_util

logger = getLogger("CycleMonitor")

class CycleMonitor:
    """
    Handles the complete monitoring and logging of tasks for a single timestamp (date/cycle).
    """

    def __init__(self, data_root, db: MonitorDB, task_cfgs: dict):
        self.data_root = data_root
        self.db = db
        self.task_cfgs = task_cfgs
        self.monitored_tasks = self._load_monitored_tasks()

    # --- Methods from the original ObsforgeMonitor ---

    def _load_monitored_tasks(self):
        """Initializes MonitoredTask objects from configuration."""
        tasks = {}
        for name, info in self.task_cfgs.items():
            tasks[name] = MonitoredTask(
                name=name,
                logfile_template=info["logfile_template"],
                obs_path_template=info["obs_path_template"],
            )
        return tasks

    def _resolve_logfile(self, task_cfg, date, cycle):
        template = task_cfg["logfile_template"]
        return os.path.join(
            self.data_root,
            template.format(date=date, cycle=cycle)
        )

    def _resolve_obs_path(self, task_cfg, date, cycle, run_type):
        template = task_cfg["obs_path_template"]
        return os.path.join(
            self.data_root,
            template.format(date=date, cycle=cycle, run_type=run_type)
        )
    
    # ------------------------------------------------------------------
    # The execution method for a single cycle
    # ------------------------------------------------------------------

    def run_cycle(self, date: str, cycle: str):
        """Processes all tasks for the given date and cycle."""
        logger.info(f"→ BEGIN timestamp date={date} cycle={cycle}")

        # Note: The iteration uses the configuration passed at initialization
        for task_name, task_cfg in self.task_cfgs.items():
            self._inspect_task(task_name, task_cfg, date, cycle)
            
        logger.info(f"← END timestamp date={date} cycle={cycle}")

    def _inspect_task(self, name, cfg, date, cycle):
        task = self.monitored_tasks[name]
        logfile = self._resolve_logfile(cfg, date, cycle)

        try:
            task_run_id = task.log_task_run(self.db, logfile)
        except Exception:
            logger.error(f"[{name}] Could not log task run")
            return

        # Note: run_type must be present in the config passed to CycleMonitor
        run_type = cfg.get("run_type", "gdas") 

        # Handle auto categories
        if cfg.get("categories") == "auto":
            obs_root = self._resolve_obs_path(cfg, date, cycle, run_type)
            category_map = monitor_util.detect_categories(obs_root)
        else:
            category_map = cfg["categories"]

        task.log_task_run_details(self.db, task_run_id, category_map)
        logger.info(f"[{name}] Completed monitoring")
