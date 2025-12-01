import os
from logging import getLogger

from wxflow import Task

from pyobsforge.monitor.monitored_task import MonitoredTask
from pyobsforge.monitor.monitor_db import MonitorDB


logger = getLogger(__name__.split('.')[-1])


class ObsforgeMonitor(Task):
    """
    WxFlow Task that *monitors* previously completed ObsForge tasks.

    Although it is a wxflow Task, it does NOT run tasks — 
    instead it inspects log output from another workflow and stores results.
    """

    # ------------------------------------------------------------------
    def __init__(self, config):
        super().__init__(config)     # <<< wxflow requirement

        self.config = config

        # --------------------------------------------------------------
        # Initialize database
        # --------------------------------------------------------------
        db_path = config.database
        logger.info(f"[Monitor] Opening MonitorDB at: {db_path}")
        self.db = MonitorDB(db_path)

        # --------------------------------------------------------------
        # Load monitored tasks from YAML-defined config
        # --------------------------------------------------------------
        if "tasks" not in config:
            raise ValueError("Monitor config must include a 'tasks' section.")

        self.monitored_tasks = self._load_monitored_tasks(config.tasks)
        logger.info(f"[Monitor] Loaded {len(self.monitored_tasks)} monitored tasks.")

    # ==================================================================
    # INTERNAL: load tasks
    # ==================================================================
    def _load_monitored_tasks(self, tasks_cfg):
        """
        Convert YAML dictionary into { name : MonitoredTask }.
        """
        tasks = {}

        for name, entry in tasks_cfg.items():
            try:
                task = MonitoredTask.from_yaml(name, entry)
                tasks[name] = task
                logger.info(f"[Monitor] Loaded MonitoredTask '{name}'")
            except Exception as e:
                logger.error(f"[Monitor] Failed to load task '{name}': {e}")

        return tasks

    # ==================================================================
    # Main workflow entry point for wxflow
    # ==================================================================
    def run(self):
        """
        WxFlow entry point.
        This method MUST exist because wxflow calls it.

        It inspects all configured tasks and logs results into the DB.
        """
        logger.info("[Monitor] Running ObsforgeMonitor as wxflow Task.")
        results = self.inspect_all_tasks()
        logger.info(f"[Monitor] Finished inspecting all tasks. Results: {results}")
        return results

    # ==================================================================
    # Inspectors (called by run())
    # ==================================================================
    def inspect_all_tasks(self):
        """
        Inspect all monitored tasks in sequential order.
        """
        results = {}
        for name in self.monitored_tasks:
            results[name] = self.inspect_task(name)
        return results

    # ------------------------------------------------------------------
    def inspect_task(self, name):
        """
        Inspect one monitored task:
            - parse log file
            - insert task_run
            - inspect category directories
            - insert task_run_details
        """
        if name not in self.monitored_tasks:
            raise KeyError(f"Monitored task '{name}' not found.")

        task = self.monitored_tasks[name]
        logger.info(f"[Monitor] === Inspecting task '{name}' ===")

        # ----------------------------------------------------------
        # 1. Parse its logfile → inserts task_run record
        # ----------------------------------------------------------
        try:
            task_run_id = task.log_task_run(self.db)
        except Exception as e:
            logger.error(f"[Monitor] Error during log_task_run for '{name}': {e}")
            return None

        # ----------------------------------------------------------
        # 2. Parse category dirs → inserts detail records
        # ----------------------------------------------------------
        try:
            task.log_task_run_details(self.db, task_run_id)
        except Exception as e:
            logger.error(f"[Monitor] Error parsing categories for '{name}': {e}")

        logger.info(f"[Monitor] === Finished '{name}' (task_run_id={task_run_id}) ===")

        return task_run_id

    # ==================================================================
    # Optional convenience function
    # ==================================================================
    def print_task_run_summary(self, task_run_id):
        logger.info(f"--- Summary for task_run_id={task_run_id} ---")
        rows = self.db.fetch_run_details(task_run_id)

        if not rows:
            logger.info("   No details found.")
            return

        for r in rows:
            logger.info(
                f"  obs_space={r['obs_space']:<25s} "
                f"count={r['obs_count']:<8d} "
                f"runtime={r['runtime_sec']}"
            )
