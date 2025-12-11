import os
import sqlite3
from logging import getLogger
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

        # --- NEW CALL: Ensure all Obs Spaces are mapped to a single Task ---
        self._initialize_task_mappings()

    def _initialize_task_mappings(self):
        """
        Initializes task and category/obs space IDs in the DB, and sets the
        Task <-> Obs Space map to enforce the disjoint set rule.

        This relies on the fact that MonitoredTask.log_task_run_details
        uses MonitoredTask.get_or_create_obs_space, which in turn calls
        db.get_or_create_category, etc., to get/create all necessary IDs.
        """
        logger.info("Initializing task, category, and obs space mappings...")

        for task_name, task_cfg in self.task_cfgs.items():
            task_id = self.db.get_or_create_task(task_name)

            # Use 'auto' detection for initialization if needed, or get
            # the full list of potential categories/spaces from the config.
            if task_cfg.get("categories") == "auto":
                logger.warning(f"[{task_name}] Cannot pre-map obs spaces using 'auto' detection.")
                continue

            for category_name, obs_space_map in task_cfg.get("obs_spaces_map", {}).items():
                category_id = self.db.get_or_create_category(category_name)

                for obs_space_name in obs_space_map.keys():
                    obs_space_id = self.db.get_or_create_obs_space(obs_space_name, category_id)

                    try:
                        # This call attempts to insert the mapping into task_obs_space_map.
                        # If obs_space_id is already mapped to a different task,
                        # the UNIQUE constraint will be violated, confirming the error.
                        self.db.set_task_obs_space_mapping(task_id, obs_space_id)
                    except sqlite3.IntegrityError:
                        logger.error(
                            f"FATAL SCHEMA VIOLATION: Obs Space '{obs_space_name}' (ID {obs_space_id}) "
                            f"is already mapped to another task. Your configuration violates the "
                            f"'disjoint sets per task' rule."
                        )
                        raise

        logger.info("Task mappings initialized successfully.")

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

    def _make_paths_absolute(self, category_map: dict, base: str) -> dict:
        """Convert relative category paths to absolute paths using obs_root as the base."""
        abs_map = {}
        for key, rel_path in category_map.items():
            # If already absolute, keep as is
            if os.path.isabs(rel_path):
                abs_map[key] = rel_path
            else:
                abs_map[key] = os.path.abspath(os.path.join(base, rel_path))
        return abs_map

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

        # Optional: Add a check to prevent crashing if file is missing
        if not os.path.exists(logfile):
            logger.debug(f"[{name}] Log file not found: {logfile}")
            return

        try:
            task_run_id = task.log_task_run(self.db, logfile,
                                            logical_date=date,
                                            logical_cycle=cycle)
        except sqlite3.IntegrityError:
            logger.warning(f"[{name}] Task run for date={date}, cycle={cycle} already logged. Skipping details.")
            return
        except Exception:
            logger.error(f"[{name}] Could not log task run")
            return

        # Note: run_type must be present in the config passed to CycleMonitor
        run_type = cfg.get("run_type", "gdas")

        obs_root = self._resolve_obs_path(cfg, date, cycle, run_type)

        if cfg.get("categories") == "auto":
            category_map = monitor_util.detect_categories(obs_root)
        else:
            rel_map = cfg["categories"]
            category_map = self._make_paths_absolute(rel_map, obs_root)

        # log_task_run_details logs the processed spaces. This will enforce
        # UNIQUE(task_run_id, obs_space_id) for each individual detail entry.
        task.log_task_run_details(self.db, task_run_id, category_map)
        logger.info(f"[{name}] Completed monitoring")
