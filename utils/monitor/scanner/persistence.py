import logging
import sqlite3
from typing import Any, Dict

from typing import Dict

from database.monitor_db import MonitorDB
# from scanner.models import CycleData
from scanner.models import CycleData, TaskRunData, FileInventoryData


logger = logging.getLogger("ScannerPersistence")
# logger = logging.getLogger("Registrar")
# logger = logging.getLogger("InventoryRegistrar")

logger = logging.getLogger("Registrar")


class ScannerStateReader:
    """
    Reads the current state of the File Inventory from the database.

    COMPATIBILITY MODE:
    Returns a dictionary of dictionaries to match the existing
    InventoryScanner logic.
    Structure: { '/path/to/file': {'mtime': 1735689000} }
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    def get_known_state(self) -> Dict[str, Dict[str, Any]]:
        """
        Retrieves tracked files and their metadata.
        """
        known_files = {}
        conn = None

        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()

            # Select columns needed for the scanner's Gatekeeper logic
            sql = "SELECT file_path, file_modified_time FROM file_inventory"

            try:
                cur.execute(sql)
                rows = cur.fetchall()
            except sqlite3.OperationalError:
                logger.warning(
                    "Inventory table not found. Assuming empty state."
                )
                return {}

            for row in rows:
                path = row[0]
                mtime = row[1]

                # The existing scanner expects a dictionary, not a raw int
                known_files[path] = {
                    'mtime': mtime if mtime is not None else 0
                }

            logger.info(
                f"Loaded inventory state: {len(known_files)} existing files."
            )

        except Exception as e:
            logger.error(f"Failed to load scanner state: {e}")
            return {}

        finally:
            if conn:
                conn.close()

        return known_files


class Registrar:
    """
    Persists scanned inventory objects into the database.

    This class owns *workflow-level* persistence logic.
    MonitorDB remains a low-level SQL helper only.
    """

    def __init__(self, db: MonitorDB):
        self.db = db
        # Reporting counters
        self.cycles = 0
        self.files = 0
        self.new_or_updated = 0
        self.skipped = 0

    def persist_cycle(self, cycle: CycleData):
        """Persist a single CycleData object."""
        self.cycles += 1
        for task in cycle.tasks:
            self._persist_task(task, cycle)
        # Commit once per cycle
        self.db.commit()

    def _persist_task(self, task: TaskRunData, cycle: CycleData):
        task_id = self.db.get_or_create_task(task.task_name)
        task_run_id, _ = self.db.log_task_run(
            task_id=task_id,
            date=cycle.date,
            cycle=cycle.cycle,
            run_type=task.run_type,
            job_id=task.job_id,
            status=task.status,
            exit_code=task.exit_code,
            attempt=task.attempt,
            host=task.host,
            logfile=task.logfile,
            start_time=task.start_time,
            end_time=task.end_time,
            runtime_sec=task.runtime_sec,
        )
        self._persist_files(task_run_id, task.files)

    def _persist_files(self, task_run_id, files):
        for f in files:
            self.files += 1

            cat_id = self.db.get_or_create_category(f.category)
            obs_space_id = self.db.get_or_create_obs_space(
                f.obs_space_name, cat_id
            )

            file_id = self.db.log_file_inventory(
                task_run_id=task_run_id,
                obs_space_id=obs_space_id,
                path=f.rel_path,
                integrity=f.integrity,
                size=f.size_bytes,
                mtime=f.mtime,
                obs_count=f.obs_count,
                error_msg=f.error_msg,
                properties=f.properties,
            )

            if file_id:
                self.new_or_updated += 1
                self._persist_file_details(file_id, obs_space_id, f)
            else:
                self.skipped += 1

    def _persist_file_details(self, file_id, obs_space_id, f: FileInventoryData):
        if f.properties and "schema" in f.properties:
            self.db.register_file_schema(obs_space_id, f.properties["schema"])
        if f.domain:
            self.db.log_file_domain(
                file_id,
                f.domain.get("start"),
                f.domain.get("end"),
                f.domain.get("min_lat"),
                f.domain.get("max_lat"),
                f.domain.get("min_lon"),
                f.domain.get("max_lon"),
            )
        if f.stats:
            self.db.log_variable_statistics(file_id, f.stats)

    def report(self) -> Dict[str, int]:
        """Return summary statistics."""
        return {
            "cycles": self.cycles,
            "files": self.files,
            "new_or_updated": self.new_or_updated,
            "skipped": self.skipped,
        }


class oldRegistrar:
    """
    Persists scanned inventory objects into the database.

    This class owns *workflow-level* persistence logic.
    MonitorDB remains a low-level SQL helper only.
    """

    def __init__(self, db: MonitorDB):
        self.db = db

        # Reporting counters
        self.cycles = 0
        self.files = 0
        self.new_or_updated = 0
        self.skipped = 0

    def persist_cycle(self, cycle: CycleData):
        """Persist a single CycleData object."""
        self.cycles += 1

        for task in cycle.tasks:
            task_id = self.db.get_or_create_task(task.task_name)

            task_run_id, _ = self.db.log_task_run(
                task_id=task_id,
                date=cycle.date,
                cycle=cycle.cycle,
                run_type=task.run_type,
                job_id=task.job_id,
                status=task.status,
                exit_code=task.exit_code,
                attempt=task.attempt,
                host=task.host,
                logfile=task.logfile,
                start_time=task.start_time,
                end_time=task.end_time,
                runtime_sec=task.runtime_sec,
            )

            for f in task.files:
                self.files += 1

                cat_id = self.db.get_or_create_category(f.category)
                obs_space_id = self.db.get_or_create_obs_space(
                    f.obs_space_name,
                    cat_id,
                )

                file_id = self.db.log_file_inventory(
                    task_run_id=task_run_id,
                    obs_space_id=obs_space_id,
                    path=f.rel_path,
                    integrity=f.integrity,
                    size=f.size_bytes,
                    mtime=f.mtime,
                    obs_count=f.obs_count,
                    error_msg=f.error_msg,
                    properties=f.properties,
                )

                if file_id:
                    self.new_or_updated += 1

                    if f.properties and "schema" in f.properties:
                        self.db.register_file_schema(
                            obs_space_id,
                            f.properties["schema"],
                        )

                    if f.domain:
                        self.db.log_file_domain(
                            file_id,
                            f.domain.get("start"),
                            f.domain.get("end"),
                            f.domain.get("min_lat"),
                            f.domain.get("max_lat"),
                            f.domain.get("min_lon"),
                            f.domain.get("max_lon"),
                        )

                    if f.stats:
                        self.db.log_variable_statistics(
                            file_id,
                            f.stats,
                        )
                else:
                    self.skipped += 1

        # Important: commit *once per cycle*
        self.db.commit()

    def report(self) -> Dict[str, int]:
        """Return summary statistics."""
        return {
            "cycles": self.cycles,
            "files": self.files,
            "new_or_updated": self.new_or_updated,
            "skipped": self.skipped,
        }

