import os
from datetime import datetime

from pyobsforge.monitor.log_file_parser import (
    parse_job_log,
    elapsed_to_seconds,
)
from pyobsforge.monitor.monitor_util import (
    parse_obs_dir,
    print_obs_space_description,
)


class MonitoredTask:
    """
    Represents a monitored task loaded from YAML configuration.
    A task has:
      - a name
      - one logfile per task run
      - several categories (sst, sss, adt, icec, etc.)
        each mapping to an nc_dir containing a set of obs-space NetCDF files.
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------
    def __init__(self, name, logfile, categories):
        self.name = name
        self.logfile = logfile               # e.g. "/path/to/task.log"
        self.categories = categories         # dict: { "sst": "/path/to/dir", ... }

    @classmethod
    def from_yaml(cls, name, yaml_entry):
        """
        Construct a MonitoredTask object from one YAML task entry.
        Expected structure:

            tasks:
              marinedump:
                logfile: "/path/log"
                categories:
                  sst: "/dir1"
                  sss: "/dir2"
        """
        if "logfile" not in yaml_entry:
            raise ValueError(f"Task '{name}' missing required 'logfile' field.")

        if "categories" not in yaml_entry:
            raise ValueError(f"Task '{name}' must define a 'categories:' section.")

        categories = yaml_entry["categories"]

        if not isinstance(categories, dict) or not categories:
            raise ValueError(
                f"Task '{name}' has malformed or empty 'categories' section."
            )

        # Validate category → string mapping
        for cat, nc_dir in categories.items():
            if not isinstance(nc_dir, str):
                raise ValueError(
                    f"Category '{cat}' in task '{name}' must map directly "
                    f"to an nc_dir string."
                )

        return cls(name=name, logfile=yaml_entry["logfile"], categories=categories)

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------
    def info(self, msg):
        """Allow tasks to log via module-level logger."""
        from logging import getLogger
        logger = getLogger(__name__.split('.')[-1])
        logger.info(f"[{self.name}] {msg}")

    def error(self, msg):
        from logging import getLogger
        logger = getLogger(__name__.split('.')[-1])
        logger.error(f"[{self.name}] {msg}")

    # ------------------------------------------------------------------
    # Task run logging
    # ------------------------------------------------------------------
    def log_task_run(self, db):
        """
        Parse the task's logfile, extract timing info, and log the task run.
        Returns: task_run_id (int)
        """
        logfile = self.logfile

        self.info(f"Parsing task log: {logfile}")

        try:
            job_info = parse_job_log(logfile, f"{self.name}.sh")
        except Exception as e:
            self.error(f"Failed to parse task logfile '{logfile}': {e}")
            raise

        # Extract fields
        try:
            start_date = job_info["start_date"]
            end_date = job_info["end_date"]
            runtime_sec = elapsed_to_seconds(job_info["elapsed_time"])
            cycle = job_info["cycle"]
            run_type = job_info["run_type"]
        except KeyError as e:
            self.error(f"Missing required field in job_info: {e}")
            raise

        today = datetime.utcnow().date().isoformat()

        # Insert into DB
        task_run_id = db.log_task_run(
            task_id=db.get_task_id(self.name),
            date=today,
            cycle=cycle,
            run_type=run_type,
            start_time=start_date.isoformat(),
            end_time=end_date.isoformat(),
            runtime_sec=runtime_sec,
            notes=None
        )

        self.info(f"Logged task_run_id={task_run_id} for {self.name}")
        return task_run_id

    # ------------------------------------------------------------------
    # Detail logging for each category
    # ------------------------------------------------------------------
    def log_task_run_details(self, db, task_run_id):
        """
        For each category (sst, adt, icec, etc.) parse the directory of obs-spaces,
        validate ioda files, extract obs counts, and log into DB.
        """
        for category, nc_dir in self.categories.items():
            self.info(f"Processing category '{category}' in directory: {nc_dir}")

            try:
                results = parse_obs_dir(category, nc_dir)
            except Exception as e:
                self.error(
                    f"Failed to parse obs directory for category '{category}': {e}"
                )
                continue  # Do not stop the entire task; move to next category

            if not results:
                self.info(f"No obs-spaces found in category '{category}'.")
                continue

            # Create collection name for the run: category + obs-spaces
            collection_id = db.ensure_space_collection(category, results.keys())

            self.info(
                f"Using obs-space collection ID {collection_id} "
                f"for category '{category}'"
            )

            # Now record one entry per obs-space
            for obs_space, info in results.items():
                filename = info["filename"]
                n_obs = info["n_obs"]

                # Pretty-print for humans
                print_obs_space_description(category, obs_space, filename, n_obs)

                # Every obs_space must be in obs_spaces table
                obs_space_id = db.ensure_obs_space(obs_space)

                db.log_task_run_detail(
                    task_run_id=task_run_id,
                    obs_space_id=obs_space_id,
                    obs_count=n_obs,
                    runtime_sec=0.0  # may add more later
                )

                self.info(
                    f"Logged {n_obs} obs in obs-space '{obs_space}' "
                    f"from file '{filename}'"
                )

    # ------------------------------------------------------------------
    def __repr__(self):
        return (
            f"MonitoredTask(name={self.name}, "
            f"logfile={self.logfile}, "
            f"categories={list(self.categories.keys())})"
        )


#################################################################
import os
from datetime import datetime
from pyobsforge.monitor.log_file_parser import *
from pyobsforge.monitor.monitor_util import parse_obs_dir, print_obs_space_description


class ooMonitoredTask:
    """
    Base class for a task that is monitored in ObsForge.
    Each task run may have multiple obs categories (sst, adt, icec, sss, etc.)
    """

    def __init__(self, name, task_config, log_file, nc_dirs):
        """
        name: task name, e.g., "marinedump.sh"
        task_config: currently unused, stores the original yaml config
        log_file: path to the task log file
        nc_dirs: dict mapping obs_category -> directory of obs-space files
        """
        self.name = name
        self.task_config = task_config  # currently unused; stores the config from yaml
        self.log_file = log_file
        self.nc_dirs = nc_dirs  # e.g., {'sst': '/path/to/sst', 'icec': '/path/to/icec'}
        print(f'PPPPPPPPPPPPPPPPPPPPPP nc_dirs = {self.nc_dirs}')

    # -------------------------------------------------------------
    # Logging a task run
    # -------------------------------------------------------------
    def log_task_run(self, db):
        """
        Parse the log file, extract start/end times, cycle, run_type,
        and store in the database. Returns task_run_id.
        """
        try:
            job_info = parse_job_log(self.log_file, f"{self.name}.sh")
        except Exception as e:
            print(f"ERROR parsing log file {self.log_file}: {e}")
            return None

        task_id = db.get_or_create_task(self.name)
        today = datetime.utcnow().date()

        # compute elapsed seconds
        runtime_sec = elapsed_to_seconds(job_info.get("elapsed_time", "00:00:00"))

        # Insert task run into DB
        task_run_id = db.log_task_run(
            task_id=task_id,
            date=today.isoformat(),
            cycle=job_info.get("cycle"),
            run_type=job_info.get("run_type"),
            start_time=job_info.get("start_date").isoformat(),
            end_time=job_info.get("end_date").isoformat(),
            runtime_sec=runtime_sec,
            notes=None,
            log_file=self.log_file
        )

        print(f"Logged task run {self.name}, id={task_run_id}")
        return task_run_id

    # -------------------------------------------------------------
    # Logging details per obs category
    # -------------------------------------------------------------
    def log_task_run_details(self, db, task_run_id):
        """
        For each obs category in nc_dirs, parse obs-space files, count observations,
        store in DB, and print.
        """
        for obs_category, nc_dir in self.nc_dirs.items():
            if not os.path.isdir(nc_dir):
                print(f"Directory not found for category {obs_category}: {nc_dir}")
                continue

            # Parse obs-space files
            results = parse_obs_dir(obs_category, nc_dir)
            print(f"Processed obs category '{obs_category}', found {len(results)} obs-spaces in {nc_dir}")

            # Create a category entry for this task run
            category_id = db.create_task_run_category(
                task_run_id=task_run_id,
                obs_category=obs_category,
                nc_dir=nc_dir
            )

            # Process each obs-space in the category
            obs_space_ids = []
            for obs_space_name, info in results.items():
                filename = info["filename"]
                n_obs = info["n_obs"]

                # Pretty-print for humans
                print_obs_space_description(obs_category, obs_space_name, filename, n_obs)

                # Ensure obs_space exists in DB
                obs_space_id = db.get_or_create_obs_space(obs_space_name)
                obs_space_ids.append(obs_space_id)

                # Log task_run_details
                db.log_task_run_detail(
                    category_id=category_id,
                    obs_space_id=obs_space_id,
                    obs_count=n_obs,
                    runtime_sec=0.0  # Could be filled if available
                )

                print(f"Logged obs-space '{obs_space_name}' ({n_obs} obs)")

            # Optionally create/update obs-space collection
            collection_id = db.get_or_create_obs_space_collection(obs_space_ids, description=f"{obs_category} collection")
            print(f"Updated obs-space collection for category '{obs_category}', collection_id={collection_id}")


###########################################################

import os
from datetime import datetime
import hashlib


def compute_collection_name(obs_type, obs_spaces):
    """
    Compute a compact, deterministic name for the obs-space collection.
    """
    key = obs_type + ":" + ",".join(sorted(obs_spaces))
    digest = hashlib.sha256(key.encode()).hexdigest()[:8]
    return f"{obs_type}:{digest}"

def get_or_create_obs_space_collection(db, obs_type, obs_spaces):
    col_name = compute_collection_name(obs_type, obs_spaces)

    description = f"{obs_type} collection of {', '.join(sorted(obs_spaces))}"

    collection_id = db.get_or_create_obs_space_collection(
        name=col_name,
        member_names=obs_spaces,
        description=description
    )

    # self.info(f"Obs-space collection '{col_name}' -> id={collection_id}")
    print(f"Obs-space collection '{col_name}' -> id={collection_id}")

    return collection_id


class oldMonitoredTask:
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
            log_file=self.logfile,
            notes=None
        )

        self.info(f"logged task run id ==>>>>> {task_run_id}")
        self.info("===========================================")

        return task_run_id

    def log_task_run_details(self, db, task_run_id, obs_type):
        """
        Parse obs-space files in this task's nc_dir and log detailed observation
        statistics (obs counts) to the database.
        """

        from pyobsforge.monitor.monitor_util import parse_obs_dir, print_obs_space_description

        results = parse_obs_dir(obs_type, self.nc_dir)

        obs_spaces = list(results.keys())  # names only

        # ---------------------------------------------------------
        # Find or create the obs-space collection
        # ---------------------------------------------------------
        collection_id = get_or_create_obs_space_collection(
            db=db,
            obs_type=obs_type,
            obs_spaces=obs_spaces
        )
        # self.info(f"Obs-space collection '{col_name}' -> id={collection_id}")

        self.info(f"Processed obs type: {obs_type}")
        self.info(f"Found {len(results)} obs-spaces in directory: {self.nc_dir}")
        self.info(f"Collection ID = {collection_id}")

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

            # Get or create obs_space_id
            obs_space_id = db.get_or_create_obs_space(obs_space)

            # Log detail record
            db.log_task_run_detail(
                task_run_id=task_run_id,
                obs_space_id=obs_space_id,
                obs_count=n_obs,
                runtime_sec=0.0,
            )

            self.info(f"Logged number of obs for '{filename}': {n_obs}")

    # --------------------------
    # Pretty-print
    # --------------------------

    def __repr__(self):
        return (
            f"<MonitoredTask name={self.name}, "
            f"logfile={self.logfile}, "
            f"nc_dir={self.nc_dir}>"
        )


class oldnewMonitoredTask:
    """
    Represents a task that produces a log file and one or more
    obs-categories, each with its own directory of IODA observation files.
    """

    def __init__(self, name, logfile, categories, logger=None):
        """
        name        : name of the task (e.g., "marinedump")
        logfile     : path to the log file for this task
        categories  : dict { obs_category : directory }
                      Example: { "icec": "/path/to/icec", "sst": "/path/to/sst" }
        logger      : optional logger
        """
        self.name = name
        self.logfile = logfile
        self.categories = categories   # dict category → directory
        self._logger = logger

    # ------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------
    def info(self, msg):
        if self._logger:
            self._logger.info(f"[{self.name}] {msg}")
        else:
            print(f"[{self.name}] {msg}")

    # ------------------------------------------------------------
    # Utility: compute deterministic shortened hash name for collection
    # ------------------------------------------------------------
    def compute_collection_name(self, obs_category, obs_spaces):
        """
        Deterministic compact collection name:
            {obs_category}:{8-char SHA256 digest}
        """
        key = obs_category + ":" + ",".join(sorted(obs_spaces))
        digest = hashlib.sha256(key.encode()).hexdigest()[:8]
        return f"{obs_category}:{digest}"

    # ------------------------------------------------------------
    # Create or retrieve obs-space collection
    # ------------------------------------------------------------
    def get_or_create_obs_space_collection(self, db, obs_category, obs_spaces):
        """
        Create an entry in obs_space_collections if it doesn't exist.
        Also adds obs_space members to obs_space_collection_members.
        """

        col_name = self.compute_collection_name(obs_category, obs_spaces)
        description = f"{obs_category} collection with members: {', '.join(sorted(obs_spaces))}"

        # Check whether it exists already
        collection_id = db.find_obs_space_collection(col_name)
        if collection_id is not None:
            self.info(f"Found existing obs-space collection '{col_name}' (id={collection_id})")
            return collection_id

        # Create new collection
        collection_id = db.insert_obs_space_collection(col_name, description)
        self.info(f"Created obs-space collection '{col_name}' (id={collection_id})")

        # Add member obs-spaces
        for sp in sorted(obs_spaces):
            obs_space_id = db.get_or_create_obs_space(sp)
            db.add_obs_space_to_collection(collection_id, obs_space_id)
            self.info(f"Added obs-space '{sp}' to collection '{col_name}'")

        return collection_id

    # ------------------------------------------------------------
    # Log a single category (obs_category → directory → obs_spaces)
    # ------------------------------------------------------------
    def log_task_run_category(self, db, task_run_id, obs_category, nc_dir):
        """
        For a given obs_category and nc_dir, parse the IODA files,
        build/find the collection, and log the obs counts.
        """
        from pyobsforge.monitor.monitor_util import parse_obs_dir, print_obs_space_description

        self.info(f"Processing obs-category '{obs_category}' in directory {nc_dir}")

        # Parse directory contents
        results = parse_obs_dir(obs_category, nc_dir)   # returns dict of obs_space → info
        obs_spaces = sorted(results.keys())
        self.info(f"Found {len(obs_spaces)} obs-spaces for category '{obs_category}'")

        # Create or retrieve collection
        collection_id = self.get_or_create_obs_space_collection(db, obs_category, obs_spaces)

        # Create task_run_category entry
        category_id = db.insert_task_run_category(
            task_run_id=task_run_id,
            obs_category=obs_category,
            collection_id=collection_id,
            directory=nc_dir
        )
        self.info(f"Logged task_run_category {category_id} for '{obs_category}'")

        # Log each obs-space for this category
        for obs_space, info in results.items():
            filename = info["filename"]
            n_obs = info["n_obs"]

            print_obs_space_description(obs_category, obs_space, filename, n_obs)

            obs_space_id = db.get_or_create_obs_space(obs_space)

            db.log_task_run_detail(
                task_run_id=task_run_id,
                category_id=category_id,
                obs_space_id=obs_space_id,
                obs_count=n_obs,
                runtime_sec=0.0    # Reserved for future use
            )

            self.info(f"Logged {n_obs} obs for '{obs_space}' in category '{obs_category}'")

    # ------------------------------------------------------------
    # Main entry point: log the entire task run
    # ------------------------------------------------------------
    def log_task_run(self, db, task_id):
        """
        Parse the task log file, write an entry to task_runs,
        and then process all obs_categories defined for this task.
        """
        from pyobsforge.monitor.log_file_parser import parse_job_log

        # Parse main job log
        job_info = parse_job_log(self.logfile, f"{self.name}.sh")

        cycle = job_info["cycle"]
        run_type = job_info["run_type"]
        start_date = job_info["start_date"]
        end_date = job_info["end_date"]

        # Store the log file path in the task_runs row
        today = datetime.utcnow().date().isoformat()

        task_run_id = db.log_task_run(
            task_id=task_id,
            date=today,
            cycle=cycle,
            run_type=run_type,
            start_time=start_date.isoformat(),
            end_time=end_date.isoformat(),
            runtime_sec=None,
            log_file=self.logfile,
            notes=None
        )

        self.info(f"Logged task_run {task_run_id} for task '{self.name}'")

        # Process each obs-category
        for obs_category, nc_dir in self.categories.items():
            self.log_task_run_category(db, task_run_id, obs_category, nc_dir)

        self.info(f"Completed logging for task_run {task_run_id}")
        return task_run_id

