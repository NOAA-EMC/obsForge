import os
import re
import logging

from datetime import datetime, date
from typing import List, Optional, Tuple, Set

from sqlalchemy import (
    # Column,
    # Integer,
    # String,
    # ForeignKey,
    # Date,
    # CheckConstraint,
    # UniqueConstraint,
    select,
)
# from sqlalchemy.orm import declarative_base, relationship, Session
from sqlalchemy.orm import Session

# from .db_base import Base  # SQLAlchemy declarative base
from .dataset_orm import (
    DatasetORM, 
    DatasetCycleORM, 
    DatasetObsSpaceORM,
    DatasetObsSpaceFileORM
)
from .obs_space_orm import ObsSpaceORM

from .file import File
from .obs_space import ObsSpace
from .dataset_cycle import DatasetCycle
from .dataset_obs_space import DatasetObsSpace
from .dataset_obs_space_file import DatasetObsSpaceFile


logger = logging.getLogger(__name__)


class Dataset:
    """
    Domain object representing a dataset.

    In-memory representation first.
    Persist explicitly using to_db(session).
    """

    def __init__(
        self,
        name: str,
        root_dir: str,
        id: Optional[int] = None
    ):
        self.id = id
        self.name = name
        self.root_dir = root_dir
        self.cycles: List[DatasetCycle] = []
        self.obs_spaces: List[DatasetObsSpaces] = []
        self.obs_space_files: dict[str, dict["DatasetCycle", "DatasetObsSpaceFile"]]


    def to_orm(self) -> "DatasetORM":
        return DatasetORM(
            id=self.id,
            name=self.name,
            root_dir=self.root_dir,
        )

    def self_to_db(self, session):
        """
        Persist the Dataset itself in the database.
        """
        from .dataset_orm import DatasetORM  # Import ORM class here only

        logger.info("Dataset.self_to_db")

        # Check if this dataset already exists
        existing = session.scalar(
            select(DatasetORM).where(DatasetORM.name == self.name)
        )

        if existing:
            # Update the id to link children later
            self.id = existing.id
            return existing

        logger.info("Dataset.self_to_db --- orm")
        # Create new ORM object for Dataset
        orm_obj = DatasetORM(
            name=self.name,
            root_dir=self.root_dir
        )

        session.add(orm_obj)
        session.flush()  # Ensure id is assigned
        self.id = orm_obj.id
        return orm_obj


    # --------------------------------------------------------
    # In-memory operations
    # --------------------------------------------------------

    def add_cycle(self, cycle_date: date, cycle_hour: str):
        self.cycles.append(DatasetCycle(self, cycle_date, cycle_hour))

    def list_cycles(self) -> List[Tuple[date, str]]:
        return [(c.cycle_date, c.cycle_hour) for c in self.cycles]

    def cycles_summary(self) -> str:
        return (
            f"Dataset {self.name}: {len(self.cycles)} cycles "
            f"from {min(self.cycles, key=lambda c: (c.cycle_date, c.cycle_hour)).cycle_date} "
            f"{min(self.cycles, key=lambda c: (c.cycle_date, c.cycle_hour)).cycle_hour} "
            f"to {max(self.cycles, key=lambda c: (c.cycle_date, c.cycle_hour)).cycle_date} "
            f"{max(self.cycles, key=lambda c: (c.cycle_date, c.cycle_hour)).cycle_hour}"
            if self.cycles else
            f"Dataset {self.name}: 0 cycles"
        )

    def obs_spaces_summary(self) -> str:
        return (
            f"Dataset {self.name}: {len(self.obs_spaces)} obs_spaces "
            # f"from {self.obs_spaces[0].obs_space.name} "
            # f"to {self.obs_spaces[-1].obs_space.name}"
            if self.obs_spaces else
            f"Dataset {self.name}: 0 obs_spaces"
        )


    def generate_obs_space_files_report(self) -> List[Tuple[str, str, str, int]]:
        """
        Generate a summary table of cycles and obs_spaces.

        Returns
        -------
        List of tuples: (dataset_name, first_cycle, last_cycle, obs_count)
        """
        from typing import List, Tuple
        from collections import defaultdict

        if not self.cycles or not self.obs_spaces:
            return []

        # Sort cycles
        sorted_cycles = sorted(self.cycles, key=lambda c: (c.cycle_date, c.cycle_hour))
        report = []

        # obs_space count per cycle
        obs_count_per_cycle = [
            (cycle, sum(1 for obs in self.obs_spaces if cycle in self.obs_space_files.get(obs.obs_space.name, {})))
            for cycle in sorted_cycles
        ]

        # Group contiguous cycles with same obs_count
        start_cycle, prev_count = obs_count_per_cycle[0]
        for i, (cycle, count) in enumerate(obs_count_per_cycle[1:], start=1):
            if count != prev_count:
                # Save previous block
                report.append((self.name, start_cycle, obs_count_per_cycle[i-1][0], prev_count))
                start_cycle, prev_count = cycle, count

        # Add last block
        report.append((self.name, start_cycle, obs_count_per_cycle[-1][0], prev_count))
        return report


    def print_obs_space_files_report(self):
        table = self.generate_obs_space_files_report()
        if not table:
            logger.info(f"{self.name}: no cycles or obs_spaces")
            return

        for dataset_name, first_cycle, last_cycle, obs_count in table:
            logger.info(
                f"{dataset_name}: "
                f"{first_cycle.cycle_date} {first_cycle.cycle_hour} → "
                f"{last_cycle.cycle_date} {last_cycle.cycle_hour} — "
                f"{obs_count} obs_spaces"
            )


    # --------------------------------------------------------
    # Persistence
    # --------------------------------------------------------
    # every entity that has id must:
    # 1. Convert to ORM
    # 2. Add to session
    # 3. Flush
    # 4. Copy generated id back to domain object

    def to_db(self, session: "Session") -> None:
        self.self_to_db(session)

        # Persist cycles
        for cycle in self.cycles:
            cycle.to_db(session)

        # Persist obs_spaces
        for dos in self.obs_spaces:
            dos.to_db(session)

        # Persist obs_space_files (2D)
        for obs_space_name, cycle_map in self.obs_space_files.items():
            for cycle, dosf in cycle_map.items():
                dosf.to_db(session)

    # UNFINISHED:
    @classmethod
    def from_db(cls, session: Session, dataset_id: int) -> Optional["Dataset"]:
        orm_obj = session.get(DatasetORM, dataset_id)
        if orm_obj is None:
            return None

        ds = cls(
            name=orm_obj.name,
            root_dir=orm_obj.root_dir,
            id=orm_obj.id,
        )

        for c in orm_obj.cycles:
            ds.cycles.append(
                DatasetCycle(
                    self,
                    cycle_date=c.cycle_date,
                    cycle_hour=c.cycle_hour,
                    id=c.id,
                )
            )

        return ds


    def register_cycles(self) -> None:
        """
        Scan self.root_dir and populate self.cycles.

        Expects directory layout:

            <root_dir>/
                gfs.20260204/
                    00/
                    06/
                    12/
                    18/
        """

        DATASET_DIR_PATTERN = re.compile(
            r"^(?P<name>[A-Za-z0-9_]+)\.(?P<date>\d{8})$"
        )

        logger.debug(f"Scanning cycles for dataset '{self.name}'")

        for entry in os.listdir(self.root_dir):

            entry_path = os.path.join(self.root_dir, entry)

            if not os.path.isdir(entry_path):
                continue

            match = DATASET_DIR_PATTERN.match(entry)
            if not match:
                continue

            dataset_name = match.group("name")
            cycle_date_str = match.group("date")

            # Only process directories for THIS dataset
            if dataset_name != self.name:
                continue

            try:
                cycle_date = datetime.strptime(
                    cycle_date_str, "%Y%m%d"
                ).date()
            except ValueError:
                logger.warning(f"Invalid date format: {cycle_date_str}")
                continue

            for hour_entry in os.listdir(entry_path):

                hour_path = os.path.join(entry_path, hour_entry)

                if not os.path.isdir(hour_path):
                    continue

                if hour_entry not in DatasetCycle.VALID_HOURS:
                    logger.warning(
                        f"Ignoring invalid cycle hour '{hour_entry}' "
                        f"in {entry_path}"
                    )
                    continue

                logger.debug(
                    f"Found cycle: {self.name} "
                    f"{cycle_date_str} {hour_entry}"
                )

                self.add_cycle(cycle_date, hour_entry)

        logger.info(self.cycles_summary())


    # ------------------------------------------------------------------

    def find_first_valid_cycle_dir(
        self,
        data_root: Optional[str] = None
    ) -> str:
        """
        Return the first valid cycle directory for this dataset.

        If data_root is not provided, self.root_dir is used.

        A valid cycle directory is:
            <root>/<dataset>.<YYYYMMDD>/<HH>/

        where HH is one of: 00, 06, 12, 18
        """

        root = data_root or self.root_dir

        if not root:
            raise ValueError(
                f"No data_root provided and dataset '{self.name}' "
                "has no root_dir set."
            )

        if not os.path.isdir(root):
            raise FileNotFoundError(f"Data root does not exist: {root}")

        pattern = re.compile(
            rf"^{re.escape(self.name)}\.(\d{{8}})$"
        )

        # Sort ensures deterministic "first"
        for entry in sorted(os.listdir(root)):
            entry_path = os.path.join(root, entry)

            if not os.path.isdir(entry_path):
                continue

            match = pattern.match(entry)
            if not match:
                continue

            # Now look for valid hour subdir
            for hour in sorted(os.listdir(entry_path)):
                hour_path = os.path.join(entry_path, hour)

                if os.path.isdir(hour_path) and hour in DatasetCycle.VALID_HOURS:
                    return hour_path

        raise FileNotFoundError(
            f"No valid cycle directory found for dataset '{self.name}' in {root}"
        )


    # Regex pattern to extract obs_space type from .nc filename
    OBS_FILE_PATTERN = re.compile(
        r"^[a-z0-9]+\.t\d{2}z\.([a-z0-9_]+).*\.nc$"
    )


    def _scan_cycle_dir(self, cycle_dir: str) -> list[tuple[str, str]]:
        """
        Scan a cycle directory and return list of
        (obs_space_name, file_path) tuples.

        Only leaf directories are considered.
        """

        results = []

        for root, dirs, files in os.walk(cycle_dir):

            # only leaf dirs
            if dirs:
                continue

            for file_name in files:
                if not file_name.endswith(".nc"):
                    continue

                match = self.OBS_FILE_PATTERN.match(file_name)
                if not match:
                    continue

                obs_space_name = match.group(1)
                full_path = os.path.join(root, file_name)

                results.append((obs_space_name, full_path))

        return results


    def register_obs_spaces(self, cycle_dir: Optional[str] = None) -> None:

        if cycle_dir is None:
            cycle_dir = self.find_first_valid_cycle_dir()

        if not os.path.isdir(cycle_dir):
            raise FileNotFoundError(f"Cycle directory not found: {cycle_dir}")

        scan_results = self._scan_cycle_dir(cycle_dir)

        existing = {dos.obs_space.name for dos in getattr(self, "obs_spaces", [])}

        for obs_space_name, _ in scan_results:
            existing.add(obs_space_name)
            logger.debug(f"Found obs space: {obs_space_name}")


        self.obs_spaces = [
            DatasetObsSpace(self, obs_space=ObsSpace(name=name))
            for name in sorted(existing)
        ]

        # logger.info(self.obs_spaces_summary())


    def register_files(self) -> None:
        """
        Scan all cycles for leaf .nc files and populate self.obs_space_files
        as a 2D structure: obs_space_name -> cycle -> DatasetObsSpaceFile.
        """
        self.obs_space_files = {}

        for cycle in self.cycles:
            cycle_dir = cycle.get_cycle_dir()
            if not os.path.isdir(cycle_dir):
                logger.warning(f"Cycle directory not found: {cycle_dir}")
                continue

            # the number of obs spaces changes with cycles....
            self.register_obs_spaces(cycle_dir)

            scan_results = self._scan_cycle_dir(cycle_dir)

            for obs_space_name, file_path in scan_results:
                # Find corresponding DatasetObsSpace
                dataset_obs_space = next(
                    (dos for dos in self.obs_spaces if dos.obs_space.name == obs_space_name),
                    None
                )
                if dataset_obs_space is None:
                    logger.warning(
                        f"ObsSpace {obs_space_name} not registered; skipping file {file_path}"
                    )
                    continue

                file_obj = File.from_path(file_path)

                dosf = DatasetObsSpaceFile(
                    dataset_obs_space=dataset_obs_space,
                    dataset_cycle=cycle,
                    file=file_obj
                )

                # Initialize nested dicts if needed
                self.obs_space_files.setdefault(
                    obs_space_name, {}
                )[cycle] = dosf

                logger.debug(f"Found file: {file_obj.path}")
