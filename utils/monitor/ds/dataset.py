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





'''

class DatasetObsSpaceFile:
    def __init__(
        self,
        dataset_obs_space: "DatasetObsSpace",
        dataset_cycle: "DatasetCycle",
        file: "File",
        id: Optional[int] = None
    ):
        self.id = id
        self.dataset_obs_space = dataset_obs_space
        self.dataset_cycle = dataset_cycle
        self.file = file

    def __repr__(self):
        return (
            f"<DatasetObsSpaceFile(id={self.id}, "
            f"obs_space={self.dataset_obs_space.obs_space.name}, "
            f"cycle={self.dataset_cycle.cycle_date} {self.dataset_cycle.cycle_hour}, "
            f"file={self.file.path})>"
        )

    def to_orm(self) -> "DatasetObsSpaceFileORM":
        """Convert to ORM object for persistence."""
        return DatasetObsSpaceFileORM(
            id=self.id,
            dataset_obs_space_id=self.dataset_obs_space.id,
            dataset_cycle_id=self.dataset_cycle.id,
            file_id=self.file.id
        )

    def to_db(self, session: "Session") -> None:
        """Persist this DatasetObsSpaceFile entry."""
        # Ensure File is persisted
        if self.file.id is None:
            self.file.to_db(session)

        # Ensure DatasetObsSpace is persisted
        if self.dataset_obs_space.id is None:
            self.dataset_obs_space.to_db(session)

        # Ensure DatasetCycle is persisted
        if self.dataset_cycle.id is None:
            self.dataset_cycle.to_db(session)

        # Check if the entry already exists
        exists = session.scalar(
            select(DatasetObsSpaceFileORM).where(
                (DatasetObsSpaceFileORM.dataset_obs_space_id == self.dataset_obs_space.id) &
                (DatasetObsSpaceFileORM.dataset_cycle_id == self.dataset_cycle.id) &
                (DatasetObsSpaceFileORM.file_id == self.file.id)
            )
        )
        if exists:
            self.id = exists.id
            return

        # Persist
        orm_obj = self.to_orm()
        session.add(orm_obj)
        session.flush()
        self.id = orm_obj.id

'''




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

    # --------------------------------------------------------
    # Persistence
    # --------------------------------------------------------
    # every entity that has id must:
    # 1. Convert to ORM
    # 2. Add to session
    # 3. Flush
    # 4. Copy generated id back to domain object


    def tryto_db(self, session: "Session") -> None:
        # Persist Dataset itself
        if self.id is None:
            existing = session.scalar(
                select(DatasetORM).where(DatasetORM.name == self.name)
            )
            if existing:
                self.id = existing.id
            else:
                session.add(self.to_orm())
                session.flush()

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


    def old_to_db(self, session):
        """
        Persist the Dataset object, its cycles, and obs_spaces to the database.
        Ensures that duplicates are avoided.
        """
        # -----------------------------
        # 1. Persist Dataset itself
        # -----------------------------
        if self.id is None:
            # Check if dataset already exists in DB
            existing = session.scalar(
                select(DatasetORM).where(DatasetORM.name == self.name)
            )
            if existing:
                self.id = existing.id
            else:
                orm_ds = self.to_orm()
                session.add(orm_ds)
                session.flush()  # assign PK
                self.id = orm_ds.id

        # -----------------------------
        # 2. Persist Cycles
        # -----------------------------
        for cycle in self.cycles:
            with session.no_autoflush:
                existing = session.scalar(
                    select(DatasetCycleORM).where(
                        (DatasetCycleORM.dataset_id == self.id) &
                        (DatasetCycleORM.cycle_date == cycle.cycle_date) &
                        (DatasetCycleORM.cycle_hour == cycle.cycle_hour)
                    )
                )
                if existing:
                    cycle.id = existing.id
                else:
                    orm_cycle = cycle.to_orm(dataset_id=self.id)
                    session.add(orm_cycle)
                    session.flush()  # assign PK

                    cycle.id = orm_cycle.id

        # -----------------------------
        # 3. Persist DatasetObsSpaces
        # -----------------------------
        for dos in self.obs_spaces:
            # dos.obs_space is the domain object
            # Check if obs_space exists
            obs = session.scalar(
                select(ObsSpaceORM).where(ObsSpaceORM.name == dos.obs_space.name)
            )
            if obs:
                obs_id = obs.id
            else:
                # Persist new obs_space
                orm_obs = dos.obs_space.to_orm()
                session.add(orm_obs)
                session.flush()

                dos.obs_space.id = orm_obs.id
                obs_id = orm_obs.id


            # Now persist the DatasetObsSpace relation
            exists = session.scalar(
                select(DatasetObsSpaceORM).where(
                    (DatasetObsSpaceORM.dataset_id == self.id) &
                    (DatasetObsSpaceORM.obs_space_id == obs_id)
                )
            )
            if not exists:
                orm_obj = dos.to_orm(dataset_id=self.id, obs_space_id=obs_id)
                session.add(orm_obj)

        # -----------------------------
        # 4. Commit all changes
        # -----------------------------
        session.commit()


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

        logging.info(f"Scanning cycles for dataset '{self.name}'")

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
                logging.warning(f"Invalid date format: {cycle_date_str}")
                continue

            for hour_entry in os.listdir(entry_path):

                hour_path = os.path.join(entry_path, hour_entry)

                if not os.path.isdir(hour_path):
                    continue

                if hour_entry not in DatasetCycle.VALID_HOURS:
                    logging.warning(
                        f"Ignoring invalid cycle hour '{hour_entry}' "
                        f"in {entry_path}"
                    )
                    continue

                logging.info(
                    f"Found cycle: {self.name} "
                    f"{cycle_date_str} {hour_entry}"
                )

                self.add_cycle(cycle_date, hour_entry)


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

        # DatasetCycle.VALID_HOURS = {"00", "06", "12", "18"}

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
            logging.info(f"Found obs space: {obs_space_name}")


        self.obs_spaces = [
            DatasetObsSpace(self, obs_space=ObsSpace(name=name))
            for name in sorted(existing)
        ]



    def old_register_obs_spaces(self, cycle_dir: Optional[str] = None) -> None:
        """
        Recursively scan a cycle directory for leaf .nc files and register
        their obs_space types in self.obs_spaces.

        Parameters
        ----------
        cycle_dir : str, optional
            Directory to scan. If None, uses the first valid cycle directory.
        """
        if cycle_dir is None:
            cycle_dir = self.find_first_valid_cycle_dir()

        if not os.path.isdir(cycle_dir):
            raise FileNotFoundError(f"Cycle directory not found: {cycle_dir}")

        # Collect existing obs_space names to avoid duplicates
        obs_space_names: Set[str] = {dos.obs_space.name for dos in getattr(self, "obs_spaces", [])}

        # Walk recursively
        for root, dirs, files in os.walk(cycle_dir):
            # Skip non-leaf directories (those that have subdirs)
            if dirs:
                continue

            for file_name in files:
                if not file_name.endswith(".nc"):
                    continue

                match = self.OBS_FILE_PATTERN.match(file_name)
                if not match:
                    continue

                obs_space_name = match.group(1)
                if obs_space_name not in obs_space_names:
                    obs_space_names.add(obs_space_name)

        # Update obs_spaces list in memory
        self.obs_spaces = [
            DatasetObsSpace(self, obs_space=ObsSpace(name=name))
            for name in sorted(obs_space_names)
        ]



    def register_files(self) -> None:
        """
        Scan all cycles for leaf .nc files and populate self.obs_space_files
        as a 2D structure: obs_space_name -> cycle -> DatasetObsSpaceFile.
        """
        self.obs_space_files = {}

        for cycle in self.cycles:
            cycle_dir = cycle.get_cycle_dir()
            if not os.path.isdir(cycle_dir):
                logging.warning(f"Cycle directory not found: {cycle_dir}")
                continue

            scan_results = self._scan_cycle_dir(cycle_dir)

            for obs_space_name, file_path in scan_results:
                # Find corresponding DatasetObsSpace
                dataset_obs_space = next(
                    (dos for dos in self.obs_spaces if dos.obs_space.name == obs_space_name),
                    None
                )
                if dataset_obs_space is None:
                    logging.warning(
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

                logging.info(f"Found file: {file_obj.path}")
