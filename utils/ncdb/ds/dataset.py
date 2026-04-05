import os
import re
import logging

from datetime import datetime, date
from typing import List, Optional, Tuple, Set

from sqlalchemy import (
    select,
    and_
)
from sqlalchemy.orm import Session

from .dataset_orm import (
    DatasetORM, 
    CycleORM, 
    FieldORM,
    DatasetFileORM
)
from .obs_space_orm import ObsSpaceORM

from .file import File
from .obs_space import ObsSpace
from .dataset_cycle import DatasetCycle
from .dataset_field import DatasetField
from .dataset_file import DatasetFile

from .file_scanner import FileScanner, SubdirFileScanner, NcObsSpaceNameParser


logger = logging.getLogger(__name__)


class Dataset:
    """
    Domain object representing a dataset.

    In-memory representation independent of db persistence.
    Persist explicitly using to_db(session).

    Data arrives in cycles, and is persisted in cycles,
    but the Dataset is a collection of fields.
    A field is a collection of files of the same ObsSpace type,
    with at most 1 file per cycle.
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

        self.dataset_fields: List[DatasetField] = []
        self.dataset_cycles: List[DatasetCycle] = []

        self.file_scanner = SubdirFileScanner("analysis/ocean/diags")
        self.obs_space_name_parser = NcObsSpaceNameParser()

    def __repr__(self) -> str:
        return (
            f"Dataset {self.name}, "
            f"id = {self.id}, "
            f"{self.root_dir}, \n"
            f"{len(self.dataset_cycles)} cycles, "
            f"{len(self.dataset_fields)} fields"
        )

    @classmethod
    def old_get_all(cls, session: Session) -> List["Dataset"]:
        stmt = select(DatasetORM).order_by(DatasetORM.name)
        return [cls.from_db_self(orm) for orm in session.scalars(stmt).all()]

    @classmethod
    def old_get_by_id(cls, session: Session, dataset_id: int) -> Optional["Dataset"]:
        orm = session.get(DatasetORM, dataset_id)
        return cls.from_db_self(orm) if orm else None


    """
    Parameters
    ----------
    n:
        > 0  → read first n cycles
        < 0  → read last |n| cycles
        None → read all cycles
        0    → read none
    """
    @staticmethod
    def _select_cycles(cycles: list["DatasetCycle"], n: Optional[int] = None) -> list["DatasetCycle"]:
        if not cycles:
            return []
        if n is None:
            selected = cycles
        elif n == 0:
            return []
        elif n > 0:
            selected = cycles[:n]
        else:  # n < 0
            selected = cycles[n:]
        return selected

    @classmethod
    def from_db_self(cls, orm: "DatasetORM") -> "Dataset":
        if not orm:
            return None
        instance = cls(
            name=orm.name,
            root_dir=orm.root_dir,
            id=orm.id
        )
        return instance

    def load_cycles_from_db(self, session: Session) -> None:
        """
        Loads all DatasetCycle identities (Date/Hour) without loading files.
        """
        if self.id is None:
            logger.error(f"Cannot load cycles for dataset '{self.name}': ID is missing.")
            return

        # Query all cycles for this dataset
        stmt = (
            select(CycleORM)
            .where(CycleORM.dataset_id == self.id)
            .order_by(CycleORM.cycle_date.desc(), CycleORM.cycle_hour.desc())
        )
        cycle_orms = session.scalars(stmt).all()
        logger.info(f"QUERY: Dataset ID is {self.id}")
        logger.info(f"RESULT: Found {len(cycle_orms)} cycles in DB")

        self.dataset_cycles = []
        for c_orm in cycle_orms:
            cycle_domain = DatasetCycle.from_orm(c_orm, self)
            self.dataset_cycles.append(cycle_domain)

        logger.info(f"Found {len(self.dataset_cycles)} cycles for dataset '{self.name}'")


    def find_field_by_id(self, field_id: int) -> Optional[DatasetField]:
        """Finds a loaded Field domain object by its database ID."""
        for field in self.dataset_fields:
            if field.id == field_id:
                return field
        return None

    '''
    def get_field_by_obs_space(self, obs_space_name: str) -> Optional[DatasetField]:
        """Finds a loaded Field by the name of its observation space."""
        for field in self.dataset_fields:
            if field.obs_space.name == obs_space_name:
                return field
        return None
    '''

    # --------------------------------------------------------
    # Persistence
    # --------------------------------------------------------
    # every entity that has id must:
    # 1. Convert to ORM
    # 2. Add to session
    # 3. Flush
    # 4. Copy generated id back to domain object

    def to_orm(self) -> "DatasetORM":
        return DatasetORM(
            id=self.id,
            name=self.name,
            root_dir=self.root_dir,
        )

    def to_db_self(self, session):
        from .dataset_orm import DatasetORM

        # logger.info("Dataset.to_db_self")

        # Check if this dataset already exists
        existing = session.scalar(
            select(DatasetORM).where(DatasetORM.name == self.name)
        )

        if existing:
            # Update the id to link children later
            self.id = existing.id
            return existing

        orm_obj = self.to_orm()
        session.add(orm_obj)
        session.flush()  # Ensure id is assigned
        self.id = orm_obj.id
        return orm_obj

    def to_db(self, repo, n: Optional[int] = None) -> None:
        repo.save_dataset(self)

        for field in self.dataset_fields:
            repo.save_field(field)

        cycles = Dataset._select_cycles(self.dataset_cycles, n)
        for cycle in cycles:
            # cycle.to_db(repo.session)
            cycle.to_db(repo)

    def add_cycle(self, cycle):
        self.dataset_cycles.append(cycle)
        self.dataset_cycles.sort()

    def discover_cycles(self) -> list[tuple[date, str]]:
        """
        Discover available cycles in the dataset root directory.

        Returns
        -------
        List of (cycle_date, cycle_hour) tuples sorted chronologically.
        Does NOT modify in-memory state.
        """

        if not self.root_dir or not os.path.isdir(self.root_dir):
            logger.warning(f"Invalid root_dir for dataset '{self.name}'")
            return []

        DATASET_DIR_PATTERN = re.compile(
            rf"^{re.escape(self.name)}\.(\d{{8}})$"
        )

        discovered = []

        for entry in os.listdir(self.root_dir):
            entry_path = os.path.join(self.root_dir, entry)

            if not os.path.isdir(entry_path):
                continue

            match = DATASET_DIR_PATTERN.match(entry)
            if not match:
                continue

            cycle_date_str = match.group(1)

            try:
                cycle_date = datetime.strptime(
                    cycle_date_str, "%Y%m%d"
                ).date()
            except ValueError:
                logger.warning(f"Invalid date format: {cycle_date_str}")
                continue

            for hour_entry in os.listdir(entry_path):
                hour_path = os.path.join(entry_path, hour_entry)

                if (
                    os.path.isdir(hour_path)
                    and hour_entry in DatasetCycle.VALID_HOURS
                ):
                    discovered.append((cycle_date, hour_entry))

        sorted_cycles = sorted(discovered, key=lambda x: (x[0], x[1]))
        logger.info(f"Discovered {len(sorted_cycles)} cycles for dataset {self.name}")
        return sorted_cycles

    def build_cycle(self, cycle_date: date, cycle_hour: str) -> DatasetCycle:
        # Get files from disk
        cycle_dir = DatasetCycle.cycle_dir(self, cycle_date, cycle_hour)
        all_files = self.file_scanner.scan(cycle_dir)
        pattern = self.obs_space_name_parser.get_search_pattern(self.name, cycle_hour)
        selected, _ = FileScanner.filter_files(all_files, pattern)

        cycle = DatasetCycle(self, cycle_date, cycle_hour)

        # Map files to fields
        for f in selected:
            obs_space = ObsSpace.from_file(f.path, self.obs_space_name_parser)
            if not obs_space: continue

            field = self.get_or_create_field(obs_space)
            
            ds_file = DatasetFile.from_file(f, field, cycle)
            field.add_file(ds_file)
            cycle.add_file(ds_file)

        return cycle

    def get_or_create_field(self, obs_space: ObsSpace) -> DatasetField:
        for f in self.dataset_fields:
            if f.obs_space.name == obs_space.name:
                return f
        new_field = DatasetField(self, obs_space)
        self.dataset_fields.append(new_field)
        return new_field

##########################################################
