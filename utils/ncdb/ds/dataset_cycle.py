import os
import logging
from datetime import date, datetime
from typing import Optional, List, Tuple
from pathlib import Path

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from .dataset_orm import DatasetCycleORM, DatasetFileORM

from .file_scanner import FileScanner, SubdirFileScanner
from .obs_space import ObsSpace
from .dataset_field import DatasetField
from .dataset_file import DatasetFile

logger = logging.getLogger(__name__)


'''
Dataset constructs a DatasetCycle object
It holds a collection of DatasetFile objects
where each file is already connected to the field
and has its attributes computed
'''
class DatasetCycle:
    VALID_HOURS = {"00", "06", "12", "18"}

    def __init__(
        self,
        dataset: "Dataset",
        cycle_date: date,
        cycle_hour: str,
        id: Optional[int] = None
    ):
        if isinstance(cycle_hour, int):
            cycle_hour = f"{cycle_hour:02d}"

        if cycle_hour not in self.VALID_HOURS:
            raise ValueError(
                f"Invalid cycle hour '{cycle_hour}'. "
                f"Must be one of {sorted(self.VALID_HOURS)}"
            )

        self.id = id
        self.dataset = dataset
        self.cycle_date = cycle_date
        self.cycle_hour = cycle_hour

        self.files: List["DatasetFile"] = []

    def __repr__(self) -> str:
        return (
            f"<Cycle "
            f"id = {self.id}, "
            f"{self.cycle_date}  {self.cycle_hour}, "
            f"{len(self.files)} files"
            ">"
        )

    def __lt__(self, other):
        """
        comparison to allow sorting by cycle_date and cycle_hour.
        """
        if not isinstance(other, DatasetCycle):
            raise TypeError(f"Cannot compare DatasetCycle with {type(other)}")

        # First compare by cycle_date, then by cycle_hour
        if self.cycle_date != other.cycle_date:
            return self.cycle_date < other.cycle_date
        return self.cycle_hour < other.cycle_hour

    def add_file(self, file):
        self.files.append(file)

    @staticmethod
    def read_cycle_files(dataset: "Dataset", cycle_date: date, cycle_hour: str):
        cycle_dir = DatasetCycle.cycle_dir(dataset, cycle_date, cycle_hour)
        if not os.path.isdir(cycle_dir):
            logger.warning(f"Cycle directory does not exist: {cycle_dir}")
            return None

        all_files = dataset.file_scanner.scan(cycle_dir)
        # logger.debug(f"from_dir: {len(all_files)} files")
        # logger.debug(f"from_dir: {all_files}")

        prefix = dataset.name 
        pattern = dataset.obs_space_name_parser.get_search_pattern(prefix, cycle_hour)
        # pattern = "*.nc"
        selected, rejected = FileScanner.filter_files(all_files, pattern)
        # logger.debug(f"from_dir: pattern = |{pattern}|")
        # logger.debug(f"from_dir: selected {len(selected)} files")

        logger.info(f"Read {cycle_dir} directory: {len(selected)} files")
        return selected

    @staticmethod
    def cycle_dir(dataset, cycle_date, cycle_hour):
        """
        Compute the directory path for a cycle
        without instantiating a DatasetCycle.
        """
        date_str = cycle_date.strftime("%Y%m%d")
        return os.path.join(
            dataset.root_dir,
            f"{dataset.name}.{date_str}",
            cycle_hour,
        )

    '''
    @classmethod
    def parse_cycle_dir(cls, path: str) -> Tuple[datetime.date, str]:
        """
        Given a path like:
        <root_dir>/<dataset.name>.<YYYYMMDD>/<cycle_hour>/

        Return:
            (cycle_date, cycle_hour)
        """
        p = Path(path).resolve()

        cycle_hour = p.name                     # last component
        date_part = p.parent.name               # dataset.name.YYYYMMDD

        # Extract YYYYMMDD (everything after last dot)
        try:
            date_str = date_part.split(".")[-1]
            cycle_date = datetime.strptime(date_str, "%Y%m%d").date()
        except (IndexError, ValueError):
            # raise ValueError(f"Invalid cycle directory format: {path}")
            logger.error(f"Invalid cycle directory format: {path}")
            return None

        return cycle_date, cycle_hour
    '''

    @classmethod
    def from_orm(
        cls, 
        session: Session, 
        orm: DatasetCycleORM, 
        dataset: "Dataset"
    ) -> "DatasetCycle":
        instance = cls.from_orm_self(orm, dataset)
        instance.from_orm_files(session)
        return instance

    @classmethod
    def from_orm_self(cls, orm: DatasetCycleORM, dataset: "Dataset") -> "DatasetCycle":
        if not orm:
            return None
            
        return cls(
            dataset=dataset,
            cycle_date=orm.cycle_date,
            cycle_hour=orm.cycle_hour,
            id=orm.id
        )

    def from_orm_files(self, session: Session) -> None:
        # Fetch DatasetFileORM objects for this specific cycle
        stmt = (
            select(DatasetFileORM)
            .where(DatasetFileORM.dataset_cycle_id == self.id)
        )
        file_orms = session.scalars(stmt).all()

        for f_orm in file_orms:
            field_domain = self.dataset.find_field_by_id(f_orm.dataset_field_id)
            
            if field_domain:
                ds_file = DatasetFile.from_orm(
                    session=session,
                    orm=f_orm,
                    dataset_field=field_domain,
                    dataset_cycle=self
                )
                field_domain.add_file(ds_file)
                self.add_file(ds_file)

    def to_orm(self) -> DatasetCycleORM:
        return DatasetCycleORM(
            dataset_id=self.dataset.id,
            cycle_date=self.cycle_date,
            cycle_hour=self.cycle_hour
        )

    def to_db(self, session):
        orm = self.to_db_self(session)
        self.to_db_files(session)

        logger.info(f"to_db {self.dataset.name} {self}")

        return orm

    def to_db_files(self, session):
        for f in self.files:
            f.to_db(session)

    def to_db_self(self, session) -> DatasetCycleORM:
        """
        Ensure this DatasetCycle exists in the DB. Returns the ORM object.
        Sets self.id.
        """

        # Already persisted? Return existing ORM
        if self.id is not None:
            # Fetch the ORM object if needed
            existing = session.get(DatasetCycleORM, self.id)
            if existing:
                return existing

        # Check DB for existing cycle
        existing = session.scalar(
            select(DatasetCycleORM).where(
                and_(
                    DatasetCycleORM.dataset_id == self.dataset.id,
                    DatasetCycleORM.cycle_date == self.cycle_date,
                    DatasetCycleORM.cycle_hour == self.cycle_hour
                )
            )
        )

        if existing:
            self.id = existing.id
            return existing

        # Create new ORM object
        orm = self.to_orm()
        session.add(orm)
        session.flush()   # generate ID without committing

        self.id = orm.id

        return orm
