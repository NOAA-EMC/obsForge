import os
import logging
from datetime import date, datetime
from typing import Optional
from pathlib import Path
from typing import Tuple

from sqlalchemy import select, and_
from .dataset_orm import DatasetCycleORM
from .file_scanner import FileScanner
from .obs_space import ObsSpace
from .dataset_field import DatasetField

logger = logging.getLogger(__name__)


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

        # each field has one file
        # these files are persisted
        # the fields are merged with dataset fields
        self.fields: List[DatasetField] = []

    def __repr__(self) -> str:
        return (
            f"<Cycle "
            f"id = {self.id}, "
            f"{self.cycle_date}  {self.cycle_hour}, "
            f"{len(self.fields)} fields"
            ">"
        )

    def add_field(self, field):
        self.fields.append(field)

    @classmethod
    def from_directory(cls, dataset: "Dataset", cycle_dir: str) -> "DatasetCycle":
        cycle_date, cycle_hour = cls.parse_cycle_dir(cycle_dir)

        this_cycle = cls(dataset=dataset, cycle_date=cycle_date, cycle_hour=cycle_hour)

        all_leaf_files = FileScanner.get_all_leaf_files(cycle_dir)

        prefix = dataset.name 
        pattern = ObsSpace.get_search_pattern(prefix, cycle_hour)
        selected, rejected = FileScanner.filter_files(all_leaf_files, pattern)

        for file_obj in selected:
            # logger.info(f"cycle file: {file_obj.path}")
            obs_space = ObsSpace.from_file(file_obj.path, prefix=prefix)
            
            if obs_space:
                field = DatasetField(dataset, obs_space)
                dsf = field.add_file(file_obj, this_cycle)

                # logger.debug(f"added file: {dsf}")

                this_cycle.add_field(field)

        logger.info(f"read {this_cycle} from {cycle_dir}")

        return this_cycle

    @classmethod
    def cycle_dir(cls, dataset, cycle_date, cycle_hour):
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
            raise ValueError(f"Invalid cycle directory format: {path}")

        return cycle_date, cycle_hour

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

    # cycle fields hold exactly one file each
    def to_db_files(self, session):
        for field in self.fields:
            field.files[0].compute_attributes()
            field.files[0].to_db(session)

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



###       # def compute_derived_attributes(self):
###           # """Orchestrate computation for all files in this cycle."""
###           # # We store the results in memory on the file objects
###           # for dos_file in self.obs_space_files:
###               # dos_file.compute_derived_attributes()
###   # 
###       # def to_db_derived_attributes(self, session):
###           # """Commit all computed results for this cycle to the database."""
###           # for dos_file in self.obs_space_files:
###               # dos_file.to_db_derived_attributes(session)
###           # # Committing at the cycle level is usually the 'Sweet Spot' for performance
###           # session.commit()
