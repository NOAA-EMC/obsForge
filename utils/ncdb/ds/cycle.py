import os
import logging
from datetime import date, datetime
from typing import Optional, List, Tuple
from pathlib import Path

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from .dataset_orm import CycleORM, DatasetFileORM

from .obs_space import ObsSpace
from .field import Field
from .dataset_file import DatasetFile

logger = logging.getLogger(__name__)


'''
Dataset constructs a Cycle object
It holds a collection of DatasetFile objects
where each file is already connected to the field
and has its attributes computed
'''
class Cycle:
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

    @property
    def datetime(self):
        year, month, day = self._parse_date(self.cycle_date)
        hour = int(self.cycle_hour)

        return datetime(year, month, day, hour)

    def _parse_date(self, cycle_date):
        if isinstance(cycle_date, str):
            y, m, d = map(int, cycle_date.split("-"))
            return y, m, d
        else:
            return cycle_date.year, cycle_date.month, cycle_date.day

    def __lt__(self, other):
        """
        comparison to allow sorting by cycle_date and cycle_hour.
        """
        if not isinstance(other, Cycle):
            raise TypeError(f"Cannot compare Cycle with {type(other)}")

        # First compare by cycle_date, then by cycle_hour
        if self.cycle_date != other.cycle_date:
            return self.cycle_date < other.cycle_date
        # return self.cycle_hour < other.cycle_hour
        return int(self.cycle_hour) < int(other.cycle_hour)

    def add_file(self, file):
        self.files.append(file)

    @classmethod
    def from_orm(cls, orm: CycleORM, dataset: "Dataset") -> "Cycle":
        if not orm:
            return None

        return cls(
            dataset=dataset,
            cycle_date=orm.cycle_date,
            cycle_hour=orm.cycle_hour,
            id=orm.id
        )

    def to_orm(self) -> CycleORM:
        return CycleORM(
            dataset_id=self.dataset.id,
            cycle_date=self.cycle_date,
            cycle_hour=self.cycle_hour
        )

#######################################################

    # def to_db(self, repo):
        # repo.save_cycle(self)
        # repo.save_cycle_files(self)
        # logger.info(f"to_db {self.dataset.name} {self}")

    @staticmethod
    def old_cycle_dir(dataset, cycle_date, cycle_hour):
        """
        Compute the directory path for a cycle
        without instantiating a Cycle.
        """
        date_str = cycle_date.strftime("%Y%m%d")
        return os.path.join(
            dataset.root_dir,
            f"{dataset.name}.{date_str}",
            cycle_hour,
        )
