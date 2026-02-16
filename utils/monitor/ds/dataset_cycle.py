import os
import logging
from datetime import date
from typing import Optional

from .dataset_orm import DatasetCycleORM
from sqlalchemy import select


logger = logging.getLogger(__name__)


'''
# from typing import List, Optional, Tuple, Set 

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

    def get_cycle_dir(self) -> str:
        """
        Return the directory path for this cycle:
        <dataset.root_dir>/<dataset.name>.<YYYYMMDD>/<cycle_hour>/
        """
        date_str = self.cycle_date.strftime("%Y%m%d")
        # Optional: include cycle hour as subdir if your filesystem uses that
        return os.path.join(
            self.dataset.root_dir, 
            f"{self.dataset.name}.{date_str}", 
            self.cycle_hour
        )

    def _to_orm(self) -> DatasetCycleORM:
        return DatasetCycleORM(
            dataset_id=self.dataset.id,
            cycle_date=self.cycle_date,
            cycle_hour=self.cycle_hour
        )

    def to_db(self, session):
        if self.id is not None:
            return

        existing = session.scalar(
            select(DatasetCycleORM).where(
                (DatasetCycleORM.dataset_id == self.dataset.id) &
                (DatasetCycleORM.cycle_date == self.cycle_date) &
                (DatasetCycleORM.cycle_hour == self.cycle_hour)
            )
        )
        if existing:
            self.id = existing.id
            return

        orm = DatasetCycleORM(
            dataset_id=self.dataset.id,
            cycle_date=self.cycle_date,
            cycle_hour=self.cycle_hour
        )
        session.add(orm)
        session.flush()
        self.id = orm.id
