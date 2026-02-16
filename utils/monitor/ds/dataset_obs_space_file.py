import logging
from typing import Optional

from .dataset_orm import DatasetObsSpaceFileORM
from sqlalchemy import select


logger = logging.getLogger(__name__)


'''
# import os
# from datetime import date
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
