import logging
from typing import Optional

from .dataset_orm import DatasetFileORM
from sqlalchemy import select

logger = logging.getLogger(__name__)


class DatasetFile:
    def __init__(
        self,
        file: "File",
        dataset_field: "DatasetField",
        dataset_cycle: "DatasetCycle",
        id: Optional[int] = None
    ):
        self.dataset_field = dataset_field
        self.dataset_cycle = dataset_cycle
        self.file = file
        self.id = id

    def __repr__(self) -> str:
        return (
            f"<DatasetFile(id={self.id}, "
            # f"obs_space={self.dataset_field.obs_space.name}, "
            f"obs_space={self.dataset_field.obs_space}, "
            # f"cycle={self.dataset_cycle.cycle_date} {self.dataset_cycle.cycle_hour}, "
            f"cycle={self.dataset_cycle}, "
            f"file={self.file.path})>"
        )

    def to_orm(self) -> "DatasetFileORM":
        """Convert to ORM object for persistence."""
        return DatasetFileORM(
            id=self.id,
            dataset_field_id=self.dataset_field.id,
            dataset_cycle_id=self.dataset_cycle.id,
            file_id=self.file.id
        )

    def to_db(self, session: "Session") -> None:
        """
            Persist this DatasetFile entry.
            Assume the field and the cycle had been persisted before.
        """
        # Ensure File is persisted
        if self.file.id is None:
            self.file.to_db(session)

        # Check if the entry already exists
        exists = session.scalar(
            select(DatasetFileORM).where(
                (DatasetFileORM.dataset_field_id == self.dataset_field.id) &
                (DatasetFileORM.dataset_cycle_id == self.dataset_cycle.id) &
                (DatasetFileORM.file_id == self.file.id)
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
