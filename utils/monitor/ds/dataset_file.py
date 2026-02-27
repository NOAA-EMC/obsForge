import logging
from typing import Optional

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from .dataset_orm import DatasetFileORM

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
            "\n"
            # f"obs_space={self.dataset_field.obs_space.name}, "
            f"field = {self.dataset_field}, "
            "\n"
            # f"obs_space={self.dataset_field.obs_space}, "
            # f"cycle={self.dataset_cycle.cycle_date} {self.dataset_cycle.cycle_hour}, "
            f"cycle={self.dataset_cycle}, "
            "\n"
            f"file={self.file.path})>"
        )

    def to_orm(self) -> "DatasetFileORM":
        return DatasetFileORM(
            id=self.id,
            dataset_field_id=self.dataset_field.id,
            dataset_cycle_id=self.dataset_cycle.id,
            file_id=self.file.id
        )


    def to_db(self, session: Session) -> "DatasetFileORM":
        """
        Ensure this DatasetFile exists in the DB. Returns the ORM object.
        Sets self.id.
        """

        # Already persisted?
        if self.id is not None:
            existing = session.get(DatasetFileORM, self.id)
            if existing:
                return existing

        logger.info(f"to_db {self}")

        # Persist underlying DatasetField first
        if self.dataset_field.id is None:
            self.dataset_field.to_db(session)

        # Persist underlying DatasetCycle first
        if self.dataset_cycle.id is None:
            self.dataset_cycle.to_db(session)

        if self.file.id is None:
            self.file.to_db(session)

        # Flush pending inserts so session sees all IDs
        # session.flush()

        # Check if a row already exists
        existing = session.scalar(
            select(DatasetFileORM).where(
                and_(
                    DatasetFileORM.dataset_field_id == self.dataset_field.id,
                    DatasetFileORM.dataset_cycle_id == self.dataset_cycle.id,
                    DatasetFileORM.file_id == self.file.id
                )
            )
        )

        if existing:
            self.id = existing.id
            return existing

        # Create ORM row
        orm = DatasetFileORM(
            dataset_field_id=self.dataset_field.id,
            dataset_cycle_id=self.dataset_cycle.id,
            file_id=self.file.id
        )
        session.add(orm)
        session.flush()
        self.id = orm.id

        return orm
