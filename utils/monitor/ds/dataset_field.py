import logging
from typing import Optional

from .dataset_orm import DatasetFieldORM
from sqlalchemy import select


logger = logging.getLogger(__name__)


class DatasetField:
    """
    Represents a variable of type ObsSpace within a Dataset.
    Owned by a Dataset, but refers to a separate ObsSpace type.
    """
    def __init__(self, dataset: "Dataset", obs_space: "ObsSpace"):
        self.dataset = dataset
        self.obs_space = obs_space
        self.id = None  # set when persisted

    def to_orm(self, dataset_id: int, obs_space_id: int) -> DatasetFieldORM:
        return DatasetFieldORM(
            id=self.id,
            dataset_id=dataset_id,
            obs_space_id=obs_space_id
        )

    def to_db(self, session):
        # persist underlying ObsSpace first
        self.obs_space.to_db(session)
        if self.id is not None:
            return

        existing = session.scalar(
            select(DatasetFieldORM).where(
                (DatasetFieldORM.dataset_id == self.dataset.id) &
                (DatasetFieldORM.obs_space_id == self.obs_space.id)
            )
        )
        if existing:
            self.id = existing.id
            return

        orm = self.to_orm(
            dataset_id=self.dataset.id,
            obs_space_id=self.obs_space.id
        )
        session.add(orm)
        session.flush()
        self.id = orm.id
