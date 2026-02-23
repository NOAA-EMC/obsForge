import logging
from typing import Optional

from sqlalchemy import select
from .dataset_orm import DatasetFieldORM

from .dataset_file import DatasetFile


logger = logging.getLogger(__name__)


class DatasetField:
    """
    Represents a variable of type ObsSpace within a Dataset.
    It has a list of DatasetFile objects, each is linked
    to a field and an ObsSpace
    The list of files probably needs to be synced to db.....
    """
    def __init__(self, dataset: "Dataset", obs_space: "ObsSpace"):
        self.dataset = dataset
        self.obs_space = obs_space
        self.id = None  # set when persisted

        self.files: List[DatasetFile] = []

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
