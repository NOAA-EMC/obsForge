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

        # the files are not persisted here, but in cycles
        self.files: List[DatasetFile] = []

    def add_file(self, f: DatasetFile, cycle):
        dsf = DatasetFile(f, self, cycle)
        self.files.append(dsf)
        return dsf

    def to_orm(self, dataset_id: int, obs_space_id: int) -> DatasetFieldORM:
        return DatasetFieldORM(
            id=self.id,
            dataset_id=dataset_id,
            obs_space_id=obs_space_id
        )

    def to_db(self, session):
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

        # persist underlying ObsSpace
        obs_space_id = self.obs_space.to_db(session)
        # now obs_space_id == self.obs_space.id

        orm = self.to_orm(
            dataset_id=self.dataset.id,
            obs_space_id=self.obs_space.id
        )
        session.add(orm)
        session.flush()
        self.id = orm.id
