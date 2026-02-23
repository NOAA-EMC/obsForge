import os
import logging
from datetime import date
from typing import Optional

from .dataset_orm import DatasetCycleORM
from sqlalchemy import select


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

        self.fields: List[DatasetField] = []
        # self.files: List[DatasetFile] = []


    @classmethod
    def from_directory(cls, dataset: "Dataset", cycle_date: date, cycle_hour: str) -> "DatasetCycle":
        this_cycle = cls(dataset=dataset, cycle_date=cycle_date, cycle_hour=cycle_hour)

        cycle_dir = this_cycle.get_cycle_dir()
        all_leaf_files = FileScanner.get_all_leaf_files(cycle_dir)

        prefix = dataset.name 
        pattern = ObsSpace.get_search_pattern(prefix, cycle_hour)
        selected, rejected = FileScanner.filter_files(all_leaf_files, pattern)

        for file_obj in selected:
            obs_space = ObsSpace.from_file(file_obj.path, prefix=prefix)
            
            if obs_space:
                field = dataset.add_file_to_field(file_obj, this_cycle, obs_space)
                this_cycle.fields.append(field)

        return this_cycle


    def get_cycle_dir(self) -> str:
        """
        Return the directory path for this cycle:
        <dataset.root_dir>/<dataset.name>.<YYYYMMDD>/<cycle_hour>/
        """
        date_str = self.cycle_date.strftime("%Y%m%d")
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
