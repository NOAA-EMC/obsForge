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

        self.files: List[DatasetFile] = []

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



###       def register_files(self) -> None:
###           """
###           Scan all cycles for leaf .nc files and populate self.obs_space_files
###           as a 2D structure: obs_space_name -> cycle -> DatasetFile.
###           """
###   
###           logger.debug(f"Scanning obs space files for dataset '{self.name}'")
###   
###           self.obs_space_files = {}
###   
###           cycle_dir = cycle.get_cycle_dir()
###   
###           if not os.path.isdir(cycle_dir):
###               logger.warning(f"Cycle directory not found: {cycle_dir}")
###               continue
###   
###           # the number of obs spaces changes with cycles....
###           self.register_obs_spaces(cycle_dir)
###   
###           scan_results = self._scan_cycle_dir(cycle_dir)
###   
###           for obs_space_name, file_path in scan_results:
###               '''
###               # Find corresponding DatasetField
###               dataset_obs_space = next(
###                   (dos for dos in self.dataset_obs_spaces if dos.obs_space.name == obs_space_name),
###                   None
###               )
###               if dataset_obs_space is None:
###                   logger.warning(
###                       f"ObsSpace {obs_space_name} not registered; skipping file {file_path}"
###                   )
###                   continue
###               '''
###   
###               file_obj = File.from_path(file_path)
###   
###               dosf = DatasetFile(
###                   dataset_obs_space=dataset_obs_space,
###                   dataset_cycle=cycle,
###                   file=file_obj
###               )
###   
###               # Initialize nested dicts if needed
###               self.obs_space_files.setdefault(
###                   obs_space_name, {}
###               )[cycle] = dosf
###   
###               logger.debug(f"Found file: {file_obj.path}")
###   
###   
###   
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
