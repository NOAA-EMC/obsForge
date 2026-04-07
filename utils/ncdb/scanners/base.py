import logging
logger = logging.getLogger(__name__)

import os
from typing import List, Tuple, Optional

from abc import ABC, abstractmethod
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from ds.db_base import Base
from ds.io.dataset_repository import DatasetRepository

from ds.file import File
from ds.dataset import Dataset


class BaseScanner(ABC):
    def __init__(self, db_path: str, root_dir: str):
        self.db_path = db_path
        self.root_dir = root_dir

        self.engine = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(self.engine)

        self.datasets: List[Dataset] = []

    @abstractmethod
    def discover_datasets(self) -> List[Dataset]:
        """Find datasets in root_dir"""
        pass

    @abstractmethod
    def discover_cycles(self, dataset: Dataset) -> List[Tuple[date, str]]:
        """Find available cycles for a dataset"""
        pass

    def scan_cycle(self, dataset_name, cycle_date, cycle_hour):
        cycle_dir = self.build_cycle_dir(
            dataset_name, cycle_date, cycle_hour
        )

        files = self._scan_files(cycle_dir)
        selected = self.select_files(files, dataset_name, cycle_hour)

        results = []
        for f in selected:
            name = self.parse_obs_space(f.path)
            if name:
                results.append((f, name))

        return results

    def build_cycle_dir(self, dataset_name, cycle_date, cycle_hour):
        raise NotImplementedError

    def parse_obs_space(self, file_path):
        raise NotImplementedError

    def select_files(self, files, dataset_name, cycle_hour):
        raise NotImplementedError

    def is_valid_cycle_hour(self, hour: str) -> bool:
        return hour in {"00", "06", "12", "18"}

    def _scan_files(self, root_path):
        all_files = []
        for dirpath, dirnames, filenames in os.walk(root_path):
            if not dirnames:
                for filename in filenames:
                    full_path = os.path.join(dirpath, filename)
                    all_files.append(File.from_path(full_path))
        return all_files

    def run(self, n_cycles: Optional[int] = None):
        self.discover_datasets()

        with Session(self.engine) as session:
            repo = DatasetRepository(session)

            for ds in self.datasets:
                repo.save_dataset(ds)
                repo.load_fields(ds)
                # logger.info(f"loaded fields for {ds}")
                # for f in ds.dataset_fields:
                    # logger.info(f"-->  {f}")

                cycles = self.discover_cycles(ds)
                selected = Dataset._select_cycles(cycles, n_cycles)

                for cycle_date, cycle_hour in selected:
                    scan_results = self.scan_cycle(
                        ds.name, cycle_date, cycle_hour
                    )
                    cycle = ds.build_cycle(
                        cycle_date, cycle_hour, scan_results
                    )
                    repo.save_cycle(cycle)

                session.commit()
