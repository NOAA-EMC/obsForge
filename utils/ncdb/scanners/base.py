import logging
logger = logging.getLogger(__name__)

import os
from typing import List, Tuple, Optional

from abc import ABC, abstractmethod
from datetime import date

from ncdb.ds.file import File
from ncdb.ds.dataset import Dataset

# the following should be moved out and refactored
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from ncdb.ds.db_base import Base
from ncdb.ds.io.dataset_repository import DatasetRepository


class ScanCycle:
    def __init__(self, dataset, cycle_date, cycle_hour, scan_results):
        self.dataset = dataset
        self.cycle_date = cycle_date
        self.cycle_hour = cycle_hour
        self.scan_results = scan_results


# scanner is configured with root_dir
# datasets are discovered at init
# we assume that the dataset does not own root_dir,
# so dataset.root_dir is not used (to be deprecated)
class BaseScanner(ABC):
    def __init__(self, root_dir: str):
        self.root_dir = root_dir
        self.datasets = self._init_datasets()

    # avoids calling virtual method inside constructor
    def _init_datasets(self):
        return self.discover_datasets()

    @abstractmethod
    def discover_datasets(self):
        pass

    @abstractmethod
    def discover_datasets(self) -> List[Dataset]:
        pass

    @abstractmethod
    def discover_cycles(self, dataset: Dataset):
        pass

    @abstractmethod
    def scan_cycle(self, dataset: Dataset, cycle_date, cycle_hour):
        pass

    def select_cycles(self, cycles, n_cycles: Optional[int]):
        if n_cycles is None:
            return cycles

        if n_cycles < 0:
            return cycles[n_cycles:]  # last N cycles

        return cycles[:n_cycles]

    def scan_dataset_cycles(self, n_cycles: Optional[int]):
        for dataset in self.datasets:
            cycles = self.discover_cycles(dataset)
            selected = self.select_cycles(cycles, n_cycles)

            for cycle_date, cycle_hour in selected:
                scan_results = self.scan_cycle(
                    dataset, cycle_date, cycle_hour
                )
                yield ScanCycle(dataset, cycle_date, cycle_hour, scan_results)
