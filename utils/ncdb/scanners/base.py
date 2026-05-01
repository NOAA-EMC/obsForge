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


class BaseScanner(ABC):
    def __init__(self):
        self.datasets: List[Dataset] = []

    @abstractmethod
    def discover_datasets(self, root_dir: str) -> List[Dataset]:
        pass

    @abstractmethod
    def discover_cycles(self, dataset: Dataset, root_dir: str):
        pass

    @abstractmethod
    def scan_cycle(self, dataset: Dataset, root_dir: str, cycle_date, cycle_hour):
        pass

    def select_cycles(self, cycles, n_cycles: Optional[int]):
        if n_cycles is None:
            return cycles

        if n_cycles < 0:
            return cycles[n_cycles:]  # last N cycles

        return cycles[:n_cycles]

    def scan_dataset_cycles(self, data_root: str, n_cycles: Optional[int]):
        self.datasets = self.discover_datasets(data_root)

        for dataset in self.datasets:
            cycles = self.discover_cycles(dataset, data_root)
            selected = self.select_cycles(cycles, n_cycles)

            for cycle_date, cycle_hour in selected:
                scan_results = self.scan_cycle(
                    dataset, data_root, cycle_date, cycle_hour
                )
                yield ScanCycle(dataset, cycle_date, cycle_hour, scan_results)
