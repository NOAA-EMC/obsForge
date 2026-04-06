from abc import ABC, abstractmethod
from typing import List, Tuple
from datetime import date

from ds.dataset import Dataset
from ds.dataset_cycle import DatasetCycle


class new_BaseScanner:
    def discover_datasets(self):
        raise NotImplementedError

    def discover_cycles(self, dataset_name):
        raise NotImplementedError

    def scan_cycle(self, dataset_name, cycle_date, cycle_hour):
        raise NotImplementedError




class BaseScanner(ABC):
    def __init__(self, root_dir: str):
        self.root_dir = root_dir

    @abstractmethod
    def discover_datasets(self) -> List[Dataset]:
        """Find datasets in root_dir"""
        pass

    @abstractmethod
    def discover_cycles(self, dataset: Dataset) -> List[Tuple[date, str]]:
        """Find available cycles for a dataset"""
        pass

    # @abstractmethod
    # def build_cycle(
        # self, dataset: Dataset, cycle_date: date, cycle_hour: str
    # ) -> DatasetCycle:
        # """Construct a DatasetCycle with files attached"""
        # pass
