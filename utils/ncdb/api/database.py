import logging
from typing import List, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from ncdb.ds.io.dataset_repository import DatasetRepository
from ncdb.scanners.marine_da_scanner import MarineDAScanner as DefaultScanner
from .dataset import Dataset

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_path: str, scanner=None):
        self.db_path = db_path

        self._engine = create_engine(f"sqlite:///{db_path}")
        self._session = Session(self._engine)

        self._repo = DatasetRepository(self._session)

        self._scanner_cls = scanner or DefaultScanner

    def _ingest_scan(self, scanner, n_cycles):
        for ds, cycle_date, cycle_hour, scan_results in scanner.scan_dataset_cycles(n_cycles):
            self._repo.save_dataset(ds)
            self._repo.load_fields(ds)

            cycle = ds.build_cycle(
                cycle_date, cycle_hour, scan_results
            )

            self._repo.save_cycle(cycle)

        self._session.commit()

    def scan(self, data_root: str, n_cycles: Optional[int]) -> None:
        logger.info(f"Scanning data root: {data_root}")

        scanner = self._scanner_cls(self.db_path, data_root)

        self._ingest_scan(scanner, n_cycles)

        logger.info("Scan complete")

    def list_datasets(self) -> List[str]:
        """
        Returns list of dataset names.
        """
        datasets = self._repo.get_all_datasets()
        return [d.name for d in datasets]

    def dataset(self, name: str):
        """
        Load a dataset by name.
        """
        datasets = self._repo.get_all_datasets()

        for d in datasets:
            if d.name == name:
                return Dataset(d, self._repo) 
                # load fields immediately (needed for API)
                # self._repo.load_fields(d)
                # return d

        raise ValueError(f"Dataset '{name}' not found")
