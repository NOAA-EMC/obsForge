import logging
from typing import List, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from ncdb.ds.io.dataset_repository import DatasetRepository
from .dataset import Dataset

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

        self._engine = create_engine(f"sqlite:///{db_path}")
        self._session = Session(self._engine)

        self._repo = DatasetRepository(self._session)

    def scan(self, data_root: str, n_cycles: Optional[int]) -> None:
        """
        Scan a directory and ingest data into the database.
        """
        logger.info(f"Scanning data root: {data_root}")

        from ncdb.scanners.scanner import Scanner

        scanner = Scanner(self.db_path, data_root)
        datasets = scanner.run(n_cycles)

        # for ds in datasets:
            # ds.to_db(self._repo)

        # self._session.commit()

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
