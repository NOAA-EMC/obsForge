import logging
logger = logging.getLogger(__name__)

import os
from typing import List, Optional

# from sqlalchemy import create_engine
# from sqlalchemy.orm import Session

# from ds.db_base import Base
# from ds.io.dataset_repository import DatasetRepository

from ds.file import File
from ds.dataset import Dataset

from .base import BaseScanner


class Scanner(BaseScanner):
    def old__init__(self, db_path: str, root_dir: str):
        self.db_path = db_path
        self.root_dir = root_dir

        self.engine = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(self.engine)

        self.datasets: List[Dataset] = []

    # def is_valid_cycle_hour(self, hour: str) -> bool:
        # return hour in {"00", "06", "12", "18"}

    def discover_datasets(self) -> None:
        if not os.path.exists(self.root_dir):
            logger.error(
                f"Data root not found: {self.root_dir}"
            )
            self.datasets = []

        dataset_names = set()

        for entry in os.listdir(self.root_dir):
            full_path = os.path.join(self.root_dir, entry)

            if not os.path.isdir(full_path):
                continue

            if "." in entry:
                prefix = entry.split(".")[0]
                dataset_names.add(prefix)

        self.datasets = [
            Dataset(name=name, root_dir=self.root_dir)
            for name in sorted(dataset_names)
        ]

        logger.info(f"Discovered {len(self.datasets)} datasets {[d.name for d in self.datasets]}")

    def discover_cycles(self, dataset: Dataset):
        import re
        from datetime import datetime

        if not dataset.root_dir or not os.path.isdir(dataset.root_dir):
            logger.warning(f"Invalid root_dir for dataset '{dataset.name}'")
            return []

        pattern = re.compile(rf"^{re.escape(dataset.name)}\.(\d{{8}})$")

        discovered = []

        for entry in os.listdir(dataset.root_dir):
            entry_path = os.path.join(dataset.root_dir, entry)

            if not os.path.isdir(entry_path):
                continue

            match = pattern.match(entry)
            if not match:
                continue

            cycle_date_str = match.group(1)

            try:
                cycle_date = datetime.strptime(cycle_date_str, "%Y%m%d").date()
            except ValueError:
                logger.warning(f"Invalid date format: {cycle_date_str}")
                continue

            for hour_entry in os.listdir(entry_path):
                hour_path = os.path.join(entry_path, hour_entry)

                if (
                    os.path.isdir(hour_path)
                    # and hour_entry in DatasetCycle.VALID_HOURS
                    and self.is_valid_cycle_hour(hour_entry)
                ):
                    discovered.append((cycle_date, hour_entry))

        discovered.sort(key=lambda x: (x[0], x[1]))

        logger.info(
            f"Discovered {len(discovered)} cycles for dataset {dataset.name}"
        )

        return discovered

    def select_files(self, files, dataset_name, cycle_hour):
        return [
            f for f in files
            if f.path.endswith(".nc")
        ]

    def build_cycle_dir(self, dataset_name, cycle_date, cycle_hour):
        return os.path.join(
            self.root_dir,
            f"{dataset_name}.{cycle_date.strftime('%Y%m%d')}",
            cycle_hour,
        )

    def parse_obs_space(self, path):
        filename = os.path.basename(path)

        if not filename.endswith(".nc"):
            return None

        return filename.removesuffix(".nc")


################################################

    '''
    def _scan_files(self, root_path):
        all_files = []
        for dirpath, dirnames, filenames in os.walk(root_path):
            if not dirnames:
                for filename in filenames:
                    full_path = os.path.join(dirpath, filename)
                    all_files.append(File.from_path(full_path))
        return all_files
    '''

    '''
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
    '''


    def old_run(self, n_cycles: Optional[int] = None):
        self.discover_datasets()

        with Session(self.engine) as session:
            repo = DatasetRepository(session)

            for ds in self.datasets:
                repo.save_dataset(ds)
                repo.load_fields(ds)

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
