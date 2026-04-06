import logging
logger = logging.getLogger(__name__)

import os
from typing import List, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from ds.db_base import Base

from ds.dataset import Dataset
from ds.dataset_cycle import DatasetCycle
from ds.io.dataset_repository import DatasetRepository

from .base import BaseScanner
from .file_scanner import SubdirFileScanner, NcObsSpaceNameParser


class Scanner(BaseScanner):
    def __init__(self, db_path: str, root_dir: str):
        self.db_path = db_path
        self.root_dir = root_dir

        self.engine = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(self.engine)

        self.datasets: List[Dataset] = []

        self.file_scanner = SubdirFileScanner("analysis/ocean/diags")
        self.parser = NcObsSpaceNameParser()


    def discover_datasets(self) -> None:
        if not os.path.exists(self.root_dir):
            # raise FileNotFoundError(
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

        # return self.datasets


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
                    and hour_entry in DatasetCycle.VALID_HOURS
                ):
                    discovered.append((cycle_date, hour_entry))

        discovered.sort(key=lambda x: (x[0], x[1]))

        logger.info(
            f"Discovered {len(discovered)} cycles for dataset {dataset.name}"
        )

        return discovered


    def scan_cycle(self, dataset_name, cycle_date, cycle_hour):
        # 1. Build cycle directory path
        cycle_dir = os.path.join(
            self.root_dir,
            f"{dataset_name}.{cycle_date.strftime('%Y%m%d')}",
            cycle_hour,
        )
        # print("SCAN DIR:", cycle_dir, os.path.exists(cycle_dir))

        # 2. Scan all files
        all_files = self.file_scanner.scan(cycle_dir)
        # print(f"{len(all_files)} FILES scanned")

        # 3. Filter + parse
        pairs = self.file_scanner.select_and_parse(
            all_files,
            self.parser,
            dataset_name,
            cycle_hour
        )

        # pairs = List[(File, obs_space_name)]
        return pairs



    def run(self, n_cycles: Optional[int] = None):
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



    def old_run(self, n_cycles: Optional[int] = None):
        self.discover_datasets()
        with Session(self.engine) as session:
            repo = DatasetRepository(session)
            for ds in self.datasets:
                repo.save_dataset(ds)
                
                repo.load_fields(ds)

                cycles = self.discover_cycles(ds)
                # cycles = ds.discover_cycles()
                selected = Dataset._select_cycles(cycles, n_cycles)

                for cycle_date, cycle_hour in selected:
                    # cycle = ds.build_cycle(cycle_date, cycle_hour)
                    cycle = self.build_cycle(ds, cycle_date, cycle_hour)
                    repo.save_cycle(cycle)
                    session.commit()




    # LEGACY METHODS

    def read(self, n_cycles: Optional[int] = None) -> None:
        for ds in self.datasets:
            discovered = ds.discover_cycles()
            selected = Dataset._select_cycles(discovered, n_cycles)
            for d, h in selected:
                cycle = ds.build_cycle(d, h) # Logic is unified here
                ds.add_cycle(cycle)

    def persist(self, n_cycles: Optional[int] = None) -> None:
        with Session(self.engine) as session:
            repo = DatasetRepository(session)
            for ds in self.datasets:
                # ds.to_db(session, n_cycles)
                ds.to_db(repo, n_cycles)
                logger.info(f"Persisted dataset: {ds.name} (ID={ds.id})")

            session.commit()

    def batch_run(self, n_cycles: Optional[int] = None) -> None:
        self.discover()
        self.read(n_cycles=n_cycles)
        self.persist(n_cycles=n_cycles)
