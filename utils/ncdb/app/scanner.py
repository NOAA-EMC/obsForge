import os
import logging
from typing import List, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from ds.db_base import Base
from ds.dataset import Dataset
from ds.dataset_cycle import DatasetCycle

logger = logging.getLogger(__name__)


class Scanner:
    """
    Application service with strict 3-phase pipeline:

        1) discover()
        2) read()
        3) persist()
    """

    def __init__(self, db_path: str, data_root: str):
        self.db_path = db_path
        self.data_root = data_root

        self.engine = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(self.engine)

        self.datasets: List[Dataset] = []

    def discover(self) -> None:
        if not os.path.exists(self.data_root):
            raise FileNotFoundError(
                f"Data root not found: {self.data_root}"
            )

        dataset_names = set()

        for entry in os.listdir(self.data_root):
            full_path = os.path.join(self.data_root, entry)

            if not os.path.isdir(full_path):
                continue

            if "." in entry:
                prefix = entry.split(".")[0]
                dataset_names.add(prefix)

        self.datasets = [
            Dataset(name=name, root_dir=self.data_root)
            for name in sorted(dataset_names)
        ]

        logger.info(f"Discovered {len(self.datasets)} datasets {[d.name for d in self.datasets]}")

    def read(self, n_cycles: Optional[int] = None) -> None:
        # logger.info(f"Reading cycles (limit = {n_cycles})")
        if n_cycles == 0:
            n_cycles = None
        for ds in self.datasets:
            ds.read_cycles(n_cycles)

    def persist(self, n_cycles: Optional[int] = None) -> None:
        with Session(self.engine) as session:
            for ds in self.datasets:
                ds.to_db(session, n_cycles)
                logger.info(f"Persisted dataset: {ds.name} (ID={ds.id})")

            session.commit()

    def run(self, n_cycles: Optional[int] = None) -> None:
        self.discover()
        self.read(n_cycles=n_cycles)
        self.persist(n_cycles=n_cycles)

    def old_run_streaming(self, n_cycles: Optional[int] = None) -> None:
        self.discover()

        with Session(self.engine) as session:
            for ds in self.datasets:
                ds.to_db_self(session)

                cycles = ds.discover_cycles()
                selected = Dataset._select_cycles(cycles, n_cycles)

                for cycle_date, cycle_hour in selected:
                    cycle_files = DatasetCycle.read_cycle_files(ds, cycle_date, cycle_hour)

                    cycle = DatasetCycle(ds, cycle_date, cycle_hour)

                    ds.add_cycle(cycle_date, cycle_hour, cycle_files)

                    cycle.to_db(session)

                    session.commit()  # or flush + periodic commit


    def run_streaming(self, n_cycles: Optional[int] = None):
        self.discover()

        with Session(self.engine) as session:
            for ds in self.datasets:
                ds.to_db_self(session)

                # ds.load_fields_from_db(session)

                cycles = ds.discover_cycles()
                selected = Dataset._select_cycles(cycles, n_cycles)

                for cycle_date, cycle_hour in selected:
                    cycle = DatasetCycle.from_dir(ds, cycle_date, cycle_hour)
                    ds.add_cycle(cycle)
                    cycle.to_db(session)

                session.commit()
