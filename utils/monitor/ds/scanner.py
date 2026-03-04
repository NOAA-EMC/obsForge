import os
import logging
from typing import List, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from .db_base import Base
from .dataset import Dataset

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

        logger.info(f"Discovered {len(self.datasets)} datasets")

    def read(self, n_cycles: Optional[int] = None) -> None:
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

        ds = self.datasets[1]
        last_cycle = ds.dataset_cycles[-1]
        file = last_cycle.fields[0].files[0]

        logger.info(f"{ds}")
        logger.info(f"selecting {last_cycle}")
        logger.info(f">>>>>>> {file}")
        logger.info(f">>>>>>> {file.netcdf_file}")
        logger.info(f">>>>>>> {file.netcdf_file.structure}")
        logger.info(f"SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
        logger.info(f"{file.netcdf_file.structure.to_json()}")
        logger.info(f"SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")

        plot_path = "/lfs/h2/emc/obsproc/noscrub/edward.givelberg/monitoring/wowplot.png"
        file.plot_variable(plot_path)
