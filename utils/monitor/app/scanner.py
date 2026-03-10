import os
import logging
from typing import List, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from ds.db_base import Base

from ds.dataset import Dataset

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

    '''
    def compute(self):
        from .products_server import DataProductsServer

        data_root = "/scratch3/NCEPDEV/da/Edward.Givelberg/monitoring/data_products"
        server = DataProductsServer(data_root)

        ds = self.datasets[0]
        cycle = ds.dataset_cycles[0]
        file = cycle.fields[0].files[0]

        # logger.info(f"{ds}")
        # logger.info(f"selecting {last_cycle}")
        # logger.info(f">>>>>>> {file}")
        # logger.info(f">>>>>>> {file.netcdf_file}")
        # logger.info(f">>>>>>> {file.netcdf_file.structure}")
        # logger.info(f"SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
        logger.info(f"{file.netcdf_file.structure.to_json()}")
        # logger.info(f"SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")

        # plot_path = "/lfs/h2/emc/obsproc/noscrub/edward.givelberg/monitoring/wowplot.png"
        # plot_path = "/scratch3/NCEPDEV/da/Edward.Givelberg/monitoring/wowplot.png"

        for ds in self.datasets:
            dataset_name = ds.name
            for cycle in ds.dataset_cycles:
                cycle_date = cycle.cycle_date
                hour = cycle.cycle_hour
                for field in cycle.fields:
                    file = field.files[0]
                    name = field.obs_space.name
                    plot_path = server.product_file(
                        dataset_name=dataset_name,
                        cycle_date=cycle_date,
                        hour=hour,
                        product="ombg_plots",
                        # product="obs_plots",
                        filename=f"{name}.png",
                    )
                    logger.info(f"Generating plot for {name} --> {plot_path}")
                    if not os.path.exists(plot_path):
                        file.plot_variable(plot_path)
    '''

    '''
        self.compute()

        from .website_generator import WebsiteGenerator
        site = WebsiteGenerator(
            self,
            products_dir="data_products",
            website_dir="website"
        )
        site.generate()
    '''

'''
    def tryweb(self):
        from website_generator import WebsiteGenerator
        from datetime import date

        # Assume you have these datasets and their cycles
        # datasets = ["gdas", "ecmwf"]
        # available_cycles = {
            # "gdas": ["20240212_00", "20240212_06", "20240212_12", "20240212_18", "20240213_00"],
            # "ecmwf": ["20240212_00", "20240212_12"]
        # }
        # obs_spaces = {
            # "gdas": ["temperature", "pressure", "humidity"],
            # "ecmwf": ["temperature", "wind"]
        # }
        # product_types = ["obs_plots", "netcdf_structure", "statistics"]

        dataset_names = [d.name: for d in self.datasets]
        available_cycles 

        wg = WebsiteGenerator(
            output_dir="website_output", 
            products_root="products", 
            datasets=dataset_names
        )

        # Generate pages and refresh data
        wg.generate_index()
        for ds in datasets:
            wg.refresh_data(ds, available_cycles[ds])
            wg.generate_dataset_page(ds, available_cycles[ds], obs_spaces[ds], data_root="products")
            for obs in obs_spaces[ds]:
                wg.generate_obs_space_page(ds, obs, available_cycles[ds], product_types)
'''
