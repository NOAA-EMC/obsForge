import os
import logging
from typing import List, Optional

# from sqlalchemy import create_engine
# from sqlalchemy.orm import Session

# from .db_base import Base
from ds.dataset import Dataset

from .products_server import DataProductsServer

logger = logging.getLogger(__name__)


def generate_data_products(datasets, server):

    # logger.info(f"{file.netcdf_file.structure.to_json()}")

    # plot_path = "/lfs/h2/emc/obsproc/noscrub/edward.givelberg/monitoring/wowplot.png"
    # plot_path = "/scratch3/NCEPDEV/da/Edward.Givelberg/monitoring/wowplot.png"

    for ds in datasets:
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
                if not os.path.exists(plot_path):
                    logger.info(f"Generating plot for {name} --> {plot_path}")
                    file.plot_variable(plot_path)
