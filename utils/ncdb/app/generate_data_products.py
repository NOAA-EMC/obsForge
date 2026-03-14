import os
import logging
from typing import List, Optional
import re

# from sqlalchemy import create_engine
# from sqlalchemy.orm import Session

# from .db_base import Base
from ds.dataset import Dataset
from plotting.plot_generator import PlotGenerator

from .products_server import DataProductsServer

logger = logging.getLogger(__name__)


def safe_name(s: str) -> str:
    return re.sub(r'_+', '_',
                  re.sub(r'[^A-Za-z0-9._-]', '_', s.replace('/', '_'))
                 ).strip('_')

def get_variable_name(path: str) -> str:
    return path.rsplit('/', 1)[-1]


def generate_surface_plot(
    server,
    dataset, 
    cycle, 
    variable_path, 
    file, 
    regenerate: bool = False
):
    if not variable_path:
        return
    product_name = safe_name(variable_path)

    obs_space_name = file.dataset_field.obs_space.name
    plot_path = server.product_file(
        dataset_name=dataset.name,
        cycle_date=cycle.cycle_date,
        hour=cycle.cycle_hour,
        product=product_name,
        filename=f"{obs_space_name}.png",
    )

    if not regenerate and os.path.exists(plot_path):
        return

    logger.info(f"Plotting {product_name} / {obs_space_name} --> {plot_path}")
    plot_data = file.get_surface_variable_data(variable_path)

    # plot_output_dir needs to go
    plot_output_dir = ""
    plotter = PlotGenerator(plot_output_dir)
    plotter.generate_surface_map(
        plot_path,
        plot_data
    )

def generate_data_products(datasets, server):
    for dataset in datasets:
        for cycle in dataset.dataset_cycles:
            for file in cycle.files:
                variables = file.netcdf_file.structure.list_variables("/ombg")
                variable_path = variables[0]
                variable_name = get_variable_name(variable_path)
                if (variable_name != "waterTemperature"):
                    generate_surface_plot(
                        server,
                        dataset, 
                        cycle, 
                        variable_path, 
                        file
                    ) 
