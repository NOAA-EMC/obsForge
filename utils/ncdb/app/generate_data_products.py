import os
import logging
from typing import List, Optional
import re

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

def df_to_plot_data(df):
    data = []
    for ts, row in df.iterrows():
        data.append({
            "date": ts.strftime("%Y%m%d"),
            "cycle": ts.hour,
            **row.to_dict()
        })
    return data


def generate_historical_plot(
    server,
    dataset, 
    cycle, 
    variable_path, 
    session,
    field, 
    regenerate: bool = False
):
    if not variable_path:
        return
    if not field:
        return

    product_name = f"hist_{safe_name(variable_path)}"
    obs_space_name = field.obs_space.name
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

    df = field.get_variable_derived_data(
        session,
        variable_path,
        metrics=["mean", "std_dev"]
    )
    if df.empty:
        logger.error(f"No data for {product_name} / {obs_space_name}")
        return
    data = df_to_plot_data(df)

    plot_output_dir = ""
    plotter = PlotGenerator(plot_output_dir)
    plotter.generate_history_plot_with_moving_avg(
        plot_path,
        data=data,
        title=f"{obs_space_name}",
        val_key="mean",
        std_key="std_dev",
        y_label=f"{get_variable_name(variable_path)}",
        days=None,          # full history
        clamp_bottom=False  # physics variable
    )


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
    if not plot_data:
        return

    # plot_output_dir needs to go
    plot_output_dir = ""
    plotter = PlotGenerator(plot_output_dir)
    plotter.generate_surface_map(
        plot_path,
        plot_data
    )

def generate_file_data_products(datasets, server):
    for dataset in datasets:
        for cycle in dataset.dataset_cycles:
            for file in cycle.files:
                variables = file.netcdf_file.structure.list_variables("/ombg")
                variable_path = variables[0]
                variable_name = get_variable_name(variable_path)

                if (variable_name != "waterTemperature") and (variable_name != "salinity"):
                    generate_surface_plot(
                        server,
                        dataset, 
                        cycle, 
                        variable_path, 
                        file
                    ) 

def generate_db_data_products(db_path, datasets, server):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session
    from ds.db_base import Base

    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        for dataset in datasets:
            current_cycle = dataset.dataset_cycles[-1]
            for field in dataset.dataset_fields:
                file = field.files[0]
                variables = file.netcdf_file.structure.list_variables("/ombg")
                variable_path = variables[0]
                variable_name = get_variable_name(variable_path)
                generate_historical_plot(
                    server,
                    dataset,
                    current_cycle,
                    variable_path,
                    session,
                    field
                ) 

def generate_data_products(db_path, datasets, server):
    generate_file_data_products(datasets, server)
    generate_db_data_products(db_path, datasets, server)
