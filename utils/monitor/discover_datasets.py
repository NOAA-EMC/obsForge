#!/usr/bin/env python3
"""
discover_datasets.py

Scans the data root directory to discover datasets and populates the datasets table.
"""

import argparse
import os
import logging
from pathlib import Path
from typing import List
import re

# from database.dataset_db import RunTypeService
# from scanner.dataset_cycle_scanner import dataset_cycle_scanner
from ds.dataset import Dataset
from ds.register_datasets import register_datasets
from ds.register_datasets import register_directory_files # , create_db_session

# debug session:
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from ds.file_orm import Base

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")



# def discover_datasets(data_root: str) -> List[str]:
#     """
#     Scan the data root directory and return a list of unique datasets.
#     A dataset is inferred as the alphabetic prefix of directories like 'gcdas.20260201'.
#     """
#     data_root_path = Path(data_root)
#     if not data_root_path.is_dir():
#         raise ValueError(f"Data root '{data_root}' does not exist or is not a directory")
# 
#     dataset_set = set()
#     pattern = re.compile(r"([a-zA-Z]+)\.\d{8}")  # letters + dot + 8 digits
# 
#     for d in data_root_path.iterdir():
#         if not d.is_dir():
#             continue
#         m = pattern.match(d.name)
#         if m:
#             dataset_set.add(m.group(1))
# 
#     return sorted(dataset_set)
# 
# 
# 
# def populate_datasets(db_path: str, datasets: List[str]):
#     """
#     Add discovered datasets to the database using RunTypeService.
#     """
#     with RunTypeService(db_path) as svc:
#         for rt_name in datasets:
#             dataset_id = svc.add(rt_name)
#             if dataset_id:
#                 logger.info(f"Inserted dataset '{rt_name}' with ID {dataset_id}")
#             else:
#                 logger.info(f"Run type '{rt_name}' already exists")


def main():
    parser = argparse.ArgumentParser(description="Discover datasets from data root")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--data-root", required=True, help="Root directory of data")
    args = parser.parse_args()

    # # datasets = discover_datasets(args.data_root)
    # # populate_datasets(args.db, datasets)
    # # dataset_cycle_scanner(args.data_root, args.db)

    datasets = register_datasets(args.data_root, args.db)
    print(f"Registered datasets: {[ds.name for ds in datasets]}")

    '''
    for ds in datasets:
        ds.register_cycles()
        # ds.register_cycles(args.data_root)
                   
        cycle_dir = ds.find_first_valid_cycle_dir()
        ds.register_obs_spaces(cycle_dir)
    '''

    '''
    engine = create_engine(f"sqlite:///{args.db}", echo=True)
    Session = sessionmaker(bind=engine)

    Base.metadata.create_all(engine)

    with Session() as session:
        files = register_directory_files(
            session, 
            # "/lfs/h2/emc/obsproc/noscrub/edward.givelberg/monitoring/obsForge/utils/monitor/"
            "/lfs/h2/emc/da/noscrub/emc.da/obsForge/COMROOT/realtime/gfs.20260206/18/ocean/icec"
        )
        session.commit()

    for f in files:
        print(f.id, f.path)
    '''

    # print(files)
    # for f in files:
        # print(f)


if __name__ == "__main__":
    main()
