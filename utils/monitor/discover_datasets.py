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
from ds.test_ioda import test_ioda

# debug session:
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
# from ds.file_orm import Base

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def main():
    parser = argparse.ArgumentParser(description="Discover datasets from data root")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--data-root", required=True, help="Root directory of data")
    args = parser.parse_args()


    test_ioda(args.db)

    # datasets = register_datasets(args.data_root, args.db)
    # print(f"Registered datasets: {[ds.name for ds in datasets]}")

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
