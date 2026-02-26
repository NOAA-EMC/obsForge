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
# from ds.register_datasets import register_datasets
# from ds.register_datasets import register_directory_files # , create_db_session
# from ds.test_ioda import test_ioda
from ds.scanner import Scanner

# debug session:
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
# from ds.file_orm import Base

from logging_config import configure_logging

# logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def main():
    parser = argparse.ArgumentParser(description="Discover datasets from data root")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--data-root", required=True, help="Root directory of data")

    parser.add_argument("--debug", action="store_true", help="Debug logging")
    parser.add_argument(
        "--limit-cycles",
        type=int,
        default=None,
        help="Only process the N most recent cycles",
    )

    args = parser.parse_args()

    # configure_logging(level=logging.INFO, log_file="gdas.log")
    configure_logging(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # test_ioda(args.db)

    # datasets = register_datasets(args.data_root, args.db)
    # print(f"Registered datasets: {[ds.name for ds in datasets]}")


    args = parser.parse_args()
    
    configure_logging(args.debug) 
    logger = logging.getLogger("UpdateInventory")
    
    logger.info(f"DB: {args.db}")
    logger.info(f"Scanning root: {args.data_root}")
        
    scanner = Scanner(
        db_path=args.db,
        data_root=args.data_root,
    )   

    if args.limit_cycles:
        cycles_to_process = - args.limit_cycles
    else:
        cycles_to_process = args.limit_cycles
    scanner.run(n_cycles=cycles_to_process)


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
