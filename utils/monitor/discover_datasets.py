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
from scanner.dataset_cycle_scanner import dataset_cycle_scanner

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")



def discover_datasets(data_root: str) -> List[str]:
    """
    Scan the data root directory and return a list of unique datasets.
    A dataset is inferred as the alphabetic prefix of directories like 'gcdas.20260201'.
    """
    data_root_path = Path(data_root)
    if not data_root_path.is_dir():
        raise ValueError(f"Data root '{data_root}' does not exist or is not a directory")

    dataset_set = set()
    pattern = re.compile(r"([a-zA-Z]+)\.\d{8}")  # letters + dot + 8 digits

    for d in data_root_path.iterdir():
        if not d.is_dir():
            continue
        m = pattern.match(d.name)
        if m:
            dataset_set.add(m.group(1))

    return sorted(dataset_set)



def populate_datasets(db_path: str, datasets: List[str]):
    """
    Add discovered datasets to the database using RunTypeService.
    """
    with RunTypeService(db_path) as svc:
        for rt_name in datasets:
            dataset_id = svc.add(rt_name)
            if dataset_id:
                logger.info(f"Inserted dataset '{rt_name}' with ID {dataset_id}")
            else:
                logger.info(f"Run type '{rt_name}' already exists")


def main():
    parser = argparse.ArgumentParser(description="Discover datasets from data root")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--data-root", required=True, help="Root directory of data")
    args = parser.parse_args()

    # datasets = discover_datasets(args.data_root)
    # populate_datasets(args.db, datasets)
    dataset_cycle_scanner(args.data_root, args.db)


if __name__ == "__main__":
    main()
