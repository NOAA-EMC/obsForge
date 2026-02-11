#!/usr/bin/env python3
"""
discover_run_types.py

Scans the data root directory to discover run types and populates the run_types table.
"""

import argparse
import os
import logging
from pathlib import Path
from typing import List
import re

from database.run_type_db import RunTypeService

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")



def discover_run_types(data_root: str) -> List[str]:
    """
    Scan the data root directory and return a list of unique run types.
    A run type is inferred as the alphabetic prefix of directories like 'gcdas.20260201'.
    """
    data_root_path = Path(data_root)
    if not data_root_path.is_dir():
        raise ValueError(f"Data root '{data_root}' does not exist or is not a directory")

    run_type_set = set()
    pattern = re.compile(r"([a-zA-Z]+)\.\d{8}")  # letters + dot + 8 digits

    for d in data_root_path.iterdir():
        if not d.is_dir():
            continue
        m = pattern.match(d.name)
        if m:
            run_type_set.add(m.group(1))

    return sorted(run_type_set)



def populate_run_types(db_path: str, run_types: List[str]):
    """
    Add discovered run types to the database using RunTypeService.
    """
    with RunTypeService(db_path) as svc:
        for rt_name in run_types:
            run_type_id = svc.add(rt_name)
            if run_type_id:
                logger.info(f"Inserted run type '{rt_name}' with ID {run_type_id}")
            else:
                logger.info(f"Run type '{rt_name}' already exists")


def main():
    parser = argparse.ArgumentParser(description="Discover run types from data root")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--data-root", required=True, help="Root directory of data")
    args = parser.parse_args()

    run_types = discover_run_types(args.data_root)
    populate_run_types(args.db, run_types)


if __name__ == "__main__":
    main()
