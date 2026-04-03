#!/usr/bin/env python3
"""
Scans the data root directory to discover datasets and populates the datasets table.
"""

import argparse
import os
import logging
from pathlib import Path
from typing import List
import re

from logging_config import configure_logging

from app.scanner import Scanner
# from app.products_server import DataProductsServer
# from app.generate_data_products import generate_data_products
# from app.website_generator import WebsiteGenerator


def main():
    parser = argparse.ArgumentParser(description="Discover datasets from data root")
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--data-root", required=True, help="Root directory of data")
    parser.add_argument("--data-products-root", required=True, help="Root directory for data products")
    parser.add_argument("--web-dir", required=True, help="Root directory for data products")

    parser.add_argument("--debug", action="store_true", help="Debug logging")
    parser.add_argument(
        "--limit-cycles",
        type=int,
        default=None,
        help="Only process the N most recent cycles",
    )

    args = parser.parse_args()

    configure_logging(level=logging.INFO)
    # configure_logging(args.debug) 
    logger = logging.getLogger(__name__)

    # if args.limit_cycles:
        # cycles_to_process = - args.limit_cycles
    # else:
        # cycles_to_process = args.limit_cycles

    cycles_to_process = (
        None if not args.limit_cycles else -args.limit_cycles
    )

    db_path=args.db
    data_root=args.data_root
    data_products_root=args.data_products_root
    website_dir=args.web_dir

    logger.info(f"DB: {args.db}")
    logger.info(f"Scanning root: {args.data_root}")

    scanner = Scanner(
        db_path=db_path,
        data_root=data_root
    )   
    scanner.run(n_cycles=cycles_to_process)

if __name__ == "__main__":
    main()
