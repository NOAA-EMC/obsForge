#!/usr/bin/env python3

import argparse
import logging
import sys

from processing.website_data.products import WebsiteDataProducts


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate monitoring data products (plots, etc.)"
    )

    parser.add_argument(
        "--db",
        required=True,
        help="Path to monitoring SQLite database"
    )

    parser.add_argument(
        "--data-root",
        required=True,
        help="Root directory of observation data tree"
    )

    parser.add_argument(
        "--out",
        required=True,
        help="Directory where generated products will be written"
    )

    parser.add_argument(
        "--limit-cycles",
        type=int,
        default=None,
        help="Only process the N most recent cycles",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    logger = logging.getLogger("process_data")

    logger.info("Starting data product generation")
    logger.info(f"DB path     : {args.db}")
    logger.info(f"Data root   : {args.data_root}")
    logger.info(f"Output dir  : {args.out}")

    try:
        data = WebsiteDataProducts(
            db_path=args.db,
            data_root=args.data_root,
            output_dir=args.out,
            limit_cycles=args.limit_cycles
        )

        data.generate()

    except Exception as e:
        logger.exception("Data product generation failed")
        sys.exit(1)

    logger.info("Data product generation completed successfully")


if __name__ == "__main__":
    main()
