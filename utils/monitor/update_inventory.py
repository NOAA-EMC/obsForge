#!/usr/bin/env python3
"""
Step 1: Scan & Register Inventory.
"""

import argparse
import logging
import sys

from scanner.scanner import Scanner


def configure_logging(debug_mode: bool):
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if debug_mode else logging.INFO)

    if root.handlers:
        for h in root.handlers:
            root.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)

    logging.getLogger("matplotlib").setLevel(logging.WARNING)


def main():
    parser = argparse.ArgumentParser(
        description="Step 1: Scan & Register Inventory"
    )
    parser.add_argument("--db", required=True, help="Path to SQLite DB")
    parser.add_argument("--data-root", required=True, help="Root scan dir")
    parser.add_argument("--debug", action="store_true", help="Debug logging")
    parser.add_argument(
        "--limit-cycles",
        type=int,
        default=None,
        help="Only process the N most recent cycles",
    )

    args = parser.parse_args()

    configure_logging(args.debug)
    logger = logging.getLogger("UpdateInventory")

    logger.info(f"DB: {args.db}")
    logger.info(f"Scanning root: {args.data_root}")

    # --- Scanner ---
    scanner = Scanner(
        db_path=args.db,
        data_root=args.data_root,
    )

    scanner.run(limit_cycles=args.limit_cycles)
    # scanner.scan_inventory(limit_cycles=args.limit_cycles)

    # --- Reporting ---
    report = scanner.report()

    logger.info(
        f"Done. Cycles: {report['cycles']} | "
        f"Files Scanned: {report['files']}"
    )
    logger.info(
        f"Summary: {report['new_or_updated']} New/Updated, "
        f"{report['skipped']} Skipped."
    )

    return


if __name__ == "__main__":
    main()
