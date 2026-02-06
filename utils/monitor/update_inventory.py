#!/usr/bin/env python3
"""
Step 1: Scan & Register Inventory.

Scan filesystem → build in-memory inventory → persist via Registrar.
"""

import argparse
import logging
import sys

from database.monitor_db import MonitorDB
from scanner.inventory_scanner import InventoryScanner
from scanner.persistence import ScannerStateReader, Registrar


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

    # --- DB + Registrar ---
    db = MonitorDB(args.db)
    registrar = Registrar(db)

    # --- Load previous scan state ---
    state_reader = ScannerStateReader(args.db)
    known_state = state_reader.get_known_state()

    scanner = InventoryScanner(
        args.data_root,
        known_state=known_state,
    )

    # --- Scan + Persist ---
    for cycle in scanner.scan_filesystem(
        limit=args.limit_cycles
    ):
        logger.info(
            f"Processing cycle {cycle.date} {cycle.cycle:02d}"
        )
        registrar.persist_cycle(cycle)

    # --- Reporting ---
    report = registrar.report()

    logger.info(
        f"Done. Cycles: {report['cycles']} | "
        f"Files Scanned: {report['files']}"
    )
    logger.info(
        f"Summary: {report['new_or_updated']} New/Updated, "
        f"{report['skipped']} Skipped."
    )

    return report


if __name__ == "__main__":
    main()
