#!/usr/bin/env python3
"""
Step 3: Inventory Inspection & Anomaly Detection.

This script loads the InventoryInspector to validate files in the database
against defined rules (Physics, Metadata, History).
"""

import argparse
import logging
import os
import sys
import traceback

# Ensure python path if running standalone
try:
    from inspection.inspector import InventoryInspector
except ImportError:
    # If running as a script, add the project root to sys.path
    sys.path.append(
        os.path.join(os.path.dirname(__file__), '../../..')
    )
    from inspection.inspector import InventoryInspector

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def main():
    """Main entry point for the inspection process."""
    parser = argparse.ArgumentParser(
        description="Step 3: Inventory Inspection & Anomaly Detection"
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to SQLite DB"
    )
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"Error: DB not found at {args.db}")
        sys.exit(1)

    try:
        inspector = InventoryInspector(args.db)
        inspector.run_checks()
    except Exception as e:
        print(f"\n[ERROR] Inspection failed: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
