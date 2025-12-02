#!/usr/bin/env python3

import argparse
from datetime import datetime, timedelta
from pyobsforge.monitor.monitor_db import MonitorDB

from monitor_db_utils import (
    list_tables,
    print_table,
    print_full_table,
    task_timings,
    obs_count_for_space,
)


DESCRIPTION = """
ObsForge Monitor Database CLI

This tool allows inspecting the ObsForge monitoring SQLite database.

Available functionality:

1. Table inspection
   - List tables
   - Print full tables
   - Apply SQL filters
   - Limit output rows

2. Task timing inspection
   - Show task runtimes for the last N days

3. Observation count inspection
   - Show obs counts for a specific obs space

4. Aggregated totals
   - obs-total: sum obs_count grouped by obs_space

Examples:

# List all tables
python monitor_cli.py --db obsforge_task_monitor.db tables

# Print full table
python monitor_cli.py --db obsforge_task_monitor.db tables task_run_details

# Filter table
python monitor_cli.py --db obsforge_task_monitor.db tables task_run_details --filter "task_run_id=3"

# Show obs counts for an obs space
python monitor_cli.py --db obsforge_task_monitor.db obs-count --obs-space sst_viirs_npp

# Show total obs counts
python monitor_cli.py --db obsforge_task_monitor.db obs-total
"""


def main():
    parser = argparse.ArgumentParser(
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("--db", required=True, help="Path to SQLite DB file")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # TABLES
    parser_tables = subparsers.add_parser("tables", help="List or print a table")
    parser_tables.add_argument("table_name", nargs="?", help="Table name")
    parser_tables.add_argument("--limit", type=int, default=None)
    parser_tables.add_argument("--filter", default=None)

    # task timings
    parser_timings = subparsers.add_parser("task-timings")
    parser_timings.add_argument("--days", type=int, default=3)

    # obs count
    parser_obs = subparsers.add_parser("obs-count")
    parser_obs.add_argument("--obs-space", required=True)
    parser_obs.add_argument("--days", type=int, default=3)

    # aggregated
    parser_total = subparsers.add_parser("obs-total")
    parser_total.add_argument("--days", type=int, default=3)

    args = parser.parse_args()
    db = MonitorDB(args.db)

    if args.command == "tables":
        if not args.table_name:
            print("\n".join(list_tables(db)))
            return

        if args.limit and not args.filter:
            print_table(db, args.table_name, limit=args.limit)
        else:
            print_full_table(db, args.table_name, filter_sql=args.filter)

    elif args.command == "task-timings":
        for row in task_timings(db, args.days):
            print(row)

    elif args.command == "obs-count":
        for row in obs_count_for_space(db, args.obs_space, args.days):
            print(row)

    elif args.command == "obs-total":
        rows = obs_total(db, args.days)

        print("\nTotal obs counts:")
        print("----------------------------------------")
        for name, total in rows:
            print(f"{name:30s} {total}")
        print("----------------------------------------")


if __name__ == "__main__":
    main()

