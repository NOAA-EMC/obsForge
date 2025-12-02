#!/usr/bin/env python3

import argparse
from pyobsforge.monitor.monitor_db import MonitorDB
from monitor_db_utils import list_tables, print_table, task_timings, obs_count_for_space


def main():
    parser = argparse.ArgumentParser(description="ObsForge DB CLI")
    parser.add_argument("--db", required=True, help="Path to database file")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Tables
    parser_tables = subparsers.add_parser("tables", help="List tables or print a table")
    parser_tables.add_argument("table_name", nargs="?", help="Table name to print (omit to list)")

    # Task timings
    parser_timings = subparsers.add_parser("task-timings", help="Show recent task timings")
    parser_timings.add_argument("--days", type=int, default=3, help="Number of past days to include")

    # Obs count
    parser_obs = subparsers.add_parser("obs-count", help="Show obs count for a specific obs space")
    parser_obs.add_argument("--obs-space", required=True)
    parser_obs.add_argument("--days", type=int, default=3)

    args = parser.parse_args()
    db = MonitorDB(args.db)

    if args.command == "tables":
        if args.table_name:
            print_table(db, args.table_name)
        else:
            print("\n".join(list_tables(db)))

    elif args.command == "task-timings":
        rows = task_timings(db, args.days)
        for r in rows:
            print(r)

    elif args.command == "obs-count":
        rows = obs_count_for_space(db, args.obs_space, args.days)
        for r in rows:
            print(r)


if __name__ == "__main__":
    main()

