#!/usr/bin/env python3
# monitor_report.py
# Main entry point for inspecting the ObsForge Monitor Database.
#
# USAGE:
#   ./monitor_report.py --db path/to/db show ranges
#   ./monitor_report.py --db path/to/db plot time --task atmos --output t.png

import sys
import argparse
from typing import Optional

# Local imports (No matplotlib here!)
from monitor_db import MonitorDB
from monitor_db_util import (
    list_tables,
    print_table,
    print_full_table,
    obs_total,
    fetch_task_timings_for_plot,
    fetch_obs_count_for_space_for_plot,
    fetch_obs_count_by_category_raw,
    get_db_ranges_report
)

DESCRIPTION = """
ObsForge Monitor Reporter

Inspects task runs, observation counts, and cycle continuity.
Note: Plotting commands require 'matplotlib' and 'plotutil.py'.
"""

class MonitorReporter:
    def __init__(self, db_path: str):
        self.db = MonitorDB(db_path)
        self.parser = argparse.ArgumentParser(
            description=DESCRIPTION,
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        self.parser.add_argument("--db", required=True, help="Path to SQLite DB file")

        subparsers = self.parser.add_subparsers(dest="command", required=True)

        # ----------------------------------------------------------------------
        # COMMAND: TABLES
        # ----------------------------------------------------------------------
        p_tables = subparsers.add_parser("tables", help="List or inspect raw DB tables.")
        p_tables.add_argument("table_name", nargs="?", help="Table name")
        p_tables.add_argument("--limit", type=int, default=None, help="Limit rows printed")
        p_tables.add_argument("--filter", default=None, help="SQL WHERE clause")

        # ----------------------------------------------------------------------
        # COMMAND: SHOW (Text Reports)
        # ----------------------------------------------------------------------
        p_show = subparsers.add_parser("show", help="Show text-based reports.")
        show_sub = p_show.add_subparsers(dest="show_command", required=True)

        # show ranges
        show_sub.add_parser("ranges", help="Report available continuous cycle ranges.")

        # show time
        ps_time = show_sub.add_parser("time", help="Show task runtimes.")
        ps_time.add_argument("--days", type=int, default=None)
        ps_time.add_argument("--task", default=None)

        # show obs
        ps_obs = show_sub.add_parser("obs", help="Show observation counts.")
        ps_obs.add_argument("--days", type=int, default=None)
        ps_obs.add_argument("--aggregate", action="store_true")
        g_obs = ps_obs.add_mutually_exclusive_group()
        g_obs.add_argument("--obs-space")
        g_obs.add_argument("--obs-category")

        # ----------------------------------------------------------------------
        # COMMAND: PLOT (Visual Reports)
        # ----------------------------------------------------------------------
        p_plot = subparsers.add_parser("plot", help="Generate plots (requires matplotlib).")
        plot_sub = p_plot.add_subparsers(dest="plot_command", required=True)

        # plot time
        pp_time = plot_sub.add_parser("time", help="Plot task runtimes.")
        pp_time.add_argument("--days", type=int, default=None)
        pp_time.add_argument("--task", default=None)
        pp_time.add_argument("--output", help="Save to file (default: show window)")

        # plot obs
        pp_obs = plot_sub.add_parser("obs", help="Plot obs counts.")
        pp_obs.add_argument("--days", type=int, default=None)
        pp_obs.add_argument("--output", help="Save to file")
        gp_obs = pp_obs.add_mutually_exclusive_group(required=True)
        gp_obs.add_argument("--obs-space")
        gp_obs.add_argument("--obs-category")

    def run(self):
        # We use parse_known_args in main block, but here standard parse is fine
        args = self.parser.parse_args()

        # Dispatch
        if args.command == "tables":
            self.handle_tables(args)
        elif args.command == "show":
            self.handle_show(args)
        elif args.command == "plot":
            self.handle_plot(args)

    # --- Handlers ---

    def handle_tables(self, args):
        if not args.table_name:
            print("\n".join(list_tables(self.db)))
        elif args.filter:
            print_full_table(self.db, args.table_name, filter_sql=args.filter)
        else:
            print_table(self.db, args.table_name, limit=args.limit)

    def handle_show(self, args):
        if args.show_command == "ranges":
            print(get_db_ranges_report(self.db))
            return

        days_str = f"Last {args.days} days" if args.days is not None else "All Time"

        if args.show_command == "time":
            rows = fetch_task_timings_for_plot(self.db, args.days, task_name=args.task)
            if not rows:
                print("No data found.")
                return
            print(f"\nTask Runtimes ({days_str}):")
            print("-" * 65)
            print("Date       Cycle | Task            | Runtime (s)")
            print("-" * 65)
            for r in rows:
                print(f"{r['date']} {r['cycle']:02d}    | {r['name']:15s} | {r['runtime_sec']:.2f}")
            print("-" * 65)

        elif args.show_command == "obs":
            if args.aggregate:
                rows = obs_total(self.db, args.days)
                print(f"\nTotal Obs by Space ({days_str}):")
                print("-" * 40)
                for name, tot in rows:
                    print(f"{name:30s} {tot}")
                print("-" * 40)
            elif args.obs_space:
                rows = fetch_obs_count_for_space_for_plot(self.db, args.obs_space, args.days)
                print(f"\nObs Count: {args.obs_space} ({days_str}):")
                for r in rows:
                    print(f"{r['date']}.{r['cycle']:02d} : {r['obs_count']}")
            elif args.obs_category:
                rows = fetch_obs_count_by_category_raw(self.db, args.obs_category, args.days)
                print(f"\nObs Category: {args.obs_category} ({days_str}):")
                # r structure: (cat, space, date, cycle, count, run_count)
                for r in rows:
                    print(f"{r[1]} @ {r[2]}.{r[3]:02d} : {r[4]}")

    def handle_plot(self, args):
        """
        Dynamically imports the plotter helper.
        This keeps the main tool lightweight and functional without matplotlib.
        """
        try:
            # Local import of the second file
            from plotutil import MonitorPlotter
        except ImportError as e:
            print(f"\n[Error] Plotting unavailable.\nReason: {e}")
            print("Please ensure 'matplotlib' is installed and 'plotutil.py' is in this directory.")
            sys.exit(1)

        # Initialize Plotter with the current DB connection
        plotter = MonitorPlotter(self.db)

        if args.plot_command == "time":
            plotter.plot_timings(args.task, args.days, args.output)
        elif args.plot_command == "obs":
            plotter.plot_obs(args.obs_space, args.obs_category, args.days, args.output)


if __name__ == "__main__":
    # Pre-parse to ensure --db is grabbed even if other args fail logic later
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--db", required=True)
    temp_args, _ = parser.parse_known_args()

    app = MonitorReporter(temp_args.db)
    app.run()
