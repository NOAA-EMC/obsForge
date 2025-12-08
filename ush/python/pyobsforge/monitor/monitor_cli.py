#!/usr/bin/env python3

import argparse
from typing import List, Optional, Dict
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import numpy as np

# Assuming MonitorDB class is in the file 'monitor_db.py' in the same directory
from monitor_db import MonitorDB 

from monitor_db_util import (
    list_tables,
    print_table,
    print_full_table,
    obs_total,
    fetch_task_timings_for_plot,
    fetch_obs_count_for_space_for_plot,
    fetch_obs_count_by_category_raw,
    fetch_obs_count_by_category_for_plot,
)

DESCRIPTION = """
ObsForge Monitor Database CLI

This tool provides raw data inspection ('show') and visualization ('plot') 
of monitored task runs and observation data.

Commands:
- tables: List tables or print content.
- show time: Raw runtimes for tasks.
- plot time: Plot runtimes for tasks.
- show obs: Raw obs counts for spaces/categories.
- plot obs: Plot obs totals for spaces/categories.

Note: By default, all commands show/plot ALL available data. Use --days N to filter 
relative to the latest recorded date.

Examples:

# Print full 'tasks' table
python monitor_cli.py --db obs.db tables tasks

# Show all available runtimes for the 'prep' task
python monitor_cli.py --db obs.db show time --task prep

# Plot runtimes for the last 7 days of data
python monitor_cli.py --db obs.db plot time --days 7 --output timings.png

# Show aggregated total counts for spaces in the 'radiance' category, exposing run redundancy
python monitor_cli.py --db obs.db show obs --obs-category radiance
"""


class MonitorCLI:
    def __init__(self, db_path: str):
        self.db = MonitorDB(db_path)
        self.parser = argparse.ArgumentParser(
            description=DESCRIPTION,
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        self.parser.add_argument("--db", required=True, help="Path to SQLite DB file")
        subparsers = self.parser.add_subparsers(dest="command", required=True)

        # ----------------------------------------------------------------------
        # TABLES COMMAND
        # ----------------------------------------------------------------------
        parser_tables = subparsers.add_parser("tables", help="List tables or print a table.")
        parser_tables.add_argument("table_name", nargs="?", help="Table name (optional)")
        parser_tables.add_argument("--limit", type=int, default=None, help="Limit rows printed (defaults to printing the entire table).")
        parser_tables.add_argument("--filter", default=None, help="SQL WHERE clause filter")

        # ----------------------------------------------------------------------
        # SHOW COMMANDS (Raw Data Output)
        # ----------------------------------------------------------------------
        parser_show = subparsers.add_parser("show", help="Show raw data (time or obs)")
        show_sub = parser_show.add_subparsers(dest="show_command", required=True)

        # SHOW TIME
        p_show_time = show_sub.add_parser("time", help="Show task runtimes.")
        p_show_time.add_argument("--days", type=int, default=None, help="Number of days back from the LATEST recorded date.")
        p_show_time.add_argument("--task", default=None, help="Filter by a specific task name.")

        # SHOW OBS
        p_show_obs = show_sub.add_parser("obs", help="Show observation metrics.")
        p_show_obs.add_argument("--days", type=int, default=None, help="Number of days back from the LATEST recorded date.")
        group_show_obs = p_show_obs.add_mutually_exclusive_group()
        group_show_obs.add_argument("--obs-space", help="Filter by a specific obs space name.")
        group_show_obs.add_argument("--obs-category", help="Filter by an observation category.")
        p_show_obs.add_argument("--aggregate", action="store_true", help="Aggregate total obs count by space/category.")

        # ----------------------------------------------------------------------
        # PLOT COMMANDS (Graphical Output)
        # ----------------------------------------------------------------------
        parser_plot = subparsers.add_parser("plot", help="Plot data (time or obs)")
        plot_sub = parser_plot.add_subparsers(dest="plot_command", required=True)

        # PLOT TIME
        p_plot_time = plot_sub.add_parser("time", help="Plot task runtimes.")
        p_plot_time.add_argument("--days", type=int, default=None, help="Number of days back from the LATEST recorded date.")
        p_plot_time.add_argument("--task", default=None, help="Optional: Task name to plot single task.")
        p_plot_time.add_argument("--output", help="Save plot to PNG file.")

        # PLOT OBS
        p_plot_obs = plot_sub.add_parser("obs", help="Plot observation metrics.")
        p_plot_obs.add_argument("--days", type=int, default=None, help="Number of days back from the LATEST recorded date.")
        group_plot_obs = p_plot_obs.add_mutually_exclusive_group(required=True)
        group_plot_obs.add_argument("--obs-space", help="Plot time series for a single observation space.")
        group_plot_obs.add_argument("--obs-category", help="Plot time series for the total count of a category.")
        p_plot_obs.add_argument("--output", help="Save plot to PNG file.")
    
    # --- Plotting Helpers ---
    def _plot_with_mean_std(self, x: List, y: List, label: str, color: Optional[str] = None):
        """Plots a line with mean and +/- 1 standard deviation shaded area."""
        y = np.array(y, dtype=float)

        mean = np.mean(y)
        std = np.std(y)

        # Main line
        plt.plot(x, y, label=label, color=color)

        # Mean line (dashed)
        plt.axhline(mean, linestyle="--", color=color, alpha=0.6,
                    label=f"{label} mean ({mean:.2f})")

        # Shaded area ±1σ
        plt.fill_between(
            x,
            mean - std,
            mean + std,
            color=color,
            alpha=0.2,
        )

    def _finalize_plot(self, output: Optional[str]):
        """Handles saving or showing the plot."""
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0))
        plt.tight_layout()

        if output:
            plt.savefig(output, dpi=150, bbox_inches="tight")
            print(f"Plot saved to: {output}")
            plt.close()
        else:
            plt.show()

    # --- Plotting Command Implementations ---

    def plot_timings(self, task: Optional[str], days: Optional[int], output: Optional[str]):
        """Implements 'plot time'"""
        rows = fetch_task_timings_for_plot(self.db, days, task_name=task)
        
        if not rows:
            print("No data found for plotting.")
            return

        # Warning for large datasets if no explicit days filter was used
        if days is None and len(rows) > 100:
            print("⚠️ Warning: Plotting all available data resulted in over 100 cycles. "
                  "To improve clarity and performance, consider using a time filter, e.g., '--days 7'.")

        plt.figure(figsize=(12, 6))

        if task:
            x = [f"{r['date']} {r['cycle']:02d}Z" for r in rows]
            y = [r["runtime_sec"] for r in rows]
            color = plt.rcParams['axes.prop_cycle'].by_key()['color'][0]
            self._plot_with_mean_std(x, y, task, color)
            plt.title(f"Task Runtime: {task} (Last {days if days is not None else 'All'} Days)")
        else:
            task_groups: Dict[str, Dict[str, List]] = {}
            for r in rows:
                tname = r["name"]
                task_groups.setdefault(tname, {"x": [], "y": []})
                task_groups[tname]["x"].append(f"{r['date']} {r['cycle']:02d}Z")
                task_groups[tname]["y"].append(r["runtime_sec"])

            colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
            for idx, (tname, data) in enumerate(task_groups.items()):
                color = colors[idx % len(colors)]
                self._plot_with_mean_std(data["x"], data["y"], tname, color)
            plt.title(f"All Task Runtimes (Last {days if days is not None else 'All'} Days)")

        plt.xlabel("Cycle")
        plt.ylabel("Runtime (sec)")
        plt.xticks(rotation=60)
        self._finalize_plot(output)

    def plot_obs(self, obs_space: Optional[str], obs_category: Optional[str], days: Optional[int], output: Optional[str]):
        """Implements 'plot obs'"""
        plt.figure(figsize=(12, 6))
        rows = [] 

        # Case 1: Plotting a single observation space (Aggregated by Cycle)
        if obs_space:
            rows = fetch_obs_count_for_space_for_plot(self.db, obs_space, days)
            if not rows:
                print(f"No data found for obs space '{obs_space}'.")
                return

            x = [f"{r['date']} {r['cycle']:02d}Z" for r in rows]
            y = [r["obs_count"] for r in rows]
            
            color = plt.rcParams['axes.prop_cycle'].by_key()['color'][0]
            self._plot_with_mean_std(x, y, obs_space, color)
            plt.title(f"Obs Count for '{obs_space}' (Last {days if days is not None else 'All'} Days)")
            
        # Case 2: Plotting the aggregated total of an observation category
        elif obs_category:
            rows = fetch_obs_count_by_category_for_plot(self.db, obs_category, days)
            if not rows:
                print(f"No data found for obs category '{obs_category}'.")
                return

            x = [f"{r['date']} {r['cycle']:02d}Z" for r in rows]
            y = [r["total_obs"] for r in rows]

            color = plt.rcParams['axes.prop_cycle'].by_key()['color'][0]
            self._plot_with_mean_std(x, y, f"Total {obs_category}", color)
            plt.title(f"Total Obs Count for Category '{obs_category}' (Last {days if days is not None else 'All'} Days)")

        # Warning for large datasets if no explicit days filter was used
        if days is None and len(rows) > 100:
            print("⚠️ Warning: Plotting all available data resulted in over 100 cycles. "
                  "To improve clarity and performance, consider using a time filter, e.g., '--days 7'.")
            
        plt.xlabel("Cycle")
        plt.ylabel("Observation count")
        plt.xticks(rotation=60)
        plt.ticklabel_format(style='plain', axis='y') # Prevent scientific notation
        self._finalize_plot(output)

    # --- Main Run Logic ---

    def run(self):
        args = self.parser.parse_args()

        if args.command == "tables":
            if not args.table_name:
                print("\n".join(list_tables(self.db)))
            elif args.filter:
                print_full_table(self.db, args.table_name, filter_sql=args.filter)
            else:
                print_table(self.db, args.table_name, limit=args.limit)
            return

        elif args.command == "show":
            if args.show_command == "time":
                self.show_time(args.days, args.task)
            elif args.show_command == "obs":
                self.show_obs(args.days, args.obs_space, args.obs_category, args.aggregate)
            return

        elif args.command == "plot":
            if args.plot_command == "time":
                self.plot_timings(args.task, args.days, args.output)
            elif args.plot_command == "obs":
                self.plot_obs(args.obs_space, args.obs_category, args.days, args.output)
            return

    def show_time(self, days: Optional[int], task: Optional[str]):
        """Implements 'show time' (raw task runtimes)"""
        rows = fetch_task_timings_for_plot(self.db, days, task_name=task)
        
        if not rows:
            print(f"No task runtime data found.")
            return

        days_str = f"Last {days} days" if days is not None else "All Time"
        task_str = f"Task: {task}" if task else "All Tasks"
        
        print(f"\nTask Runtimes ({task_str}, {days_str}):")
        print("------------------------------------------------------------------")
        print(" | ".join(["Date", "Cycle", "Task", "Runtime (sec)"]))
        print("------------------------------------------------------------------")
        for r in rows:
            print(f"{r['date']} | {r['cycle']:02d} | {r['name']:15s} | {r['runtime_sec']:.2f}")
        print("------------------------------------------------------------------")

    def show_obs(self, days: Optional[int], obs_space: Optional[str], obs_category: Optional[str], aggregate: bool):
        """Implements 'show obs' (raw obs counts)"""
        days_str = f"Last {days} days" if days is not None else "All Time"
        
        if aggregate:
            rows = obs_total(self.db, days)
            
            print(f"\nTotal Obs Counts Aggregated by Space ({days_str}):")
            print("----------------------------------------")
            for name, total in rows:
                print(f"{name:30s} {total}")
            print("----------------------------------------")
            return

        elif obs_space:
            # Show AGGREGATED data for a single space (one line per cycle)
            rows = fetch_obs_count_for_space_for_plot(self.db, obs_space, days)
            
            if not rows:
                 print(f"No data found for space '{obs_space}'.")
                 return
            
            print(f"\nObs Counts for Space: {obs_space} ({days_str}):")
            print("---------------------------------------------------------")
            print(" | ".join(["Date", "Cycle", "Run Count", "Total Obs Count"]))
            print("---------------------------------------------------------")
            for r in rows:
                print(f"{r['date']} | {r['cycle']:02d} | {r['run_count']:9d} | {r['obs_count']}")
            print("---------------------------------------------------------")
            return

        elif obs_category:
            # Show raw data for all spaces in a category (aggregated by space/cycle)
            rows = fetch_obs_count_by_category_raw(self.db, obs_category, days)
            
            if not rows:
                 print(f"No data found for category '{obs_category}'.")
                 return
            
            print(f"\nObs Counts for Category: {obs_category} ({days_str}):")
            print("-------------------------------------------------------------------------------------------------")
            print(" | ".join(["Category", "Space", "Date", "Cycle", "Run Count", "Total Obs Count"]))
            print("-------------------------------------------------------------------------------------------------")
            for r in rows:
                print(f"{r[0]:10s} | {r[1]:15s} | {r[2]} | {r[3]:02d} | {r[5]:9d} | {r[4]}")
            print("-------------------------------------------------------------------------------------------------")
            return
        
        else:
            # If no filter is specified, default to showing all aggregated totals
            self.show_obs(days, None, None, aggregate=True)


if __name__ == "__main__":
    # Manually parse --db argument to instantiate the class correctly
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--db", required=True)
    temp_args, _ = parser.parse_known_args()
    
    cli = MonitorCLI(temp_args.db)
    cli.run()

