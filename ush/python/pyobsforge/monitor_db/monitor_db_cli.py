#!/usr/bin/env python3

import argparse
import random
from datetime import datetime, timedelta
from obsforge_monitor_db import ObsforgeMonitorDB

import matplotlib.pyplot as plt
import numpy as np


# Initialize DB
# ----------------------------------------------------------------
# python3 monitor_cli.py init-db
# 
# Generate test data for 5 days
# ----------------------------------------------------------------
# python3 monitor_cli.py generate-test-data --days 5
# 
# Print tables
# ----------------------------------------------------------------
# python3 monitor_cli.py tables tasks
# python3 monitor_cli.py tables task_runs
# 
# Task timings from last 3 days
# ----------------------------------------------------------------
# python3 monitor_cli.py task-timings --days 3
# 
# Obs count for a specific obs space
# ----------------------------------------------------------------
# python3 monitor_cli.py obs-count --obs-space radiances --days 7

# Plotting commands
# ----------------------------------------------------------------
# python monitor_cli.py plot-timings --days 5
# python monitor_cli.py plot-timings --task prep --days 5
# python monitor_cli.py plot-obs --obs-space radiances --days 7
# python monitor_cli.py plot-obs-total --spaces radiances conventional gpsro --days 7

# optional output argument for plots
# python monitor_cli.py plot-timings --days 5 --output timings.png
# python monitor_cli.py plot-timings --task prep --days 5 --output prep_timings.png
# python monitor_cli.py plot-obs --obs-space radiances --days 3 --output rad_obs.png
# python monitor_cli.py plot-obs-total --spaces radiances conventional --days 7 --output total_obs.png







# ------------------------------------------------------------
# Helper function: pretty table
# ------------------------------------------------------------
def print_table(title, rows):
    print("\n" + title)
    print("=" * len(title))

    if not rows:
        print("(no rows)")
        return

    headers = rows[0].keys()
    print(" | ".join(headers))
    print("-" * 40)

    for r in rows:
        print(" | ".join(str(r[h]) for h in headers))


# ------------------------------------------------------------
# Test data generator
# ------------------------------------------------------------
def generate_test_data(db, days=3):
    """
    Generate synthetic data for testing:
    - 4 tasks
    - 3 obs spaces
    - Task runs for N days x 4 cycles/day
    - Random obs counts
    """

    print(f"Generating {days} days of test data...")

    # Tasks (idempotent)
    t_prep = db.add_task("prep", "Preparation task", "gdas")
    t_var = db.add_task("var", "Variational task", "gdas")
    t_ens = db.add_task("ens", "Ensemble task", "gfs")
    t_post = db.add_task("post", "Post-processing task", "gfs")

    tasks = [t_prep, t_var, t_ens, t_post]

    # Obs spaces (idempotent)
    s_rad = db.add_obs_space("radiances", "Satellite radiances")
    s_conv = db.add_obs_space("conventional", "Conventional observations")
    s_gps = db.add_obs_space("gpsro", "GPS-RO")

    obs_spaces = [s_rad, s_conv, s_gps]

    # Generate multiple days
    today = datetime.utcnow().date()
    for day_offset in range(days):
        day = today - timedelta(days=day_offset)
        date_str = day.isoformat()

        for cycle in [0, 6, 12, 18]:
            for t_id in tasks:
                start = datetime(day.year, day.month, day.day, cycle, 0, 0)
                end = start + timedelta(minutes=random.randint(5, 40))

                run_id = db.log_task_run(
                    task_id=t_id,
                    date=date_str,
                    cycle=cycle,
                    run_type="gdas" if random.random() < 0.5 else "gfs",
                    start_time=start.isoformat(),
                    end_time=end.isoformat(),
                    notes=None
                )

                # Add details for each obs space
                for s_id in obs_spaces:
                    db.log_task_run_detail(
                        task_run_id=run_id,
                        obs_space_id=s_id,
                        obs_count=random.randint(10_000, 200_000),
                        runtime_sec=random.randint(10, 200)
                    )

    print("Test data generated.")


# ------------------------------------------------------------
# Class-based CLI for obsforge monitor database
# ------------------------------------------------------------
class MonitorCLI:
    def __init__(self):
        self.db = ObsforgeMonitorDB("obsforge.db")

        self.parser = argparse.ArgumentParser(description="Obsforge Monitoring CLI")
        sub = self.parser.add_subparsers(dest="command", required=True)

        # init-db
        sub.add_parser("init-db", help="Initialize database tables")

        # reset-db
        sub.add_parser("reset-db", help="Drop all tables and rebuild empty DB")

        # tables
        p_tables = sub.add_parser("tables", help="Print a DB table")
        p_tables.add_argument("table_name")

        # task timings for past N days
        p_timing = sub.add_parser("task-timings", help="Print task timings for past X days")
        p_timing.add_argument("--days", type=int, default=3)

        # obs count for obs space over X days
        p_obs = sub.add_parser("obs-count", help="Obs count for a given space over X days")
        p_obs.add_argument("--obs-space", required=True)
        p_obs.add_argument("--days", type=int, default=3)

        # generate test data
        p_gen = sub.add_parser("generate-test-data", help="Generate synthetic test dataset")
        p_gen.add_argument("--days", type=int, default=3)

        # plot: timings for all tasks or one task
        p_plot_t = sub.add_parser("plot-timings", help="Plot task runtimes for past X days")
        p_plot_t.add_argument("--task", default=None)
        p_plot_t.add_argument("--days", type=int, default=3)
        p_plot_t.add_argument("--output", help="Save plot to PNG")

        # plot: obs count for a single obs space
        p_plot_obs = sub.add_parser("plot-obs", help="Plot obs count for a single obs space")
        p_plot_obs.add_argument("--obs-space", required=True)
        p_plot_obs.add_argument("--days", type=int, default=3)
        p_plot_obs.add_argument("--output", help="Save plot to PNG")

        # plot: total obs counts for multiple obs spaces
        p_plot_total = sub.add_parser("plot-obs-total", help="Plot total obs for multiple obs spaces")
        p_plot_total.add_argument("--spaces", nargs="+", required=True)
        p_plot_total.add_argument("--days", type=int, default=3)
        p_plot_total.add_argument("--output", help="Save plot to PNG")



    # --------------------------------------------------------
    def run(self):
        args = self.parser.parse_args()

        if args.command == "init-db":
            self.db.init_db()
            print("Database initialized.")

        elif args.command == "reset-db":
            self.db.reset_db()
            print("Database reset and reinitialized.")

        elif args.command == "tables":
            self.print_table(args.table_name)

        elif args.command == "task-timings":
            self.print_task_timings(args.days)

        elif args.command == "obs-count":
            self.print_obs_counts(args.obs_space, args.days)

        elif args.command == "generate-test-data":
            generate_test_data(self.db, days=args.days)

        elif args.command == "plot-timings":
            self.plot_timings(task=args.task, days=args.days, output=args.output)

        elif args.command == "plot-obs":
            self.plot_obs_space(obs_space=args.obs_space, days=args.days, output=args.output)

        elif args.command == "plot-obs-total":
            self.plot_obs_total(obs_spaces=args.spaces, days=args.days, output=args.output)

    # --------------------------------------------------------
    def print_table(self, table_name):
        table_name = table_name.lower()
        cur = self.db.conn.cursor()

        try:
            rows = cur.execute(f"SELECT * FROM {table_name}").fetchall()
        except Exception as e:
            print(f"Error: {e}")
            return

        print_table(f"Table: {table_name}", rows)

    # --------------------------------------------------------
    def print_task_timings(self, days):
        cutoff = (datetime.utcnow().date() - timedelta(days=days)).isoformat()

        rows = self.db.conn.cursor().execute("""
            SELECT tr.date, tr.cycle, t.name AS task, tr.run_type, tr.runtime_sec
            FROM task_runs tr
            JOIN tasks t ON t.id = tr.task_id
            WHERE tr.date >= ?
            ORDER BY tr.date DESC, tr.cycle
        """, (cutoff,)).fetchall()

        print_table(f"Task timings for past {days} days", rows)

    # --------------------------------------------------------
    def print_obs_counts(self, obs_space, days):
        cutoff = (datetime.utcnow().date() - timedelta(days=days)).isoformat()

        rows = self.db.conn.cursor().execute("""
            SELECT tr.date, tr.cycle, t.name AS task, s.name AS obs_space, d.obs_count
            FROM task_run_details d
            JOIN task_runs tr ON tr.id = d.task_run_id
            JOIN tasks t ON t.id = tr.task_id
            JOIN obs_spaces s ON s.id = d.obs_space_id
            WHERE s.name = ?
              AND tr.date >= ?
            ORDER BY tr.date DESC, tr.cycle
        """, (obs_space, cutoff)).fetchall()

        print_table(
            f"Obs counts for obs_space='{obs_space}' over last {days} days",
            rows
        )

    def _plot_with_mean_std(self, x, y, label, color=None):
        y = np.array(y, dtype=float)

        mean = np.mean(y)
        std = np.std(y)

        # Main line
        plt.plot(x, y, label=label, color=color)

        # Mean line (dashed)
        plt.axhline(mean, linestyle="--", color=color, alpha=0.6,
                    label=f"{label} mean")

        # Shaded area ±1σ
        plt.fill_between(
            x,
            mean - std,
            mean + std,
            color=color,
            alpha=0.2,
        )

    # helper for output argument
    def _finalize_plot(self, output):
        if output:
            plt.savefig(output, dpi=150, bbox_inches="tight")
            print(f"Plot saved to: {output}")
            plt.close()
        else:
            plt.show()

    def plot_timings(self, task=None, days=3, output=None):
        cutoff = (datetime.utcnow().date() - timedelta(days=days)).isoformat()
        cur = self.db.conn.cursor()

        if task:
            rows = cur.execute("""
                SELECT tr.date, tr.cycle, tr.runtime_sec, t.name
                FROM task_runs tr
                JOIN tasks t ON t.id = tr.task_id
                WHERE tr.date >= ? AND t.name = ?
                ORDER BY tr.date, tr.cycle
            """, (cutoff, task)).fetchall()
        else:
            rows = cur.execute("""
                SELECT tr.date, tr.cycle, tr.runtime_sec, t.name
                FROM task_runs tr
                JOIN tasks t ON t.id = tr.task_id
                WHERE tr.date >= ?
                ORDER BY tr.date, tr.cycle
            """, (cutoff,)).fetchall()

        if not rows:
            print("No data.")
            return

        # Case 1: single task
        if task:
            x = [f"{r['date']} {r['cycle']:02d}Z" for r in rows]
            y = [r["runtime_sec"] for r in rows]

            color = plt.rcParams['axes.prop_cycle'].by_key()['color'][0]
            self._plot_with_mean_std(x, y, task, color)

            plt.title(f"Task runtime: {task} (past {days} days)")

        # Case 2: multiple tasks
        else:
            task_groups = {}
            for r in rows:
                tname = r["name"]
                task_groups.setdefault(tname, {"x": [], "y": []})
                task_groups[tname]["x"].append(f"{r['date']} {r['cycle']:02d}Z")
                task_groups[tname]["y"].append(r["runtime_sec"])

            colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

            for idx, (tname, data) in enumerate(task_groups.items()):
                color = colors[idx % len(colors)]
                self._plot_with_mean_std(data["x"], data["y"], tname, color)

            plt.title(f"All task runtimes (past {days} days)")

        plt.xlabel("Cycle")
        plt.ylabel("Runtime (sec)")
        plt.xticks(rotation=60)
        plt.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0))
        plt.tight_layout()

        self._finalize_plot(output)

    def plot_obs_space(self, obs_space, days=3, output=None):
        cutoff = (datetime.utcnow().date() - timedelta(days=days)).isoformat()

        rows = self.db.conn.cursor().execute("""
            SELECT tr.date, tr.cycle, d.obs_count
            FROM task_run_details d
            JOIN task_runs tr ON tr.id = d.task_run_id
            JOIN obs_spaces s ON s.id = d.obs_space_id
            WHERE s.name = ? AND tr.date >= ?
            ORDER BY tr.date, tr.cycle
        """, (obs_space, cutoff)).fetchall()

        if not rows:
            print("No data.")
            return

        x = [f"{r['date']} {r['cycle']:02d}Z" for r in rows]
        y = [r["obs_count"] for r in rows]

        color = plt.rcParams['axes.prop_cycle'].by_key()['color'][0]
        self._plot_with_mean_std(x, y, obs_space, color)

        plt.title(f"Obs count for '{obs_space}' (past {days} days)")
        plt.xlabel("Cycle")
        plt.ylabel("Observation count")
        plt.xticks(rotation=60)

        plt.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0))
        plt.tight_layout()

        self._finalize_plot(output)

    def plot_obs_total(self, obs_spaces, days=3, output=None):
        cutoff = (datetime.utcnow().date() - timedelta(days=days)).isoformat()

        in_clause = ",".join("?" * len(obs_spaces))
        params = [cutoff] + obs_spaces

        rows = self.db.conn.cursor().execute(f"""
            SELECT tr.date, tr.cycle, s.name, d.obs_count
            FROM task_run_details d
            JOIN task_runs tr ON tr.id = d.task_run_id
            JOIN obs_spaces s ON s.id = d.obs_space_id
            WHERE tr.date >= ? AND s.name IN ({in_clause})
            ORDER BY tr.date, tr.cycle
        """, params).fetchall()

        if not rows:
            print("No data.")
            return

        totals = {}
        for r in rows:
            key = f"{r['date']} {r['cycle']:02d}Z"
            totals.setdefault(key, 0)
            totals[key] += r["obs_count"]

        x = list(totals.keys())
        y = list(totals.values())

        color = plt.rcParams['axes.prop_cycle'].by_key()['color'][0]
        self._plot_with_mean_std(x, y, "total", color)

        space_str = ", ".join(obs_spaces)
        plt.title(f"Total obs count for [{space_str}] (past {days} days)")
        plt.xlabel("Cycle")
        plt.ylabel("Total obs")
        plt.xticks(rotation=60)

        plt.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0))
        plt.tight_layout()

        self._finalize_plot(output)


# ------------------------------------------------------------
# Run the CLI
# ------------------------------------------------------------
if __name__ == "__main__":
    MonitorCLI().run()
