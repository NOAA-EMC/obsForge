
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from obsforge_monitor import ObsforgeMonitor


# ------------------------------------------------------------
# Helper printing functions
# ------------------------------------------------------------
def print_table(title, rows):
    print("\n" + title)
    print("-" * len(title))

    if not rows:
        print("(no rows)")
        return

    # Print header
    headers = rows[0].keys()
    print(" | ".join(headers))
    print("-" * 40)

    # Print rows
    for r in rows:
        print(" | ".join(str(r[h]) for h in headers))


# ------------------------------------------------------------
# TEST / DEMO SCRIPT
# ------------------------------------------------------------
def main():
    print("Initializing test database...")

    db = ObsforgeMonitor("test_obsforge.db")
    db.reset_db()

    # Add sample tasks
    t_prep = db.add_task("prep", "Preparation task", "gdas")
    t_var = db.add_task("variational", "VAR task", "gdas")

    # Add sample obs spaces
    s_rad = db.add_obs_space("radiances", "Satellite radiances")
    s_conv = db.add_obs_space("conventional", "Conventional observations")

    # Insert a couple of task runs
    date = "2025-01-01"

    # Run 1 - PREP 00Z
    start = "2025-01-01T00:00:00"
    end = "2025-01-01T00:10:20"
    run1 = db.log_task_run(t_prep, date, 0, "gdas", start, end)

    db.log_task_run_detail(run1, s_rad, obs_count=120000)
    db.log_task_run_detail(run1, s_conv, obs_count=25000)

    # Run 2 - VAR 00Z
    start2 = "2025-01-01T00:15:00"
    end2 = "2025-01-01T00:42:10"
    run2 = db.log_task_run(t_var, date, 0, "gdas", start2, end2)

    db.log_task_run_detail(run2, s_rad, obs_count=110000)
    db.log_task_run_detail(run2, s_conv, obs_count=30000)

    # ------------------------------------------------------------
    # Extract + print
    # ------------------------------------------------------------
    all_runs = db.fetch_all_runs()
    print_table("Task Runs", all_runs)

    for r in all_runs:
        details = db.fetch_run_details(r["id"])
        print_table(f"Details for Run {r['id']} ({r['task']})", details)


if __name__ == "__main__":
    main()

