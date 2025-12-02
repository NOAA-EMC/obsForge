import sqlite3
from typing import List, Tuple, Optional
from datetime import datetime, timedelta
from pyobsforge.monitor.monitor_db import MonitorDB


def list_tables(db: MonitorDB) -> List[str]:
    """Return all table names in the database."""
    cur = db.conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [row[0] for row in cur.fetchall()]


def print_table(db: MonitorDB, table_name: str, limit: Optional[int] = 20):
    """Print the first `limit` rows of a table."""
    cur = db.conn.cursor()
    try:
        cur.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
        rows = cur.fetchall()
        if not rows:
            print(f"No rows in table {table_name}")
            return
        col_names = [desc[0] for desc in cur.description]
        print(f"Table: {table_name}")
        print("-" * 80)
        print("\t".join(col_names))
        print("-" * 80)
        for row in rows:
            print("\t".join(str(x) for x in row))
        print("-" * 80)
    except sqlite3.OperationalError as e:
        print(f"Error: {e}")


def task_timings(db: MonitorDB, days: int = 3) -> List[Tuple]:
    """Return task runs in the last `days` days."""
    cutoff = datetime.utcnow() - timedelta(days=days)
    cur = db.conn.cursor()
    cur.execute(
        """
        SELECT tr.id, t.name, tr.date, tr.cycle, tr.runtime_sec
        FROM task_runs tr
        JOIN tasks t ON t.id = tr.task_id
        WHERE tr.date >= ?
        ORDER BY tr.date DESC, tr.cycle DESC
        """,
        (cutoff.strftime("%Y%m%d"),)
    )
    return cur.fetchall()


def obs_count_for_space(db: MonitorDB, obs_space_name: str, days: int = 3) -> List[Tuple]:
    """Return obs counts for a given obs_space in the last `days` days."""
    cutoff = datetime.utcnow() - timedelta(days=days)
    cur = db.conn.cursor()
    cur.execute(
        """
        SELECT tr.date, tr.cycle, d.obs_count
        FROM task_run_details d
        JOIN task_runs tr ON tr.id = d.task_run_id
        JOIN obs_spaces s ON s.id = d.obs_space_id
        WHERE s.name = ? AND tr.date >= ?
        ORDER BY tr.date DESC, tr.cycle DESC
        """,
        (obs_space_name, cutoff.strftime("%Y%m%d"))
    )
    return cur.fetchall()

