import sqlite3
from typing import List, Tuple, Optional
from datetime import datetime, timedelta
from pyobsforge.monitor.monitor_db import MonitorDB


def list_tables(db: MonitorDB) -> List[str]:
    cur = db.conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [row[0] for row in cur.fetchall()]


def print_table(db: MonitorDB, table_name: str, limit: Optional[int] = 20):
    cur = db.conn.cursor()
    try:
        sql = f"SELECT * FROM {table_name}"
        if limit:
            sql += f" LIMIT {limit}"

        cur.execute(sql)
        rows = cur.fetchall()

        if not rows:
            print(f"No rows in table {table_name}")
            return

        col_names = [desc[0] for desc in cur.description]

        print(f"Table: {table_name}")
        print("-" * 100)
        print(" | ".join(col_names))
        print("-" * 100)

        for row in rows:
            print(" | ".join(str(x) for x in row))

        print("-" * 100)

    except sqlite3.OperationalError as e:
        print(f"Error: {e}")


def print_full_table(db: MonitorDB, table_name: str, filter_sql: Optional[str] = None):
    cur = db.conn.cursor()

    query = f"SELECT * FROM {table_name}"
    if filter_sql:
        query += f" WHERE {filter_sql}"

    try:
        cur.execute(query)
        rows = cur.fetchall()
        col_names = [d[0] for d in cur.description]
    except Exception as e:
        print(f"Error querying table {table_name}: {e}")
        return

    print(f"\nTable: {table_name}")
    print("-" * 100)
    print(" | ".join(col_names))
    print("-" * 100)

    if not rows:
        print("(no rows)")
    else:
        for row in rows:
            print(" | ".join(str(x) for x in row))

    print("-" * 100 + "\n")


def task_timings(db: MonitorDB, days: int = 3) -> List[Tuple]:
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


def obs_total(db: MonitorDB, days: int = 3) -> List[Tuple[str, int]]:
    """
    Returns total obs counts aggregated by obs space.
    """
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y%m%d")

    cur = db.conn.cursor()
    cur.execute(
        """
        SELECT s.name, SUM(d.obs_count)
        FROM task_run_details d
        JOIN task_runs tr ON tr.id = d.task_run_id
        JOIN obs_spaces s ON s.id = d.obs_space_id
        WHERE tr.date >= ?
        GROUP BY s.name
        ORDER BY s.name
        """,
        (cutoff,)
    )

    return cur.fetchall()

