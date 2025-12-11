import sqlite3
from typing import List, Tuple, Optional, Dict
from datetime import datetime, timedelta
from collections import defaultdict

# Assuming MonitorDB class is in the file 'monitor_db.py' in the same directory
from monitor_db import MonitorDB


# ---------------------------------------------------------------------
# Helper: Date Calculation
# ---------------------------------------------------------------------

def get_latest_task_run_date(db: MonitorDB) -> Optional[str]:
    """Returns the latest 'date' (YYYYMMDD) found in task_runs."""
    cur = db.conn.cursor()
    cur.execute("SELECT MAX(date) FROM task_runs;")
    row = cur.fetchone()
    # Ensure the result is not None or an empty string
    return row[0] if row and row[0] else None


def calculate_cutoff_date(db: MonitorDB, days: Optional[int] = None) -> str:
    """
    Calculates the cutoff date string (YYYYMMDD) for SQL queries.
    - If days is None (default behavior), returns a date far in the past ("19700101").
    - If days is specified, returns max_date - N days.
    """
    latest_date_str = get_latest_task_run_date(db)

    # Case 1: No data in the DB or 'days' is None (Show All)
    if days is None or not latest_date_str:
        return "19700101"

    # Case 2: Days is specified (Calculate relative date)
    try:
        latest_date = datetime.strptime(latest_date_str, "%Y%m%d").date()
        cutoff_date = latest_date - timedelta(days=days)
        return cutoff_date.strftime("%Y%m%d")
    except ValueError:
        return "19700101"


# ---------------------------------------------------------------------
# Raw Print Utility Functions
# ---------------------------------------------------------------------

def list_tables(db: MonitorDB) -> List[str]:
    cur = db.conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [row[0] for row in cur.fetchall()]


def print_table(db: MonitorDB, table_name: str, limit: Optional[int] = None):
    """Prints table contents. Default is full table, limit restricts rows."""
    cur = db.conn.cursor()
    try:
        sql = f"SELECT * FROM {table_name}"

        if limit is not None and limit > 0:
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
    """Prints table contents with a custom filter (no limit)."""
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


def obs_total(db: MonitorDB, days: Optional[int] = None) -> List[Tuple[str, int]]:
    """Returns total obs counts aggregated by obs space."""
    cutoff = calculate_cutoff_date(db, days)

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

# ---------------------------------------------------------------------
# Data Fetching for CLI Output (Show/Plot)
# ---------------------------------------------------------------------

def fetch_task_timings_for_plot(db: MonitorDB, days: Optional[int] = None, task_name: Optional[str] = None) -> List[Dict]:
    """Fetches task runtimes, used for 'show time' and 'plot time'."""
    cutoff = calculate_cutoff_date(db, days)
    cur = db.conn.cursor()

    base_query = """
        SELECT tr.date, tr.cycle, tr.runtime_sec, t.name
        FROM task_runs tr
        JOIN tasks t ON t.id = tr.task_id
        WHERE tr.date >= ?
    """
    params = [cutoff]

    if task_name:
        base_query += " AND t.name = ?"
        params.append(task_name)

    base_query += " ORDER BY tr.date, tr.cycle"

    cur.execute(base_query, tuple(params))

    col_names = [desc[0] for desc in cur.description]
    rows = [dict(zip(col_names, row)) for row in cur.fetchall()]
    return rows


def fetch_obs_count_for_space_for_plot(db: MonitorDB, obs_space: str, days: Optional[int] = None) -> List[Dict]:
    """
    Fetches obs count for a single space, AGGREGATING by date and cycle,
    to enforce the "once per cycle" view.
    """
    cutoff = calculate_cutoff_date(db, days)
    cur = db.conn.cursor()

    cur.execute("""
        SELECT
            tr.date,
            tr.cycle,
            SUM(d.obs_count) AS obs_count_sum,
            COUNT(d.id) AS run_count
        FROM task_run_details d
        JOIN task_runs tr ON tr.id = d.task_run_id
        JOIN obs_spaces s ON s.id = d.obs_space_id
        WHERE s.name = ? AND tr.date >= ?
        GROUP BY tr.date, tr.cycle
        ORDER BY tr.date, tr.cycle
    """, (obs_space, cutoff))

    col_names = [desc[0] for desc in cur.description]
    rows = []
    for row in cur.fetchall():
        row_dict = dict(zip(col_names, row))
        # Rename the summed column for consistency in the CLI
        row_dict['obs_count'] = row_dict.pop('obs_count_sum')
        rows.append(row_dict)

    return rows


def fetch_obs_count_by_category_raw(db: MonitorDB, obs_category_name: str, days: Optional[int] = None) -> List[Tuple]:
    """
    Fetches aggregated obs counts for all spaces within a category,
    used for 'show obs --obs-category'.
    """
    cutoff = calculate_cutoff_date(db, days)

    cur = db.conn.cursor()
    # Note: We aggregate here to show a single, consolidated obs count per space per cycle
    cur.execute(
        """
        SELECT
            c.name AS category,
            s.name AS space,
            tr.date,
            tr.cycle,
            SUM(d.obs_count) AS total_obs_count,
            COUNT(d.id) AS run_count
        FROM task_run_details d
        JOIN task_runs tr ON tr.id = d.task_run_id
        JOIN obs_spaces s ON s.id = d.obs_space_id
        JOIN obs_space_categories c ON c.id = s.category_id
        WHERE c.name = ? AND tr.date >= ?
        GROUP BY c.name, s.name, tr.date, tr.cycle
        ORDER BY tr.date DESC, tr.cycle DESC, s.name
        """,
        (obs_category_name, cutoff)
    )
    return cur.fetchall()


def fetch_obs_count_by_category_for_plot(db: MonitorDB, obs_category_name: str, days: Optional[int] = None) -> List[Dict]:
    """
    Fetches obs counts aggregated across all spaces in a category by date/cycle,
    used for 'plot obs --obs-category'.
    """
    cutoff = calculate_cutoff_date(db, days)

    cur = db.conn.cursor()
    cur.execute("""
        SELECT tr.date, tr.cycle, SUM(d.obs_count) AS total_obs
        FROM task_run_details d
        JOIN task_runs tr ON tr.id = d.task_run_id
        JOIN obs_spaces s ON s.id = d.obs_space_id
        JOIN obs_space_categories c ON c.id = s.category_id
        WHERE c.name = ? AND tr.date >= ?
        GROUP BY tr.date, tr.cycle
        ORDER BY tr.date, tr.cycle
    """, (obs_category_name, cutoff))

    col_names = [desc[0] for desc in cur.description]
    rows = [dict(zip(col_names, row)) for row in cur.fetchall()]
    return rows


def get_db_ranges_report(db: MonitorDB) -> str:
    """
    Generates a formatted text report of continuous cycle ranges for each run type.
    """
    cur = db.conn.cursor()

    # 1. Fetch DISTINCT cycles per run_type.
    # (We use DISTINCT because multiple tasks like 'atmos' and 'marine'
    #  might both run for the same cycle/run_type).
    cur.execute("""
        SELECT DISTINCT run_type, date, cycle
        FROM task_runs
        ORDER BY run_type, date, cycle
    """)
    rows = cur.fetchall()

    if not rows:
        return "Database is empty."

    # 2. Group datetimes by run_type
    grouped_runs = defaultdict(list)
    for r_type, date_str, cyc_int in rows:
        if not r_type:
            r_type = "Unknown"
        try:
            # Construct datetime object: "20251120" + 0 -> 2025-11-20 00:00:00
            dt = datetime.strptime(f"{date_str}{cyc_int:02d}", "%Y%m%d%H")
            grouped_runs[r_type].append(dt)
        except ValueError:
            continue

    # 3. Build Report
    report_lines = []
    report_lines.append("\nAvailable Cycle Ranges per Run Type")
    report_lines.append("=" * 60)

    for r_type in sorted(grouped_runs.keys()):
        timestamps = grouped_runs[r_type]
        ranges = _condense_to_ranges(timestamps)

        # Format strings
        range_strs = []
        for start, end in ranges:
            s_str = start.strftime('%Y%m%d%H')
            e_str = end.strftime('%Y%m%d%H')
            if s_str == e_str:
                range_strs.append(s_str)    # Single point
            else:
                range_strs.append(f"{s_str} through {e_str}")

        report_lines.append(f"Run Type: {r_type}")
        # Indent ranges for readability
        for r_str in range_strs:
            report_lines.append(f"  - {r_str}")
        report_lines.append("")     # Empty line between types

    return "\n".join(report_lines)


def _condense_to_ranges(sorted_dates: List[datetime], interval_hours=6) -> List[Tuple[datetime, datetime]]:
    """
    Helper: Condenses a list of sorted datetimes into continuous ranges.
    A gap larger than 'interval_hours' breaks the continuity.
    """
    if not sorted_dates:
        return []

    ranges = []
    start = sorted_dates[0]
    prev = sorted_dates[0]
    gap = timedelta(hours=interval_hours)

    for current in sorted_dates[1:]:
        # If the jump between this cycle and the previous one is > 6 hours (or custom interval)
        if current - prev > gap:
            ranges.append((start, prev))
            start = current
        prev = current

    # Close the final range
    ranges.append((start, prev))
    return ranges
