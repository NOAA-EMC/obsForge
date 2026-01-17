import logging
import math
import sqlite3
from collections import defaultdict
from typing import Any, Dict, List, Optional

logger = logging.getLogger("ReportDataService")


class ReportDataService:
    """
    Data Access Layer for Reporting (Website & CLI).

    Provides read-only access to the database for generating plots,
    HTML tables, and command-line summaries.
    """

    def __init__(self, db_path: str):
        # Direct SQLite connection
        # check_same_thread=False allows usage in simple multi-threaded contexts
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def fetch_all(self, sql: str, params: tuple = ()) -> List[sqlite3.Row]:
        """Helper to execute query and return all rows."""
        try:
            cur = self.conn.cursor()
            cur.execute(sql, params)
            return cur.fetchall()
        except sqlite3.Error as e:
            logger.error(f"SQL Error: {e}")
            return []

    def fetch_one(
        self, sql: str, params: tuple = ()
    ) -> Optional[sqlite3.Row]:
        """Helper to execute query and return a single row."""
        try:
            cur = self.conn.cursor()
            cur.execute(sql, params)
            return cur.fetchone()
        except sqlite3.Error as e:
            logger.error(f"SQL Error: {e}")
            return None

    # ==========================================================================
    # 1. DATABASE METADATA & UTILITIES (For CLI/Debugging)
    # ==========================================================================

    def fetch_table_names(self) -> List[str]:
        """Returns a list of all tables in the database."""
        sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        rows = self.fetch_all(sql)
        return [r[0] for r in rows]

    def get_table_schema(self, table_name: str) -> List[str]:
        """Returns column names for a given table."""
        try:
            rows = self.fetch_all(f"PRAGMA table_info({table_name})")
            return [r['name'] for r in rows]
        except Exception:
            return []

    def get_raw_table_rows(
        self, table_name: str, limit: int = None, filter_sql: str = None
    ) -> List[tuple]:
        """Fetches raw rows for CLI inspection."""
        sql = f"SELECT * FROM {table_name}"
        if filter_sql:
            sql += f" WHERE {filter_sql}"
        if limit:
            sql += f" LIMIT {limit}"

        rows = self.fetch_all(sql)
        return [tuple(r) for r in rows]

    # ==========================================================================
    # 2. CONFIGURATION & ENTITY METADATA
    # ==========================================================================

    def get_all_run_types(self) -> List[str]:
        """Returns list of run types (e.g. ['gdas', 'gfs'])."""
        sql = """
            SELECT DISTINCT run_type
            FROM task_runs
            WHERE run_type IS NOT NULL
            ORDER BY run_type
        """
        rows = self.fetch_all(sql)
        return [r[0] for r in rows]

    def get_all_categories(self) -> List[str]:
        """Returns list of categories (e.g. ['Marine', 'Atmosphere'])."""
        sql = "SELECT name FROM obs_space_categories ORDER BY name"
        rows = self.fetch_all(sql)
        return [r[0] for r in rows]

    def get_obs_spaces_for_category(self, category: str) -> List[str]:
        """Returns list of Obs Space names for a category."""
        sql = """
            SELECT s.name
            FROM obs_spaces s
            JOIN obs_space_categories c ON s.category_id = c.id
            WHERE c.name = ?
            ORDER BY s.name
        """
        rows = self.fetch_all(sql, (category,))
        return [r[0] for r in rows]

    def get_all_task_names(self, run_type: str) -> List[str]:
        """Returns list of distinct tasks executed for a run type."""
        sql = """
            SELECT DISTINCT t.name
            FROM task_runs tr
            JOIN tasks t ON tr.task_id = t.id
            WHERE tr.run_type = ?
        """
        rows = self.fetch_all(sql, (run_type,))
        return [r[0] for r in rows]

    # ==========================================================================
    # 3. FILE STATUS & STATISTICS
    # ==========================================================================

    def get_flagged_files(self, run_type: str):
        """Returns files marked as Anomalies (Warning/Failure)."""
        sql = """
            SELECT
                fi.file_path, fi.integrity_status, fi.error_message,
                os.name as obs_space, tr.date, tr.cycle
            FROM file_inventory fi
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            WHERE tr.run_type = ? AND fi.integrity_status != 'OK'
            ORDER BY tr.date DESC, tr.cycle DESC, os.name
        """
        return [dict(r) for r in self.fetch_all(sql, (run_type,))]

    def get_file_statistics(self, pat: str):
        """CLI Helper: Returns stats for files matching a pattern."""
        sql = """
            SELECT
                fi.file_path, v.name as variable,
                s.min_val, s.max_val, s.mean_val, s.std_dev
            FROM file_variable_statistics s
            JOIN file_inventory fi ON s.file_id = fi.id
            JOIN variables v ON s.variable_id = v.id
            WHERE fi.file_path LIKE ?
            ORDER BY fi.file_path, v.name
            LIMIT 50
        """
        return [dict(r) for r in self.fetch_all(sql, (f"%{pat}%",))]

    def get_obs_space_domains(self, run_type, space):
        """
        Fetches Lat/Lon bounds AND Depth/Pressure bounds for the LATEST cycle.
        Used to display domain info on the website.
        """
        # 1. Get Spatial/Time Bounds (from file_domains table)
        sql_domain = """
            SELECT
                fd.min_lat, fd.max_lat,
                fd.min_lon, fd.max_lon,
                tr.date, tr.cycle
            FROM file_domains fd
            JOIN file_inventory fi ON fd.file_id = fi.id
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            WHERE tr.run_type = ? AND os.name = ?
            ORDER BY tr.date DESC, tr.cycle DESC
            LIMIT 1
        """
        res = self.fetch_one(sql_domain, (run_type, space))
        domain = dict(res) if res else {}

        if not domain:
            return {}

        # 2. Get Vertical Bounds (Depth/Pressure from statistics table)
        latest_date = domain['date']
        latest_cycle = domain['cycle']

        sql_vert = """
            SELECT
                v.name, MIN(s.min_val) as min_v, MAX(s.max_val) as max_v
            FROM file_variable_statistics s
            JOIN variables v ON s.variable_id = v.id
            JOIN file_inventory fi ON s.file_id = fi.id
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            WHERE tr.run_type = ? AND os.name = ?
              AND (v.name = 'depth' OR v.name = 'pressure' OR
                   v.name = 'air_pressure')
              AND tr.date = ? AND tr.cycle = ?
            GROUP BY v.name
        """
        vert_rows = self.fetch_all(
            sql_vert, (run_type, space, latest_date, latest_cycle)
        )

        for r in vert_rows:
            domain[f"{r['name']}_min"] = r['min_v']
            domain[f"{r['name']}_max"] = r['max_v']

        return domain

    def get_obs_space_schema_details(self, space_name):
        """Returns dimensionality info for determining if a plot should be 3D."""
        sql = """
            SELECT v.dimensionality
            FROM obs_space_content c
            JOIN variables v ON c.variable_id = v.id
            JOIN obs_spaces os ON c.obs_space_id = os.id
            WHERE os.name = ?
        """
        rows = self.fetch_all(sql, (space_name,))
        return [dict(r) for r in rows]

    def get_obs_space_schema(self, space_name):
        """Returns list of variables in the Obs Space (for dropdowns)."""
        sql = """
            SELECT c.group_name, v.name
            FROM obs_space_content c
            JOIN variables v ON c.variable_id = v.id
            JOIN obs_spaces s ON c.obs_space_id = s.id
            WHERE s.name = ?
        """
        rows = self.fetch_all(sql, (space_name,))
        return [dict(r) for r in rows]

    # ==========================================================================
    # 4. INVENTORY MATRIX (Compressed Status View)
    # ==========================================================================

    def get_compressed_inventory(
        self, run_type_filter: str = None, limit: int = None
    ):
        """
        Returns inventory history.
        Collapses sequential rows into a group if all tasks are SUCCEEDED
        and files are OK.
        """
        sql_tasks = """
            SELECT
                tr.id as run_id, tr.date, tr.cycle,
                t.name as task, tr.status, tr.run_type
            FROM task_runs tr
            JOIN tasks t ON tr.task_id = t.id
        """
        params = []
        if run_type_filter:
            sql_tasks += " WHERE tr.run_type = ?"
            params.append(run_type_filter)

        sql_tasks += " ORDER BY tr.date DESC, tr.cycle DESC"
        if limit:
            sql_tasks += f" LIMIT {limit}"

        rows = self.fetch_all(sql_tasks, tuple(params))

        # Identify "Bad" runs (runs that produced flagged files)
        sql_bad = (
            "SELECT task_run_id FROM file_inventory "
            "WHERE integrity_status != 'OK'"
        )
        bad_run_ids = set(r[0] for r in self.fetch_all(sql_bad))

        cycles = defaultdict(dict)
        ordered_keys = []

        # Group tasks by Cycle
        for r in rows:
            key = (r['date'], r['cycle'], r['run_type'])
            if key not in cycles:
                ordered_keys.append(key)

            status = r['status']
            # If the output file was bad, downgrade status to WARNING
            if r['run_id'] in bad_run_ids:
                status = 'WARNING'
            cycles[key][r['task']] = status

        compressed = []
        current_group = []

        # Compress Logic
        for key in ordered_keys:
            tasks = cycles[key]
            # Strict Collapse Rule: Must be SUCCEEDED and Clean (No Warnings)
            is_perfect = tasks and all(s == 'SUCCEEDED' for s in tasks.values())

            if is_perfect:
                # If structure matches previous group item, add to group
                if current_group and \
                        set(cycles[current_group[-1]].keys()) == set(tasks.keys()):
                    current_group.append(key)
                else:
                    # Structure changed, flush old group and start new one
                    self._flush(compressed, current_group, cycles)
                    current_group = [key]
            else:
                # Imperfect cycle, flush group and add this one individually
                self._flush(compressed, current_group, cycles)
                current_group = []
                compressed.append({
                    'type': 'single',
                    'date': key[0], 'cycle': key[1], 'run_type': key[2],
                    'tasks': tasks
                })

        self._flush(compressed, current_group, cycles)
        return compressed

    def get_inventory_matrix(self, *args, **kwargs):
        """CLI Alias for get_compressed_inventory."""
        return self.get_compressed_inventory(*args, **kwargs)

    def _flush(self, result, group, data):
        """Helper to write a group (or singles) to the result list."""
        if not group:
            return

        # Don't compress very small groups (keep < 3 visible)
        if len(group) < 3:
            for k in group:
                result.append({
                    'type': 'single',
                    'date': k[0], 'cycle': k[1], 'run_type': k[2],
                    'tasks': data[k]
                })
        else:
            s, e = group[0], group[-1]
            result.append({
                'type': 'group',
                'start_date': s[0], 'start_cycle': s[1],
                'end_date': e[0], 'end_cycle': e[1],
                'run_type': s[2],
                'count': len(group),
                'tasks': data[s]
            })

    # ==========================================================================
    # 5. PLOTTING DATA (Time Series)
    # ==========================================================================

    def get_task_timing_series(
        self, run_type: str, task: str, days: int = None
    ):
        """Returns avg runtime per cycle. Used for Temporal Variance plotting."""
        sql = """
            SELECT tr.date, tr.cycle, AVG(tr.runtime_sec) as mean_runtime
            FROM task_runs tr JOIN tasks t ON tr.task_id = t.id
            WHERE tr.run_type = ? AND t.name = ? AND tr.runtime_sec > 0
        """
        params = [run_type, task]
        if days:
            sql += " AND tr.date >= strftime('%Y%m%d', date('now', ?))"
            params.append(f"-{days} days")

        sql += " GROUP BY tr.date, tr.cycle ORDER BY tr.date, tr.cycle"

        return [dict(r) for r in self.fetch_all(sql, tuple(params))]

    def get_category_counts(
        self, run_type: str, category: str, days: int = None
    ):
        """Calculates Total Obs per cycle for the Category."""
        sql = """
            SELECT
                tr.date, tr.cycle,
                SUM(fi.obs_count) as total_obs
            FROM file_inventory fi
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            JOIN obs_space_categories c ON os.category_id = c.id
            WHERE tr.run_type = ? AND c.name = ?
        """
        params = [run_type, category]
        if days:
            sql += " AND tr.date >= strftime('%Y%m%d', date('now', ?))"
            params.append(f"-{days} days")

        sql += " GROUP BY tr.date, tr.cycle ORDER BY tr.date, tr.cycle"

        return [dict(r) for r in self.fetch_all(sql, tuple(params))]

    def get_obs_space_counts(
        self, run_type: str, obs_space: str, days: int = None
    ):
        """Calculates Total Obs per cycle for the Obs Space."""
        sql = """
            SELECT
                tr.date, tr.cycle,
                SUM(fi.obs_count) as total_obs
            FROM file_inventory fi
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            WHERE tr.run_type = ? AND os.name = ?
        """
        params = [run_type, obs_space]
        if days:
            sql += " AND tr.date >= strftime('%Y%m%d', date('now', ?))"
            params.append(f"-{days} days")

        sql += " GROUP BY tr.date, tr.cycle ORDER BY tr.date, tr.cycle"

        return [dict(r) for r in self.fetch_all(sql, tuple(params))]

    def get_variable_physics_series(
        self, run_type: str, space: str, var: str, days: int = None
    ):
        """
        Aggregates multiple files per cycle into a single spatial Mean/Std point.
        This enables 'Spatial Variance' plotting.
        """
        sql = """
            SELECT
                tr.date, tr.cycle,
                AVG(s.mean_val) as avg_mean,
                AVG(s.std_dev) as avg_std
            FROM file_variable_statistics s
            JOIN file_inventory fi ON s.file_id = fi.id
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            JOIN variables v ON s.variable_id = v.id
            WHERE tr.run_type = ? AND os.name = ? AND v.name = ?
        """
        params = [run_type, space, var]
        if days:
            sql += " AND tr.date >= strftime('%Y%m%d', date('now', ?))"
            params.append(f"-{days} days")

        sql += " GROUP BY tr.date, tr.cycle ORDER BY tr.date, tr.cycle"

        return [
            dict(
                date=r['date'], cycle=r['cycle'],
                mean_val=r['avg_mean'], std_dev=r['avg_std']
            )
            for r in self.fetch_all(sql, tuple(params))
        ]

    # ==========================================================================
    # 6. CLI ADAPTERS (For legacy/command-line tools)
    # ==========================================================================

    def get_obs_totals(self, days=7, run_type=None):
        """CLI: Returns aggregated totals by Obs Space."""
        sql = """
            SELECT os.name, SUM(fi.obs_count) as total
            FROM file_inventory fi
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            JOIN task_runs tr ON fi.task_run_id = tr.id
            WHERE tr.date >= strftime('%Y%m%d', date('now', ?))
        """
        params = [f"-{days} days"]
        if run_type:
            sql += " AND tr.run_type = ?"
            params.append(run_type)

        sql += " GROUP BY os.name ORDER BY total DESC LIMIT 20"
        rows = self.fetch_all(sql, tuple(params))
        return [(r['name'], r['total']) for r in rows]

    def get_task_timings(self, days=None, task_name=None, run_type=None):
        """CLI Adapter for timing data."""
        if task_name and run_type:
            series = self.get_task_timing_series(run_type, task_name, days)
            return [
                {
                    'date': r['date'],
                    'cycle': r['cycle'],
                    'task': task_name,
                    'duration': r['mean_runtime']
                }
                for r in series
            ]
        return []

    def get_category_obs_sums(self, *args, **kwargs):
        """CLI Alias."""
        return self.get_category_counts(*args, **kwargs)

    def get_obs_counts_by_category(self, category, days, run_type):
        """CLI Adapter."""
        data = self.get_category_counts(run_type, category, days)
        return [
            {'date': r['date'], 'cycle': r['cycle'], 'count': r['total_obs']}
            for r in data
        ]

    def get_obs_counts_by_space(self, space, days, run_type):
        """CLI Adapter."""
        data = self.get_obs_space_counts(run_type, space, days)
        return [
            {'date': r['date'], 'cycle': r['cycle'], 'count': r['total_obs']}
            for r in data
        ]

    def get_physics_series(self, *args, **kwargs):
        """CLI Alias."""
        return self.get_variable_physics_series(*args, **kwargs)
