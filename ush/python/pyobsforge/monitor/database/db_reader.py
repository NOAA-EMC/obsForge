import sqlite3
from typing import List, Dict, Tuple, Optional, Any
from datetime import datetime, timedelta
from collections import defaultdict
from pyobsforge.monitor.database.monitor_db import MonitorDB


class DBReader:
    """
    Data Access Layer (DAL).
    Centralizes all read queries. Returns structured Python objects (Dicts/Lists).
    """
    def __init__(self, db_path: str):
        self.db = MonitorDB(db_path) 

    # ------------------------------------------------------------------
    # 1. Metadata & Raw Inspection (The Missing Piece)
    # ------------------------------------------------------------------
    def get_task_list(self) -> List[str]:
        """Returns sorted list of all known task names."""
        try:
            with self.db.conn:
                cur = self.db.conn.execute("SELECT name FROM tasks ORDER BY name")
                return [row[0] for row in cur.fetchall()]
        except sqlite3.OperationalError:
            return []

    def fetch_table_names(self) -> List[str]:
        """Returns list of all tables in the DB."""
        try:
            with self.db.conn:
                cur = self.db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                return [row[0] for row in cur.fetchall()]
        except Exception:
            return []

    def get_table_schema(self, table_name: str) -> List[str]:
        """Returns list of column names for a table."""
        try:
            with self.db.conn:
                # Limit 0 is a fast way to get description without data
                cur = self.db.conn.execute(f"SELECT * FROM {table_name} LIMIT 0")
                return [d[0] for d in cur.description]
        except Exception:
            return []

    def get_raw_table_rows(self, table_name: str, limit: Optional[int] = None, filter_sql: Optional[str] = None) -> List[tuple]:
        """Fetches raw rows for the 'tables' command."""
        query = f"SELECT * FROM {table_name}"
        if filter_sql:
            query += f" WHERE {filter_sql}"
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            with self.db.conn:
                cur = self.db.conn.execute(query)
                return cur.fetchall()
        except Exception as e:
            print(f"DB Read Error: {e}")
            return []

    # ------------------------------------------------------------------
    # 2. Inventory / Matrix Data
    # ------------------------------------------------------------------
    def get_inventory_matrix(self, run_type_filter: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Returns structured inventory data pivoted by Cycle.
        List of {date, cycle, run_type, tasks: {task_name: {status...}}}
        """
        # A. Fetch Flat Data
        filter_clause = ""
        params = []
        if run_type_filter:
            filter_clause = "AND tr.run_type = ?"
            params.append(run_type_filter)

        query = f"""
        SELECT 
            tr.date, 
            tr.cycle, 
            tr.run_type, 
            t.name as task_name,
            tr.logfile,
            (SELECT SUM(obs_count) FROM task_run_details trd WHERE trd.task_run_id = tr.id) as total_obs
        FROM task_runs tr
        JOIN tasks t ON tr.task_id = t.id
        WHERE 1=1 {filter_clause}
        ORDER BY tr.date DESC, tr.cycle DESC, tr.run_type ASC
        """
        
        with self.db.conn:
            cur = self.db.conn.execute(query, tuple(params))
            rows = cur.fetchall()

        # B. Group & Pivot (Business Logic for Data Structure)
        grouped = defaultdict(dict)
        ordered_keys = []
        seen_keys = set()

        for r in rows:
            date, cycle, run_type, task, logfile, count = r
            
            key = (date, cycle, run_type)
            if key not in seen_keys:
                ordered_keys.append(key)
                seen_keys.add(key)

            has_log = bool(logfile and "missing" not in (logfile or ""))
            has_obs = bool(count is not None and count > 0)
            
            # Simple status determination for transport
            if has_log and has_obs: status = "OK"
            elif has_log: status = "LOG"
            elif has_obs: status = "DAT"
            else: status = "MIS"

            grouped[key][task] = {
                "status": status,
                "has_log": has_log,
                "count": count or 0
            }

        # C. Limit & Format
        result = []
        for i, key in enumerate(ordered_keys):
            if i >= limit: break
            result.append({
                "date": key[0],
                "cycle": key[1],
                "run_type": key[2],
                "tasks": grouped[key]
            })
            
        return result

    def get_cycle_ranges(self) -> Dict[str, List[datetime]]:
        """Returns datetimes grouped by run_type for range analysis."""
        with self.db.conn:
            cur = self.db.conn.execute(
                "SELECT DISTINCT run_type, date, cycle FROM task_runs ORDER BY run_type, date, cycle"
            )
            rows = cur.fetchall()

        grouped = defaultdict(list)
        for r_type, date_str, cyc in rows:
            if not r_type: r_type = "Unknown"
            try:
                dt = datetime.strptime(f"{date_str}{cyc:02d}", "%Y%m%d%H")
                grouped[r_type].append(dt)
            except ValueError: pass
        return grouped

    # ------------------------------------------------------------------
    # 3. Data for Plots & Timings
    # ------------------------------------------------------------------
    def _calculate_cutoff_date(self, days: Optional[int]) -> str:
        if days is None: return "19700101"
        with self.db.conn:
            cur = self.db.conn.execute("SELECT MAX(date) FROM task_runs")
            row = cur.fetchone()
            max_date = row[0] if row else None
            
        if not max_date: return "19700101"
        
        try:
            dt = datetime.strptime(max_date, "%Y%m%d") - timedelta(days=days)
            return dt.strftime("%Y%m%d")
        except ValueError:
            return "19700101"

    def get_task_timings(self, days: Optional[int] = None, task_name: Optional[str] = None, run_type: Optional[str] = None) -> List[Dict]:
        """Used by 'show time' and 'plot time'."""
        cutoff = self._calculate_cutoff_date(days)
        query = """
            SELECT tr.date, tr.cycle, tr.run_type, t.name, tr.runtime_sec
            FROM task_runs tr
            JOIN tasks t ON t.id = tr.task_id
            WHERE tr.date >= ?
        """
        params = [cutoff]
        if task_name:
            query += " AND t.name = ?"
            params.append(task_name)
        if run_type:
            query += " AND tr.run_type = ?"
            params.append(run_type)
        
        query += " ORDER BY tr.date, tr.cycle"
        
        with self.db.conn:
            cur = self.db.conn.execute(query, tuple(params))
            # Manually map to dict to avoid row_factory dependencies
            return [
                {"date": r[0], "cycle": r[1], "run_type": r[2], "task": r[3], "duration": r[4]}
                for r in cur.fetchall()
            ]

    def get_obs_counts_by_space(self, obs_space: str, days: Optional[int] = None, run_type: Optional[str] = None) -> List[Dict]:
        """Used by 'plot obs --obs-space'."""
        cutoff = self._calculate_cutoff_date(days)
        query = """
            SELECT tr.date, tr.cycle, SUM(d.obs_count)
            FROM task_run_details d
            JOIN task_runs tr ON tr.id = d.task_run_id
            JOIN obs_spaces s ON s.id = d.obs_space_id
            WHERE s.name = ? AND tr.date >= ?
        """
        params = [obs_space, cutoff]
        if run_type:
            query += " AND tr.run_type = ?"
            params.append(run_type)

        query += " GROUP BY tr.date, tr.cycle ORDER BY tr.date, tr.cycle"
        
        with self.db.conn:
            cur = self.db.conn.execute(query, tuple(params))
            return [
                {"date": r[0], "cycle": r[1], "count": r[2]}
                for r in cur.fetchall()
            ]

    def get_obs_counts_by_category(self, category: str, days: Optional[int] = None, run_type: Optional[str] = None) -> List[Dict]:
        """Used by 'plot obs --obs-category'."""
        cutoff = self._calculate_cutoff_date(days)
        query = """
            SELECT tr.date, tr.cycle, c.name, SUM(d.obs_count)
            FROM task_run_details d
            JOIN task_runs tr ON tr.id = d.task_run_id
            JOIN obs_spaces s ON s.id = d.obs_space_id
            JOIN obs_space_categories c ON c.id = s.category_id
            WHERE c.name = ? AND tr.date >= ?
        """
        params = [category, cutoff]
        if run_type:
            query += " AND tr.run_type = ?"
            params.append(run_type)

        query += " GROUP BY tr.date, tr.cycle, c.name ORDER BY tr.date, tr.cycle"
        
        with self.db.conn:
            cur = self.db.conn.execute(query, tuple(params))
            return [
                {"date": r[0], "cycle": r[1], "entity": r[2], "count": r[3]}
                for r in cur.fetchall()
            ]

    def get_obs_totals(self, days: Optional[int] = None, run_type: Optional[str] = None) -> List[Tuple[str, int]]:
        """Used by 'show obs --aggregate'."""
        cutoff = self._calculate_cutoff_date(days)
        query = """
            SELECT s.name, SUM(d.obs_count)
            FROM task_run_details d
            JOIN task_runs tr ON tr.id = d.task_run_id
            JOIN obs_spaces s ON s.id = d.obs_space_id
            WHERE tr.date >= ?
        """
        params = [cutoff]
        if run_type:
            query += " AND tr.run_type = ?"
            params.append(run_type)

        query += " GROUP BY s.name ORDER BY s.name"
        
        with self.db.conn:
            cur = self.db.conn.execute(query, tuple(params))
            return cur.fetchall()

    def get_all_categories(self) -> List[str]:
        """Returns a list of all category names defined in the DB."""
        try:
            with self.db.conn:
                cur = self.db.conn.execute("SELECT name FROM obs_space_categories ORDER BY name")
                return [r[0] for r in cur.fetchall()]
        except Exception:
            return []

    def get_all_spaces(self) -> List[Tuple[str, str]]:
        """
        Returns list of (space_name, category_name).
        Useful for iterating over all spaces.
        """
        try:
            with self.db.conn:
                sql = """
                    SELECT s.name, c.name 
                    FROM obs_spaces s 
                    JOIN obs_space_categories c ON s.category_id = c.id 
                    ORDER BY s.name
                """
                cur = self.db.conn.execute(sql)
                return cur.fetchall()
        except Exception:
            return []
