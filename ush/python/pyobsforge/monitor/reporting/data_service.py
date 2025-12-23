from collections import defaultdict
from typing import List, Dict, Any
from pyobsforge.monitor.database.connection import DBConnection

class ReportDataService:
    """
    Data Access Layer for the Website Generator and CLI Reports.
    Handles aggregation, formatting, and business logic for views.
    """
    def __init__(self, db_path: str):
        self.conn = DBConnection(db_path)

    # ==========================================================================
    # 1. METADATA & UTILS (Used by CLI)
    # ==========================================================================
    
    def fetch_table_names(self) -> List[str]:
        rows = self.conn.fetch_all("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        return [r[0] for r in rows]

    def get_table_schema(self, table_name: str) -> List[str]:
        try:
            rows = self.conn.fetch_all(f"PRAGMA table_info({table_name})")
            return [r['name'] for r in rows]
        except: return []

    def get_raw_table_rows(self, table_name: str, limit: int = None, filter_sql: str = None) -> List[tuple]:
        sql = f"SELECT * FROM {table_name}"
        if filter_sql: sql += f" WHERE {filter_sql}"
        if limit: sql += f" LIMIT {limit}"
        try:
            with self.conn.get_cursor() as cur:
                return [tuple(r) for r in cur.execute(sql).fetchall()]
        except: return []

    def get_cycle_ranges(self):
        try:
            sql = "SELECT DISTINCT run_type FROM task_runs"
            rows = self.conn.fetch_all(sql)
            return {r['run_type']: [] for r in rows}
        except: return {}

    # ==========================================================================
    # 2. SCHEMA & STATS (Used by CLI)
    # ==========================================================================

    def get_obs_space_schema_details(self, obs_space_name: str):
        sql = """
            SELECT 
                c.group_name, v.name as var_name, v.data_type, v.dimensionality
            FROM obs_space_content c
            JOIN variables v ON c.variable_id = v.id
            JOIN obs_spaces os ON c.obs_space_id = os.id
            WHERE os.name = ?
            ORDER BY c.group_name, v.name
        """
        return [dict(r) for r in self.conn.fetch_all(sql, (obs_space_name,))]

    def get_file_statistics(self, file_path_pattern: str):
        sql = """
            SELECT 
                f.file_path, c.group_name, v.name as variable,
                s.min_val, s.max_val, s.mean_val, s.std_dev
            FROM file_variable_statistics s
            JOIN file_inventory f ON s.file_id = f.id
            JOIN variables v ON s.variable_id = v.id
            JOIN obs_space_content c ON (c.obs_space_id = f.obs_space_id AND c.variable_id = v.id)
            WHERE f.file_path LIKE ?
            ORDER BY f.file_path, v.name
            LIMIT 50
        """
        return [dict(r) for r in self.conn.fetch_all(sql, (f"%{file_path_pattern}%",))]

    # ==========================================================================
    # 3. WEB & DASHBOARD LOGIC
    # ==========================================================================

    def get_all_run_types(self) -> List[str]:
        rows = self.conn.fetch_all("SELECT DISTINCT run_type FROM task_runs WHERE run_type IS NOT NULL ORDER BY run_type")
        return [r[0] for r in rows]

    def get_all_categories(self) -> List[str]:
        # Alias for backward compatibility if needed, or use get_categories
        return self.get_categories()

    def get_categories(self) -> List[str]:
        rows = self.conn.fetch_all("SELECT name FROM obs_space_categories ORDER BY name")
        return [r[0] for r in rows]

    def get_obs_spaces_for_category(self, category: str) -> List[str]:
        sql = "SELECT s.name FROM obs_spaces s JOIN obs_space_categories c ON s.category_id = c.id WHERE c.name = ? ORDER BY s.name"
        return [r[0] for r in self.conn.fetch_all(sql, (category,))]

    def get_all_task_names(self, run_type: str) -> List[str]:
        rows = self.conn.fetch_all("SELECT DISTINCT t.name FROM task_runs tr JOIN tasks t ON tr.task_id = t.id WHERE tr.run_type = ?", (run_type,))
        return [r[0] for r in rows]

    # --- INVENTORY MATRIX ---
    
    def get_compressed_inventory(self, run_type_filter: str = None, limit: int = 100):
        """
        Returns inventory matrix with identical 'OK' rows collapsed.
        Matches call in website_generator.py
        """
        run_type = run_type_filter 
        sql = """
            SELECT tr.date, tr.cycle, t.name as task, tr.status, tr.run_type
            FROM task_runs tr JOIN tasks t ON tr.task_id = t.id
        """
        params = []
        if run_type:
            sql += " WHERE tr.run_type = ?"
            params.append(run_type)
        
        sql += " ORDER BY tr.date DESC, tr.cycle DESC"
        
        rows = self.conn.fetch_all(sql, tuple(params))
        
        cycles = defaultdict(dict)
        ordered_keys = []
        for r in rows:
            key = (r['date'], r['cycle'], r['run_type']) 
            if key not in cycles: ordered_keys.append(key)
            cycles[key][r['task']] = r['status']

        compressed = []
        current_group = []
        
        for key in ordered_keys:
            tasks = cycles[key]
            is_ok = tasks and all(s == 'SUCCEEDED' for s in tasks.values())
            
            if is_ok:
                if current_group and set(cycles[current_group[-1]].keys()) == set(tasks.keys()):
                    current_group.append(key)
                else:
                    self._flush(compressed, current_group, cycles)
                    current_group = [key]
            else:
                self._flush(compressed, current_group, cycles)
                current_group = []
                compressed.append({'type': 'single', 'date': key[0], 'cycle': key[1], 'run_type': key[2], 'tasks': tasks})
                
        self._flush(compressed, current_group, cycles)
        return compressed[:limit]

    def get_inventory_matrix(self, *args, **kwargs):
        """Alias for CLI compatibility."""
        return self.get_compressed_inventory(*args, **kwargs)

    def _flush(self, result, group, data):
        if not group: return
        if len(group) < 3:
            for k in group: result.append({'type': 'single', 'date': k[0], 'cycle': k[1], 'run_type': k[2], 'tasks': data[k]})
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

    # --- PLOTTING / TIME SERIES ---

    # RENAMED from get_task_timing to match website_generator call
    def get_task_timing_series(self, run_type: str, task: str, days: int = 90):
        sql = """
            SELECT tr.date, tr.cycle, tr.runtime_sec 
            FROM task_runs tr JOIN tasks t ON tr.task_id = t.id 
            WHERE tr.run_type = ? AND t.name = ? AND tr.runtime_sec > 0
            AND tr.date >= strftime('%Y%m%d', date('now', ?)) ORDER BY tr.date, tr.cycle
        """
        return [dict(r) for r in self.conn.fetch_all(sql, (run_type, task, f"-{days} days"))]

    def get_task_timings(self, days=None, task_name=None, run_type=None):
        """CLI Helper."""
        if task_name and run_type:
            series = self.get_task_timing_series(run_type, task_name, days or 30)
            return [{'date': r['date'], 'cycle': r['cycle'], 'task': task_name, 'duration': r['runtime_sec']} for r in series]
        return []

    def get_category_obs_sums(self, run_type: str, category: str, days: int = 90):
        sql = """
            SELECT tr.date, tr.cycle, SUM(fi.obs_count) as total_obs
            FROM file_inventory fi JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            JOIN obs_space_categories c ON os.category_id = c.id
            WHERE tr.run_type = ? AND c.name = ? 
            AND tr.date >= strftime('%Y%m%d', date('now', ?))
            GROUP BY tr.date, tr.cycle ORDER BY tr.date, tr.cycle
        """
        rows = self.conn.fetch_all(sql, (run_type, category, f"-{days} days"))
        return [dict(r) for r in rows]

    def get_obs_counts_by_category(self, category, days, run_type):
        """CLI Adapter."""
        data = self.get_category_obs_sums(run_type, category, days or 30)
        return [{'date': r['date'], 'cycle': r['cycle'], 'count': r['total_obs']} for r in data]

    def get_obs_space_counts(self, run_type: str, obs_space: str, days: int = 30):
        sql = """
            SELECT tr.date, tr.cycle, SUM(fi.obs_count) as count
            FROM file_inventory fi
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            WHERE tr.run_type = ? AND os.name = ?
              AND tr.date >= strftime('%Y%m%d', date('now', ?))
            GROUP BY tr.date, tr.cycle
            ORDER BY tr.date, tr.cycle
        """
        return [dict(r) for r in self.conn.fetch_all(sql, (run_type, obs_space, f"-{days} days"))]

    def get_obs_counts_by_space(self, space, days, run_type):
        """CLI Adapter."""
        data = self.get_obs_space_counts(run_type, space, days or 30)
        return [{'date': r['date'], 'cycle': r['cycle'], 'count': r['count']} for r in data]

    def get_variable_physics_series(self, run_type: str, space: str, var: str, days: int = 90):
        sql = """
            SELECT tr.date, tr.cycle, s.mean_val, s.std_dev
            FROM file_variable_statistics s JOIN file_inventory fi ON s.file_id = fi.id
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            JOIN variables v ON s.variable_id = v.id
            WHERE tr.run_type = ? AND os.name = ? AND v.name = ?
            AND tr.date >= strftime('%Y%m%d', date('now', ?))
            ORDER BY tr.date, tr.cycle
        """
        return [dict(r) for r in self.conn.fetch_all(sql, (run_type, space, var, f"-{days} days"))]

    def get_obs_space_schema(self, space):
        sql = "SELECT c.group_name, v.name FROM obs_space_content c JOIN variables v ON c.variable_id = v.id JOIN obs_spaces s ON c.obs_space_id = s.id WHERE s.name = ?"
        return [dict(r) for r in self.conn.fetch_all(sql, (space,))]

    def get_obs_totals(self, days=7, run_type=None):
        sql = """
            SELECT os.name, SUM(fi.obs_count) as total 
            FROM file_inventory fi JOIN obs_spaces os ON fi.obs_space_id = os.id 
            JOIN task_runs tr ON fi.task_run_id = tr.id 
            WHERE tr.date >= strftime('%Y%m%d', date('now', ?))
        """
        params = [f"-{days} days"]
        if run_type:
            sql += " AND tr.run_type = ?"
            params.append(run_type)
        sql += " GROUP BY os.name ORDER BY total DESC LIMIT 20"
        return [(r['name'], r['total']) for r in self.conn.fetch_all(sql, tuple(params))]
