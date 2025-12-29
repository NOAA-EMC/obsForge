from collections import defaultdict
import math
from typing import List, Dict, Any
from pyobsforge.monitor.database.connection import DBConnection

class ReportDataService:
    """
    Data Access Layer for the Website Generator and CLI Reports.
    
    Responsibilities:
    1. Metadata: Fetch run types, categories, tasks.
    2. Statistics: Aggregates obs counts, runtimes, and physics stats.
    3. Inventory: Generates the status matrix.
    4. Domains: Fetches spatio-temporal and vertical bounds.
    5. Anomalies: Retrieves flagged files.
    """
    
    def __init__(self, db_path: str):
        self.conn = DBConnection(db_path)

    # ==========================================================================
    # 1. METADATA & UTILS
    # ==========================================================================
    
    def fetch_table_names(self) -> List[str]:
        """Returns a list of all tables in the database."""
        sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        rows = self.conn.fetch_all(sql)
        return [r[0] for r in rows]

    def get_table_schema(self, table_name: str) -> List[str]:
        """Returns column names for a given table."""
        try:
            rows = self.conn.fetch_all(f"PRAGMA table_info({table_name})")
            return [r['name'] for r in rows]
        except Exception:
            return []

    def get_raw_table_rows(self, table_name: str, limit: int = None, filter_sql: str = None) -> List[tuple]:
        """Fetches raw rows for CLI inspection."""
        sql = f"SELECT * FROM {table_name}"
        if filter_sql:
            sql += f" WHERE {filter_sql}"
        if limit:
            sql += f" LIMIT {limit}"
        try:
            with self.conn.get_cursor() as cur:
                return [tuple(r) for r in cur.execute(sql).fetchall()]
        except Exception:
            return []

    def get_cycle_ranges(self):
        """Returns map of run_type -> list of cycles."""
        try:
            sql = "SELECT DISTINCT run_type FROM task_runs"
            rows = self.conn.fetch_all(sql)
            return {r['run_type']: [] for r in rows} 
        except Exception:
            return {}

    # ==========================================================================
    # 2. SCHEMA & STATS (For detailed inspection)
    # ==========================================================================

    def get_obs_space_schema_details(self, obs_space_name: str):
        """Returns the schema (variables, groups, types) for a named Obs Space."""
        sql = """
            SELECT c.group_name, v.name as var_name, v.data_type, v.dimensionality
            FROM obs_space_content c
            JOIN variables v ON c.variable_id = v.id
            JOIN obs_spaces os ON c.obs_space_id = os.id
            WHERE os.name = ?
            ORDER BY c.group_name, v.name
        """
        return [dict(r) for r in self.conn.fetch_all(sql, (obs_space_name,))]

    def get_file_statistics(self, file_path_pattern: str):
        """Returns stats for files matching a pattern."""
        sql = """
            SELECT f.file_path, c.group_name, v.name as variable,
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

    def get_obs_space_schema(self, space):
        """Returns variable list for dropdowns."""
        sql = """
            SELECT c.group_name, v.name 
            FROM obs_space_content c 
            JOIN variables v ON c.variable_id = v.id 
            JOIN obs_spaces s ON c.obs_space_id = s.id 
            WHERE s.name = ?
        """
        return [dict(r) for r in self.conn.fetch_all(sql, (space,))]

    # ==========================================================================
    # 3. DASHBOARD METADATA & ANOMALIES
    # ==========================================================================

    def get_all_run_types(self) -> List[str]:
        sql = """
            SELECT DISTINCT run_type FROM task_runs 
            WHERE run_type IS NOT NULL ORDER BY run_type
        """
        rows = self.conn.fetch_all(sql)
        return [r[0] for r in rows]

    def get_all_categories(self) -> List[str]:
        sql = "SELECT name FROM obs_space_categories ORDER BY name"
        rows = self.conn.fetch_all(sql)
        return [r[0] for r in rows]

    def get_obs_spaces_for_category(self, category: str) -> List[str]:
        sql = """
            SELECT s.name FROM obs_spaces s 
            JOIN obs_space_categories c ON s.category_id = c.id 
            WHERE c.name = ? ORDER BY s.name
        """
        return [r[0] for r in self.conn.fetch_all(sql, (category,))]

    def get_all_task_names(self, run_type: str) -> List[str]:
        sql = """
            SELECT DISTINCT t.name FROM task_runs tr 
            JOIN tasks t ON tr.task_id = t.id 
            WHERE tr.run_type = ?
        """
        rows = self.conn.fetch_all(sql, (run_type,))
        return [r[0] for r in rows]
        
    def get_flagged_files(self, run_type: str):
        """Returns details for all files with non-OK status (Anomalies)."""
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
        return [dict(r) for r in self.conn.fetch_all(sql, (run_type,))]
        
    def get_obs_space_domains(self, run_type, space):
        """
        Fetches Lat/Lon bounds AND Depth/Pressure bounds for the LATEST available cycle.
        Uses explicit ordering to find the last update for THIS sensor.
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
        res = self.conn.fetch_one(sql_domain, (run_type, space))
        domain = dict(res) if res else {}

        if not domain: return {}

        # 2. Get Vertical Bounds (Depth/Pressure from statistics table)
        # Must match the EXACT same cycle we found above
        latest_date = domain['date']
        latest_cycle = domain['cycle']

        sql_vert = """
            SELECT v.name, MIN(s.min_val) as min_v, MAX(s.max_val) as max_v
            FROM file_variable_statistics s
            JOIN variables v ON s.variable_id = v.id
            JOIN file_inventory fi ON s.file_id = fi.id
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            WHERE tr.run_type = ? AND os.name = ?
            AND (v.name = 'depth' OR v.name = 'pressure' OR v.name = 'air_pressure')
            AND tr.date = ? AND tr.cycle = ?
            GROUP BY v.name
        """
        vert_rows = self.conn.fetch_all(sql_vert, (run_type, space, latest_date, latest_cycle))
        
        for r in vert_rows:
            # Map to readable keys like 'depth_min', 'pressure_max'
            domain[f"{r['name']}_min"] = r['min_v']
            domain[f"{r['name']}_max"] = r['max_v']
            
        return domain

    # ==========================================================================
    # 4. INVENTORY MATRIX
    # ==========================================================================
    
    def get_compressed_inventory(self, run_type_filter: str = None, limit: int = None):
        """
        Returns inventory history. limit=None means ALL history.
        Collapses rows only if tasks are OK and no files are flagged.
        """
        sql_tasks = """
            SELECT tr.id as run_id, tr.date, tr.cycle, t.name as task, tr.status, tr.run_type
            FROM task_runs tr JOIN tasks t ON tr.task_id = t.id
        """
        params = []
        if run_type_filter:
            sql_tasks += " WHERE tr.run_type = ?"
            params.append(run_type_filter)
        
        sql_tasks += " ORDER BY tr.date DESC, tr.cycle DESC"
        if limit:
            sql_tasks += f" LIMIT {limit}"
        
        rows = self.conn.fetch_all(sql_tasks, tuple(params))
        
        # Identify bad runs (Integrity Check)
        bad_run_ids = set(r[0] for r in self.conn.fetch_all("SELECT task_run_id FROM file_inventory WHERE integrity_status != 'OK'"))

        cycles = defaultdict(dict)
        ordered_keys = []
        
        for r in rows:
            key = (r['date'], r['cycle'], r['run_type'])
            if key not in cycles:
                ordered_keys.append(key)
            
            status = r['status']
            if r['run_id'] in bad_run_ids:
                status = 'WARNING'
            cycles[key][r['task']] = status

        compressed = []
        current_group = []
        
        for key in ordered_keys:
            tasks = cycles[key]
            # Strict Collapse Rule: Must be SUCCEEDED and Clean
            is_perfect = tasks and all(s == 'SUCCEEDED' for s in tasks.values())
            
            if is_perfect:
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
        return compressed

    def get_inventory_matrix(self, *args, **kwargs):
        """CLI Alias."""
        return self.get_compressed_inventory(*args, **kwargs)

    def _flush(self, result, group, data):
        """Helper to write a group (or singles) to the result list."""
        if not group: return
        if len(group) < 3:
            for k in group:
                result.append({'type': 'single', 'date': k[0], 'cycle': k[1], 'run_type': k[2], 'tasks': data[k]})
        else:
            s, e = group[0], group[-1]
            result.append({'type': 'group', 'start_date': s[0], 'start_cycle': s[1], 'end_date': e[0], 'end_cycle': e[1], 'run_type': s[2], 'count': len(group), 'tasks': data[s]})

    # ==========================================================================
    # 5. TIME SERIES & PLOTS
    # ==========================================================================

    def get_task_timing_series(self, run_type: str, task: str, days: int = None):
        """Returns aggregated runtime stats (Mean & StdDev)."""
        sql = """
            SELECT tr.date, tr.cycle, AVG(tr.runtime_sec) as mean_runtime, AVG(tr.runtime_sec * tr.runtime_sec) as mean_sq_runtime
            FROM task_runs tr JOIN tasks t ON tr.task_id = t.id 
            WHERE tr.run_type = ? AND t.name = ? AND tr.runtime_sec > 0
        """
        params = [run_type, task]
        if days:
            sql += " AND tr.date >= strftime('%Y%m%d', date('now', ?))"
            params.append(f"-{days} days")
            
        sql += " GROUP BY tr.date, tr.cycle ORDER BY tr.date, tr.cycle"
        
        rows = self.conn.fetch_all(sql, tuple(params))
        results = []
        for r in rows:
            var = r['mean_sq_runtime'] - (r['mean_runtime'] ** 2)
            results.append({
                'date': r['date'], 'cycle': r['cycle'], 
                'runtime_sec': r['mean_runtime'], 
                'std_dev': math.sqrt(var) if var > 0 else 0
            })
        return results

    def get_task_timings(self, days=None, task_name=None, run_type=None):
        """CLI Adapter."""
        if task_name and run_type:
            series = self.get_task_timing_series(run_type, task_name, days)
            return [{'date': r['date'], 'cycle': r['cycle'], 'task': task_name, 'duration': r['runtime_sec']} for r in series]
        return []

    def get_category_counts(self, run_type: str, category: str, days: int = None):
        """
        Calculates Total Obs, Mean per File, and StdDev per File.
        Used for 'Band Plots' (Mean line with +/- Sigma shading).
        """
        sql = """
            SELECT 
                tr.date, tr.cycle, 
                SUM(fi.obs_count) as total_obs,
                AVG(fi.obs_count) as mean_obs,
                AVG(fi.obs_count * fi.obs_count) as mean_sq_obs
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
        
        rows = self.conn.fetch_all(sql, tuple(params))
        results = []
        for r in rows:
            var = r['mean_sq_obs'] - (r['mean_obs'] ** 2)
            results.append({
                'date': r['date'], 'cycle': r['cycle'], 
                'total_obs': r['total_obs'], 
                'file_mean': r['mean_obs'], 
                'file_std': math.sqrt(var) if var > 0 else 0
            })
        return results

    def get_category_obs_sums(self, *args, **kwargs):
        return self.get_category_counts(*args, **kwargs)

    def get_obs_counts_by_category(self, category, days, run_type):
        """CLI Adapter."""
        data = self.get_category_counts(run_type, category, days)
        return [{'date': r['date'], 'cycle': r['cycle'], 'count': r['total_obs']} for r in data]

    def get_obs_space_counts(self, run_type: str, obs_space: str, days: int = None):
        """
        UPDATED: Calculates Mean/StdDev for specific Obs Space (Band Plot Ready).
        """
        sql = """
            SELECT 
                tr.date, tr.cycle, 
                SUM(fi.obs_count) as total_obs,
                AVG(fi.obs_count) as mean_obs,
                AVG(fi.obs_count * fi.obs_count) as mean_sq_obs
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
        
        rows = self.conn.fetch_all(sql, tuple(params))
        results = []
        for r in rows:
            var = r['mean_sq_obs'] - (r['mean_obs'] ** 2)
            results.append({
                'date': r['date'], 'cycle': r['cycle'], 
                'count': r['total_obs'], 
                'file_mean': r['mean_obs'], 
                'file_std': math.sqrt(var) if var > 0 else 0
            })
        return results

    def get_obs_counts_by_space(self, space, days, run_type):
        """CLI Adapter."""
        data = self.get_obs_space_counts(run_type, space, days)
        return [{'date': r['date'], 'cycle': r['cycle'], 'count': r['count']} for r in data]

    def get_variable_physics_series(self, run_type: str, space: str, var: str, days: int = None):
        """
        UPDATED: Aggregates multiple files per cycle into a single Mean/Std point.
        This prevents 'messy' plots where multiple points exist for one timestamp.
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
        
        return [dict(date=r['date'], cycle=r['cycle'], mean_val=r['avg_mean'], std_dev=r['avg_std']) for r in self.conn.fetch_all(sql, tuple(params))]

    def get_physics_series(self, *args, **kwargs):
        return self.get_variable_physics_series(*args, **kwargs)

    def get_obs_totals(self, days=7, run_type=None):
        """CLI: Returns aggregated totals."""
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
        return [(r['name'], r['total']) for r in self.conn.fetch_all(sql, tuple(params))]
