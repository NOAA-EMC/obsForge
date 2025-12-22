import sqlite3
import json
from typing import List, Dict, Any, Optional
from collections import defaultdict

class DBReader:
    def __init__(self, db_path):
        self.db_path = db_path
        # Open read-only to prevent accidental writes during reporting/learning
        self.conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        self.conn.row_factory = sqlite3.Row

    # ------------------------------------------------------------------
    # 1. VALIDATION SUPPORT (For monitor_validate.py)
    # ------------------------------------------------------------------
    def get_knowledge_base(self) -> Dict[int, Dict[str, str]]:
        """Returns the learned truth: { obs_space_id: { key: value } }"""
        try:
            sql = "SELECT obs_space_id, property_key, property_value FROM obs_space_properties"
            rows = self.conn.execute(sql).fetchall()
            kb = {}
            for r in rows:
                sid = r['obs_space_id']
                if sid not in kb: kb[sid] = {}
                kb[sid][r['property_key']] = r['property_value']
            return kb
        except Exception:
            return {}

    def get_validation_candidates(self):
        """Returns files that are physically readable and have metadata to check."""
        try:
            sql = """
                SELECT id, obs_space_id, integrity_status, metadata, error_message 
                FROM file_inventory 
                WHERE integrity_status IN ('OK', 'BAD_META') AND metadata IS NOT NULL
            """
            return [dict(r) for r in self.conn.execute(sql).fetchall()]
        except Exception:
            return []

    # ------------------------------------------------------------------
    # 2. LEARNING SUPPORT (For learn_properties.py)
    # ------------------------------------------------------------------
    def get_all_obs_spaces_map(self) -> List[Dict]:
        try:
            return [dict(r) for r in self.conn.execute("SELECT id, name FROM obs_spaces").fetchall()]
        except: return []

    def get_metadata_history(self, obs_space_id: int) -> List[str]:
        try:
            sql = "SELECT metadata FROM file_inventory WHERE obs_space_id = ? AND metadata IS NOT NULL"
            return [r['metadata'] for r in self.conn.execute(sql, (obs_space_id,)).fetchall()]
        except: return []

    def get_current_property(self, obs_space_id: int, key: str) -> Optional[str]:
        try:
            sql = "SELECT property_value FROM obs_space_properties WHERE obs_space_id=? AND property_key=?"
            row = self.conn.execute(sql, (obs_space_id, key)).fetchone()
            return row['property_value'] if row else None
        except: return None

    # ------------------------------------------------------------------
    # 3. INTROSPECTION (For CLI 'tables')
    # ------------------------------------------------------------------
    def fetch_table_names(self) -> List[str]:
        cur = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        return [r['name'] for r in cur.fetchall()]

    def get_table_schema(self, table_name: str) -> List[str]:
        try:
            cur = self.conn.execute(f"PRAGMA table_info({table_name})")
            return [r['name'] for r in cur.fetchall()]
        except: return []

    def get_raw_table_rows(self, table_name: str, limit: int = None, filter_sql: str = None) -> List[tuple]:
        sql = f"SELECT * FROM {table_name}"
        if filter_sql: sql += f" WHERE {filter_sql}"
        if limit: sql += f" LIMIT {limit}"
        try:
            return [tuple(r) for r in self.conn.execute(sql).fetchall()]
        except: return []

    # ------------------------------------------------------------------
    # 4. METADATA HELPERS (For Reporting)
    # ------------------------------------------------------------------
    def get_task_list(self) -> List[str]:
        try:
            return [r['name'] for r in self.conn.execute("SELECT name FROM tasks ORDER BY name").fetchall()]
        except: return []

    def get_all_categories(self) -> List[str]:
        try:
            return [r['name'] for r in self.conn.execute("SELECT name FROM obs_space_categories ORDER BY name").fetchall()]
        except: return []

    def get_all_spaces(self) -> List[tuple]:
        try:
            sql = """SELECT s.name as space, c.name as cat 
                     FROM obs_spaces s JOIN obs_space_categories c ON s.category_id = c.id 
                     ORDER BY c.name, s.name"""
            return [(r['space'], r['cat']) for r in self.conn.execute(sql).fetchall()]
        except: return []

    # ------------------------------------------------------------------
    # 5. DASHBOARD & PLOTS
    # ------------------------------------------------------------------
    def get_inventory_matrix(self, run_type_filter=None, limit=50):
        """Returns execution status matrix."""
        sql = """
            SELECT tr.date, tr.cycle, tr.run_type, t.name as task_name, tr.status, tr.job_id, tr.attempt 
            FROM task_runs tr JOIN tasks t ON tr.task_id = t.id
        """
        params = []
        if run_type_filter:
            sql += " WHERE tr.run_type = ?"
            params.append(run_type_filter)
        sql += " ORDER BY tr.date DESC, tr.cycle DESC"
        
        cur = self.conn.execute(sql, params)
        cycle_map = defaultdict(dict)
        seen_cycles = []
        
        for r in cur.fetchall():
            key = (r['date'], r['cycle'], r['run_type'])
            if key not in cycle_map:
                if len(seen_cycles) >= limit: continue 
                seen_cycles.append(key)
            
            db_status = r['status']
            # Map DB Status to UI Status
            if db_status == "SUCCEEDED": status_code = "OK"
            elif db_status == "FAILED": status_code = "FAIL"
            elif db_status == "RUNNING": status_code = "RUN"
            elif db_status == "DEAD": status_code = "DEAD"
            else: status_code = "MIS"
            
            cycle_map[key][r['task_name']] = {"status": status_code, "job_id": r['job_id'], "attempt": r['attempt']}
        
        return [{"date": d, "cycle": c, "run_type": rt, "tasks": cycle_map[(d,c,rt)]} for d,c,rt in seen_cycles]

    def get_task_timings(self, days=None, task_name=None, run_type=None):
        sql = """
            SELECT tr.date, tr.cycle, t.name as task, tr.runtime_sec as duration 
            FROM task_runs tr JOIN tasks t ON tr.task_id = t.id 
            WHERE tr.runtime_sec > 0
        """
        params = []
        if days:
            sql += " AND tr.date >= strftime('%Y%m%d', date('now', ?))"
            params.append(f"-{days} days")
        if task_name:
            sql += " AND t.name = ?"
            params.append(task_name)
        if run_type:
            sql += " AND tr.run_type = ?"
            params.append(run_type)
        sql += " ORDER BY tr.date, tr.cycle"
        
        return [dict(r) for r in self.conn.execute(sql, params)]

    def get_obs_counts_by_space(self, space_name, days=None, run_type=None):
        """Restored: Required for single-space plotting."""
        sql = """
            SELECT tr.date, tr.cycle, SUM(fi.obs_count) as count
            FROM file_inventory fi
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            WHERE os.name = ?
        """
        params = [space_name]
        if days:
            sql += " AND tr.date >= strftime('%Y%m%d', date('now', ?))"
            params.append(f"-{days} days")
        if run_type:
            sql += " AND tr.run_type = ?"
            params.append(run_type)
        sql += " GROUP BY tr.date, tr.cycle ORDER BY tr.date, tr.cycle"
        return [dict(r) for r in self.conn.execute(sql, params)]

    def get_obs_counts_by_category(self, cat_name, days=None, run_type=None):
        sql = """
            SELECT tr.date, tr.cycle, SUM(fi.obs_count) as count
            FROM file_inventory fi
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            JOIN obs_space_categories cat ON os.category_id = cat.id
            WHERE cat.name = ?
        """
        params = [cat_name]
        if days:
            sql += " AND tr.date >= strftime('%Y%m%d', date('now', ?))"
            params.append(f"-{days} days")
        if run_type:
            sql += " AND tr.run_type = ?"
            params.append(run_type)
        sql += " GROUP BY tr.date, tr.cycle ORDER BY tr.date, tr.cycle"
        return [dict(r) for r in self.conn.execute(sql, params)]

    def get_obs_totals(self, days=7, run_type=None):
        """Top active spaces."""
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
        return [(r['name'], r['total']) for r in self.conn.execute(sql, params)]

    def get_cycle_ranges(self):
        try:
            sql = "SELECT DISTINCT run_type FROM task_runs"
            return {r['run_type']: [] for r in self.conn.execute(sql)}
        except: return {}

    def get_files_for_cycle(self, date, cycle, run_type):
        """Drill down details for web pages."""
        sql = """
            SELECT 
                t.name as task,
                fi.file_path, fi.file_type, fi.integrity_status, fi.obs_count,
                fi.file_size_bytes, fi.error_message
            FROM file_inventory fi
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN tasks t ON tr.task_id = t.id
            WHERE tr.date=? AND tr.cycle=? AND tr.run_type=?
            ORDER BY t.name, fi.file_path
        """
        return [dict(r) for r in self.conn.execute(sql, (str(date), int(cycle), run_type))]
