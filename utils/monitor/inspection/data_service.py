import sqlite3
import json
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger("InspectionData")

class InspectionDataService:
    """
    Data Access Layer for the Inventory Inspector.
    
    Provides read-only access to file metadata, history, and statistics
    to support anomaly detection rules.
    """
    
    def __init__(self, db_path: str):
        # Direct SQLite connection
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

    def fetch_one(self, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        """Helper to execute query and return a single row."""
        try:
            cur = self.conn.cursor()
            cur.execute(sql, params)
            return cur.fetchone()
        except sqlite3.Error as e:
            logger.error(f"SQL Error: {e}")
            return None

    def get_recent_files(self, days: int = 1) -> List[Dict[str, Any]]:
        """
        Fetches files from recent cycles to inspect.
        
        Filter Logic:
        - Only fetches files currently marked as 'OK'.
        - This prevents re-inspecting files already flagged as Anomalies.
        - Includes DOMAIN info (lat/lon/time) for spatial validation.
        """
        sql = """
            SELECT 
                fi.id, 
                fi.file_path, 
                fi.obs_count, 
                fi.properties,
                os.name as obs_space, 
                tr.run_type, 
                tr.date, 
                tr.cycle,
                fd.min_lat, fd.max_lat, 
                fd.min_lon, fd.max_lon,
                fd.start_time, fd.end_time
            FROM file_inventory fi
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            LEFT JOIN file_domains fd ON fi.id = fd.file_id
            WHERE tr.date >= strftime('%Y%m%d', date('now', ?))
              AND fi.integrity_status = 'OK'
        """
        
        rows = self.fetch_all(sql, (f"-{days} days",))
        
        results = []
        for r in rows:
            d = dict(r)
            
            # Robust JSON parsing for file properties (e.g. outlier flags)
            props = d.get('properties')
            if props:
                if isinstance(props, str):
                    try:
                        d['properties'] = json.loads(props)
                    except Exception:
                        d['properties'] = {}
                # else: it's already a dict/object (rare in sqlite result)
            else:
                d['properties'] = {}
                
            results.append(d)
            
        return results

    def get_baseline_stats(self, obs_space_name: str, run_type: str) -> int:
        """
        Calculates the Volume Threshold based on history.
        
        Logic:
        - Looks at the last 30 days of data for this specific Obs Space.
        - Returns 50% of the average observation count.
        - Used to detect "Low Volume" anomalies.
        """
        sql = """
            SELECT 
                CAST(AVG(fi.obs_count) * 0.5 as INT) as threshold
            FROM file_inventory fi
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            WHERE os.name = ? 
              AND tr.run_type = ?
              AND tr.date >= strftime('%Y%m%d', date('now', '-30 days'))
        """
        
        row = self.fetch_one(sql, (obs_space_name, run_type))
        return row['threshold'] if row and row['threshold'] else 0

    def get_file_stats(self, file_id: int) -> List[Dict[str, Any]]:
        """
        Fetches physical statistics for a file AND the validation rules.
        
        Joins:
        - file_variable_statistics (The actual data in the file)
        - variables (The physical limits defined in the DB)
        
        Used by: PhysicalRangeRule
        """
        sql = """
            SELECT 
                v.name, 
                s.min_val as min, 
                s.max_val as max, 
                s.mean_val as mean, 
                s.std_dev,
                v.units,
                v.valid_min,
                v.valid_max,
                v.min_std_dev
            FROM file_variable_statistics s
            JOIN variables v ON s.variable_id = v.id
            WHERE s.file_id = ?
        """
        
        rows = self.fetch_all(sql, (file_id,))
        return [dict(r) for r in rows]
