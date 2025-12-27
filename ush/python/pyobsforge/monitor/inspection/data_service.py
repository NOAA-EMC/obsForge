import json
from pyobsforge.monitor.database.connection import DBConnection

class InspectionDataService:
    """
    Data Access Layer for the Inventory Inspector.
    """
    def __init__(self, db_path: str):
        self.conn = DBConnection(db_path)

    def get_recent_files(self, days=1):
        """
        Fetches files from recent cycles to inspect.
        Includes DOMAIN info (lat/lon/time) for validation.
        """
        sql = """
            SELECT 
                fi.id, fi.file_path, fi.obs_count, fi.properties,
                os.name as obs_space, tr.run_type, tr.date, tr.cycle,
                fd.min_lat, fd.max_lat, fd.min_lon, fd.max_lon,
                fd.start_time, fd.end_time
            FROM file_inventory fi
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            LEFT JOIN file_domains fd ON fi.id = fd.file_id
            WHERE tr.date >= strftime('%Y%m%d', date('now', ?))
              AND fi.integrity_status = 'OK'
        """
        rows = self.conn.fetch_all(sql, (f"-{days} days",))
        
        # Parse JSON properties
        results = []
        for r in rows:
            d = dict(r)
            if d['properties'] and isinstance(d['properties'], str):
                try:
                    d['properties'] = json.loads(d['properties'])
                except:
                    d['properties'] = {}
            results.append(d)
        return results

    def get_baseline_stats(self, obs_space_name, run_type):
        """Calculates 30-day average volume threshold."""
        sql = """
            SELECT 
                CAST(AVG(fi.obs_count) * 0.5 as INT) as threshold
            FROM file_inventory fi
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            WHERE os.name = ? AND tr.run_type = ?
              AND tr.date >= strftime('%Y%m%d', date('now', '-30 days'))
        """
        row = self.conn.fetch_one(sql, (obs_space_name, run_type))
        return row['threshold'] if row and row['threshold'] else 0

    def get_file_stats(self, file_id):
        """
        Fetches physical variable statistics (Min/Max).
        Used by Physics checks (Vertical Integrity, SST limits).
        """
        sql = """
            SELECT v.name, s.min_val, s.max_val 
            FROM file_variable_statistics s
            JOIN variables v ON s.variable_id = v.id
            WHERE s.file_id = ?
        """
        rows = self.conn.fetch_all(sql, (file_id,))
        
        stats = {}
        for r in rows:
            # Map full name "Group/Variable"
            stats[r['name']] = {'min': r['min_val'], 'max': r['max_val']}
            
            # Also map short name "Variable" for easier rule writing
            if '/' in r['name']:
                short_name = r['name'].split('/')[-1]
                if short_name not in stats:
                     stats[short_name] = {'min': r['min_val'], 'max': r['max_val']}
                     
        return stats
