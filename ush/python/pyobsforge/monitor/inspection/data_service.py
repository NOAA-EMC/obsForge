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
        Only considers files that physically exist (integrity_status='OK').
        """
        sql = """
            SELECT 
                fi.id, fi.file_path, fi.obs_count, 
                os.name as obs_space, tr.run_type
            FROM file_inventory fi
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            WHERE tr.date >= strftime('%Y%m%d', date('now', ?))
              AND fi.integrity_status = 'OK'
        """
        # Returns list of dicts
        return [dict(r) for r in self.conn.fetch_all(sql, (f"-{days} days",))]

    def get_baseline_stats(self, obs_space_name, run_type):
        """
        Calculates the 30-day average observation count for a specific stream.
        """
        sql = """
            SELECT 
                AVG(fi.obs_count) as avg_count, 
                CAST(AVG(fi.obs_count) * 0.5 as INT) as threshold
            FROM file_inventory fi
            JOIN task_runs tr ON fi.task_run_id = tr.id
            JOIN obs_spaces os ON fi.obs_space_id = os.id
            WHERE os.name = ? AND tr.run_type = ?
              AND tr.date >= strftime('%Y%m%d', date('now', '-30 days'))
        """
        # UPDATED CALL: fetch_one
        row = self.conn.fetch_one(sql, (obs_space_name, run_type))
        return row if row else {'avg_count': 0, 'threshold': 0}
