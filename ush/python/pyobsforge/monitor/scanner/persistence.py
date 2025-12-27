from typing import Dict, Any
from pyobsforge.monitor.database.connection import DBConnection

class ScannerStateReader:
    """
    Provides the InventoryScanner with the current database state
    to enable incremental scanning.
    """
    def __init__(self, db_path: str):
        self.conn = DBConnection(db_path)

    def get_known_state(self) -> Dict[str, Dict[str, Any]]:
        """
        Returns map of file_path -> {mtime, obs_count, integrity}
        Used to preserve metadata for unchanged files.
        """
        try:
            sql = "SELECT file_path, file_modified_time, obs_count, integrity_status FROM file_inventory WHERE file_modified_time IS NOT NULL"
            rows = self.conn.fetch_all(sql)
            state = {}
            for r in rows:
                state[r['file_path']] = {
                    'mtime': r['file_modified_time'],
                    'obs_count': r['obs_count'],
                    'integrity': r['integrity_status']
                }
            return state
        except:
            return {}
