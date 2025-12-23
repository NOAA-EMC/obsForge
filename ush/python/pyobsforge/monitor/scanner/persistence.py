from typing import Dict
from pyobsforge.monitor.database.connection import DBConnection

class ScannerStateReader:
    """
    Provides the InventoryScanner with the current database state
    to enable incremental scanning.
    """
    def __init__(self, db_path: str):
        self.conn = DBConnection(db_path)

    def get_known_mtimes(self) -> Dict[str, int]:
        """Returns {file_path: mtime} for all currently indexed files."""
        sql = "SELECT file_path, file_modified_time FROM file_inventory WHERE file_modified_time IS NOT NULL"
        rows = self.conn.fetch_all(sql)
        return {r['file_path']: r['file_modified_time'] for r in rows}
