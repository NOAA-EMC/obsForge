import sqlite3
import logging
from typing import Dict, Any

logger = logging.getLogger("ScannerPersistence")

class ScannerStateReader:
    """
    Reads the current state of the File Inventory from the database.
    
    COMPATIBILITY MODE:
    Returns a dictionary of dictionaries to match the existing InventoryScanner logic.
    Structure: { '/path/to/file': {'mtime': 1735689000} }
    """
    
    def __init__(self, db_path: str):
        self.db_path = db_path

    def get_known_state(self) -> Dict[str, Dict[str, Any]]:
        """
        Retrieves tracked files and their metadata.
        """
        known_files = {}
        conn = None
        
        try:
            conn = sqlite3.connect(self.db_path)
            cur = conn.cursor()
            
            # Select columns needed for the scanner's Gatekeeper logic
            sql = "SELECT file_path, file_modified_time FROM file_inventory"
            
            try:
                cur.execute(sql)
                rows = cur.fetchall()
            except sqlite3.OperationalError:
                logger.warning("Inventory table not found. Assuming empty state.")
                return {}
            
            for row in rows:
                path = row[0]
                mtime = row[1]
                
                # The existing scanner expects a dictionary, not a raw int
                known_files[path] = {
                    'mtime': mtime if mtime is not None else 0
                }
            
            logger.info(f"Loaded inventory state: {len(known_files)} existing files.")
            
        except Exception as e:
            logger.error(f"Failed to load scanner state: {e}")
            return {}
            
        finally:
            if conn:
                conn.close()
                
        return known_files
