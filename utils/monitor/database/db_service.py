import sqlite3
import logging
from typing import List, Any, Optional, Tuple

from .schema import MonitorSchema


class DBDataService:
    """
    Base Data Access Layer.
    
    Responsibilities:
    1. Manage SQLite connection and lifecycle.
    2. Provide generic execute/fetch helper methods.
    3. GUARANTEE schema existence on initialization.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        
        # Return results as dictionary-like objects
        self.conn.row_factory = sqlite3.Row
        
        # AUTOMATIC SCHEMA INITIALIZATION
        # This ensures tables exist before any service tries to read/write.
        MonitorSchema(self.conn)

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()

    def execute(self, query: str, params: Tuple = ()) -> int:
        """
        Executes a write operation.
        Returns the number of affected rows (for UPDATE/DELETE) 
        or lastrowid (for INSERT).
        """
        cur = self.conn.cursor()
        cur.execute(query, params)
        
        # If it was an UPDATE/DELETE, rowcount tells us how many rows matched.
        # If it was an INSERT, lastrowid tells us the new ID.
        # Returning rowcount is standard for 'was this successful' checks.
        if query.strip().upper().startswith("UPDATE") or query.strip().upper().startswith("DELETE"):
            return cur.rowcount
        
        return cur.lastrowid

    def fetch_all(self, query: str, params: Tuple = ()) -> List[sqlite3.Row]:
        """Executes a read operation and returns all rows."""
        cur = self.conn.cursor()
        cur.execute(query, params)
        return cur.fetchall()

    def fetch_one(self, query: str, params: Tuple = ()) -> Optional[sqlite3.Row]:
        """Executes a read operation and returns a single row or None."""
        cur = self.conn.cursor()
        cur.execute(query, params)
        return cur.fetchone()

    def commit(self):
        """Manually commits the current transaction."""
        self.conn.commit()

    def __enter__(self):
        """Context Manager support."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Auto-close on exit."""
        self.close()




    '''
    def update_file_status(self, file_id, status, error_msg=None, obs_count=None):
        """Universal status updater."""
        if obs_count is not None:
            sql = "UPDATE file_inventory SET integrity_status = ?, error_message = ?, obs_count = ? WHERE id = ?"
            params = (status, error_msg, obs_count, file_id)
        else:
            sql = "UPDATE file_inventory SET integrity_status = ?, error_message = ? WHERE id = ?"
            params = (status, error_msg, file_id)
        
        self.execute(sql, params)
    '''

    # def get_files_by_status(self, 
                           # statuses: Union[str, List[str]], 
                           # columns: List[str] = None) -> List[Dict[str, Any]]:
        # """
        # Universal fetcher.
        # Defaults to columns: ['id', 'file_path', 'obs_space_id']
        # """
        # if isinstance(statuses, str):
            # statuses = [statuses]
        # 
        # # Default columns match your original get_pending_files
        # cols = columns or ['id', 'file_path', 'obs_space_id']
        # cols_str = ", ".join(cols)
        # 
        # placeholders = ', '.join(['?'] * len(statuses))
        # 
        # sql = f"""
            # SELECT {cols_str}
            # FROM file_inventory
            # WHERE integrity_status IN ({placeholders})
        # """
        # 
        # return self.fetch_all(sql, tuple(statuses))
