import sqlite3
import os
from contextlib import contextmanager

class DBConnection:
    """
    Shared factory for SQLite connections. 
    Enforces Read-Only mode by default to prevent locking issues.
    """
    def __init__(self, db_path: str):
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database not found: {db_path}")
        self.db_path = db_path

    @contextmanager
    def get_cursor(self):
        # Open in Read-Only URI mode
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            yield conn.cursor()
        finally:
            conn.close()

    def fetch_all(self, sql: str, params: tuple = ()):
        """Returns a list of Row objects."""
        with self.get_cursor() as cur:
            return cur.execute(sql, params).fetchall()

    def fetch_one(self, sql: str, params: tuple = ()):
        """Returns a single Row object or None."""
        with self.get_cursor() as cur:
            return cur.execute(sql, params).fetchone()
