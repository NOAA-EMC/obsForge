import sqlite3
from datetime import datetime, timedelta
import sys
import os
from wxflow.sqlitedb import SQLiteDB


class BaseDatabase(SQLiteDB):
    """Base class for managing different types of file-based databases."""

    def __init__(self, db_name: str, base_dir: str) -> None:
        """
        Initialize the database.

        :param db_name: Name of the SQLite database.
        :param base_dir: Directory containing observation files.
        """
        super().__init__(db_name)
        self.base_dir = base_dir
        self.create_database()

    def create_database(self):
        """Create the SQLite database. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement create_database method")

    def get_connection(self):
        """Return the database connection."""
        return self.connection

    def parse_filename(self):
        """Parse a filename and extract relevant metadata. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement parse_filename method")

    def ingest_files(self):
        """Scan the directory for new observation files and insert them into the database."""
        raise NotImplementedError("Subclasses must implement ingest_files method")

    def insert_record(self, query: str, params: tuple) -> None:
        """Insert a record into the database."""
        self.connect()
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            self.connection.commit()
        except sqlite3.IntegrityError:
            pass  # Skip duplicates
        finally:
            self.disconnect()

    def execute_query(self, query: str, params: tuple = None) -> list:
        """Execute a query and return the results."""
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(query, params or [])
        results = cursor.fetchall()
        self.disconnect()
        return results

    def get_valid_files(self,
                        da_cycle: str,
                        window_hours: int = 3,
                        instrument: str = None,
                        satellite: str = None,
                        obs_type: str = None,
                        cutoff_delta: int = 0) -> list:
        """
        Retrieve a list of observation files within a DA window, possibly filtered by instrument,
        satellite, observation type, and cutoff delta (known latency to emulate the early cycle if needed).
        """
        da_time = datetime.strptime(da_cycle, "%Y%m%d%H%M%S")
        window = timedelta(hours=window_hours)
        cutoff_delta = timedelta(hours=cutoff_delta)
        window_begin = da_time - window
        window_end = da_time + window - cutoff_delta

        query = """
        SELECT filename FROM obs_files
        WHERE obs_time BETWEEN ? AND ?
        """
        params = [window_begin, window_end]

        if instrument:
            query += " AND instrument = ?"
            params.append(instrument)
        if satellite:
            query += " AND satellite = ?"
            params.append(satellite)
        if obs_type:
            query += " AND obs_type = ?"
            params.append(obs_type)

        results = self.execute_query(query, tuple(params))
        valid_files = []
        for row in results:
            valid_files.append(row[0])

        return valid_files
