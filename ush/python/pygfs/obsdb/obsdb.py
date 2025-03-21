import sqlite3
import os
import re
import glob
from datetime import datetime, timedelta


class BaseDatabase:
    """Base class for managing different types of file-based databases."""

    def __init__(self, db_path, base_dir, pattern):
        """
        Initialize the database.

        :param db_path: Path to the SQLite database file.
        :param base_dir: Directory containing observation files.
        :param pattern: Regular expression pattern for extracting metadata.
        """
        self.db_path = db_path
        self.base_dir = base_dir
        self.pattern = pattern
        self.create_database()

    def create_database(self):
        """Create the SQLite database. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement create_database method")

    def parse_filename(self, filename):
        """Parse a filename and extract relevant metadata. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement parse_filename method")

    def insert_record(self, query, params):
        """Insert a record into the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            conn.commit()
        except sqlite3.IntegrityError:
            pass  # Skip duplicates
        finally:
            conn.close()

    def execute_query(self, query, params=None):
        """Execute a query and return the results."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(query, params or [])
        results = cursor.fetchall()
        conn.close()
        return results

    def ingest_files(self):
        """Scan the directory for new observation files and insert them into the database."""
        obs_files = glob.glob(os.path.join(self.base_dir, "*.nc"))
        print(f"Found {len(obs_files)} new files to ingest")
        #print(obs_files)
        for file in obs_files:
            filename = os.path.basename(file)
            parsed_data = self.parse_filename(filename)
            if parsed_data:
                query = """
                    INSERT INTO obs_files (filename, obs_time, instrument, satellite, obs_type)
                    VALUES (?, ?, ?, ?, ?)
                """
                self.insert_record(query, parsed_data)

    def get_valid_files(self, da_cycle, window_hours=3, instrument=None, satellite=None, obs_type=None, cutoff_time=None):
        """Retrieve observation files within a DA window, filtered by instrument, satellite, observation type, and cutoff time."""
        da_time = datetime.strptime(da_cycle, "%Y%m%d%H%M%S")
        window = timedelta(hours=window_hours)

        query = """
        SELECT filename FROM obs_files
        WHERE obs_time BETWEEN ? AND ?
        """
        params = [da_time - window, da_time + window]

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

        if cutoff_time:
            filtered_files = []
            for file in valid_files:
                file_time_str = file.split('-')[0]
                file_time = datetime.strptime(file_time_str, "%Y%m%d%H%M%S")
                if file_time <= cutoff_time:
                    filtered_files.append(file)
            valid_files = filtered_files

        return valid_files
