from logging import getLogger
import sqlite3
from datetime import datetime, timedelta
from wxflow.sqlitedb import SQLiteDB
from wxflow import FileHandler
from os.path import basename, join

logger = getLogger(__name__.split('.')[-1])


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

    def parse_filename(self, filename):
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
                        window_begin: datetime,
                        window_end: datetime,
                        dst_dir: str,
                        satellite: str = None,
                        instrument: str = None,
                        obs_type: str = None,
                        check_receipt: str = "none") -> list:
        """
        Retrieve and copy to dst_dir a list of valid observation files within a specified time window.
        Optionally filter by satellite, instrument, obs_type, and receipt time.
        """
        query = """
        SELECT filename FROM obs_files
        WHERE obs_time BETWEEN ? AND ?
        """
        minutes_behind_realtime = {'gdas': 160, 'gfs': 20}
        params = [window_begin, window_end]

        # Optionally filter by satellite if provided
        if satellite:
            query += " AND satellite = ?"
            params.append(satellite)

        # Optionally filter by instrument if available and provided
        if instrument:
            query += " AND instrument = ?"
            params.append(instrument)

        # Optionally filter by obs_type if available and provided
        if obs_type:
            query += " AND obs_type = ?"
            params.append(obs_type)

        # Execute query to get relevant files
        results = self.execute_query(query, tuple(params))
        valid_files = []

        for row in results:
            filename = row[0]

            # Optional receipt time filtering based on check_receipt parameter
            if check_receipt in ["gdas", "gfs"]:
                query = "SELECT receipt_time FROM obs_files WHERE filename = ?"
                receipt_time = self.execute_query(query, (filename,))[0][0]
                try:
                    receipt_time = datetime.strptime(receipt_time, "%Y-%m-%d %H:%M:%S.%f")
                except ValueError:
                    receipt_time = datetime.strptime(receipt_time, "%Y-%m-%d %H:%M:%S")  # Try parsing without microseconds if it fails

                # Filter based on receipt time threshold
                if receipt_time <= window_end - timedelta(minutes=minutes_behind_realtime[check_receipt]):
                    continue

            valid_files.append(filename)

        # Copy valid files to the destination directory
        dst_files = []
        if len(valid_files) > 0:
            src_dst_obs_list = []  # List of [src_file, dst_file]
            for src_file in valid_files:
                dst_file = join(dst_dir, f"{basename(src_file)}")
                dst_files.append(dst_file)
                src_dst_obs_list.append([src_file, dst_file])
            
            # Ensure the destination directory exists
            FileHandler({'mkdir': [dst_dir]}).sync()
            FileHandler({'copy': src_dst_obs_list}).sync()

        return dst_files

