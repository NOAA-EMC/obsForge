import sqlite3
import os
import re
import glob
import time
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class ObsDatabase:
    """Class to manage an observation file database for data assimilation."""

    def __init__(self, db_path="obs_files.db", base_dir="/home/gvernier/Volumes/hera-s1/runs/realtimeobs/lfs/h1/ops/prod/dcom/"):
        self.db_path = db_path
        self.base_dir = base_dir
        self.create_database()

    def create_database(self):
        """Create the SQLite database and table if it doesn't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS obs_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE,
            obs_time TIMESTAMP,
            ingest_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            obs_dir TEXT,
            instrument TEXT,
            satellite TEXT,
            obs_type TEXT
        )
        """)

        conn.commit()
        conn.close()

    def ingest_files(self, date_str, obs_dir):
        """Scan a specific directory for new observation files and insert them into the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Define the directory path based on date and obs_dir
        obs_path = os.path.join(self.base_dir, date_str, obs_dir)
        #if not os.path.exists(obs_path):
        #    print(f"Warning: Directory {obs_path} does not exist.")
        #    conn.close()
        #    return

        # Regex pattern to extract timestamps, instrument, satellite, and obs_type from filenames
        pattern = re.compile(r"(\d{14})-OSPO-L3U_GHRSST-(\w+)-(\w+)_(\w+)-ACSPO.*\.nc")

        # Get list of NetCDF files
        obs_files = glob.glob(os.path.join(obs_path, "*.nc"))

        for file in obs_files:
            filename = os.path.basename(file)
            match = pattern.match(filename)
            if match:
                obs_time = datetime.strptime(match.group(1), "%Y%m%d%H%M%S")
                obs_type = match.group(2)  # Example: SSTsubskin
                instrument = match.group(3)  # Example: AVHRRF
                satellite = match.group(4)  # Example: MB

                try:
                    cursor.execute("""
                        INSERT INTO obs_files (filename, obs_time, obs_dir, instrument, satellite, obs_type)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (filename, obs_time, obs_dir, instrument, satellite, obs_type))
                except sqlite3.IntegrityError:
                    pass  # Skip duplicates

        conn.commit()
        conn.close()

    def get_valid_files(self, da_cycle, window_hours=3, instrument=None, satellite=None, obs_type=None):
        """Retrieve observation files within a DA window, filtered by instrument, satellite, and observation type."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

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

        cursor.execute(query, tuple(params))
        valid_files = [row[0] for row in cursor.fetchall()]
        conn.close()

        return valid_files

    def watch_directory(self, date_str, obs_dir):
        """Monitor the directory and ingest new files in real-time."""
        class FileHandler(FileSystemEventHandler):
            """Handles new file creation events."""
            def on_created(event_handler, event):
                if event.is_directory:
                    return
                if event.src_path.endswith(".nc"):
                    self.ingest_files(date_str, obs_dir)

        obs_path = os.path.join(self.base_dir, date_str, obs_dir)
        event_handler = FileHandler()
        observer = Observer()
        observer.schedule(event_handler, obs_path, recursive=False)
        observer.start()

        try:
            while True:
                time.sleep(5)  # Adjust polling interval as needed
        except KeyboardInterrupt:
            observer.stop()
        observer.join()


# Example Usage
if __name__ == "__main__":
    db = ObsDatabase()

    # Define a specific date and observation directory
    date_str = "*"  # "20250316"
    obs_dir = "sst"

    # Ingest existing files
    db.ingest_files(date_str, obs_dir)

    # Query files for a given DA cycle
    da_cycle = "20250316000000"  # Example DA cycle time
    valid_files = db.get_valid_files(da_cycle, instrument="VIIRS", satellite="N20", obs_type="SSTsubskin")
    print("Valid observation files:", valid_files)

    print(f"Found {len(valid_files)} valid files for DA cycle {da_cycle}")
    for valid_file in valid_files:
        resolved_date_str = datetime.strptime(valid_file.split('-')[0], "%Y%m%d%H%M%S").strftime("%Y%m%d")
        print(os.path.join(db.base_dir, resolved_date_str, obs_dir, valid_file))


    # Start monitoring the directory for new files (Run in a separate process if needed)
    # db.watch_directory(date_str, obs_dir)
