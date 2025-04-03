import os
import glob
from datetime import datetime, timedelta
from pyobsforge.obsdb import BaseDatabase


class GhrSstDatabase(BaseDatabase):
    """Class to manage an observation file database for data assimilation."""

    def __init__(self, db_name="obs_files.db",
                 dcom_dir="/lfs/h1/ops/prod/dcom/",
                 obs_dir="sst"):
        base_dir = os.path.join(dcom_dir, '*', obs_dir)
        super().__init__(db_name, base_dir)

    def create_database(self):
        """
        Create the SQLite database and observation files table.

        This method initializes the database with a table named `obs_files` to store metadata
        about observation files. The table contains the following columns:

        - `id`: A unique identifier for each record (auto-incremented primary key).
        - `filename`: The full path to the observation file (must be unique).
        - `obs_time`: The timestamp of the observation, extracted from the filename.
        - `receipt_time`: The timestamp when the file was added to the `dcom` directory.
        - `instrument`: The instrument used to collect the observation (e.g., AVHRR, VIIRS).
        - `satellite`: The satellite from which the observation was collected (e.g., NPP, NOAA-20).
        - `obs_type`: The type of observation (e.g., SSTsubskin, SSTskin).

        The table is created if it does not already exist.
        """
        query = """
        CREATE TABLE IF NOT EXISTS obs_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE,
            obs_time TIMESTAMP,
            receipt_time TIMESTAMP,
            instrument TEXT,
            satellite TEXT,
            obs_type TEXT
        )
        """
        self.execute_query(query)

    def parse_filename(self, filename):
        """Extract metadata from filenames matching the expected pattern."""
        parts = os.path.basename(filename).replace('_', '-').split('-')
        if len(parts) >= 6 and parts[0].isdigit() and len(parts[0]) == 14:
            obs_time = datetime.strptime(parts[0][0:12], "%Y%m%d%H%M")
            obs_type = parts[4] if len(parts) > 2 else None
            instrument = parts[5] if len(parts) > 3 else None
            satellite = parts[6] if len(parts) > 4 else None
            receipt_time = datetime.fromtimestamp(os.path.getctime(filename))
            return filename, obs_time, receipt_time, instrument, satellite, obs_type
        return None

    def ingest_files(self):
        """Scan the directory for new observation files and insert them into the database."""
        obs_files = glob.glob(os.path.join(self.base_dir, "*-OSPO-L3U_GHRSST-*.nc"))
        print(f"Found {len(obs_files)} new files to ingest")
        for file in obs_files:
            parsed_data = self.parse_filename(file)
            if parsed_data:
                query = """
                    INSERT INTO obs_files (filename, obs_time, receipt_time, instrument, satellite, obs_type)
                    VALUES (?, ?, ?, ?, ?, ?)
                """
                self.insert_record(query, parsed_data)


# Example Usage
if __name__ == "__main__":
    db = GhrSstDatabase(db_name="sst_obs.db",
                        dcom_dir="/home/gvernier/Volumes/hera-s1/runs/realtimeobs/lfs/h1/ops/prod/dcom/",
                        obs_dir="sst")

    # Check for new files
    db.ingest_files()

    # Query files for a given DA cycle
    da_cycle = "20250316000000"
    window_begin = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") - timedelta(hours=3)
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)

    valid_files = db.get_valid_files(window_begin=window_begin,
                                     window_end=window_end,
                                     instrument="VIIRS",
                                     satellite="NPP",
                                     obs_type="SSTsubskin")

    print(f"Found {len(valid_files)} valid files for DA cycle {da_cycle}")
    for valid_file in valid_files:
        if os.path.exists(valid_file):
            print(f"Valid file: {valid_file}")
        else:
            print(f"File does not exist: {valid_file}")
