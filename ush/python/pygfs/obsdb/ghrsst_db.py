import os
import re
import glob
from datetime import datetime, timedelta
from obsdb import BaseDatabase


class GhrSstDatabase(BaseDatabase):
    """Class to manage an observation file database for data assimilation."""

    def __init__(self, db_path="obs_files.db",
                 dcom_dir="/home/gvernier/Volumes/hera-s1/runs/realtimeobs/lfs/h1/ops/prod/dcom/",
                 obs_dir="sst",
                 pattern=re.compile(r"(\d{14})-OSPO-L3U_GHRSST-(\w+)-(\w+)_(\w+)-ACSPO.*\.nc")):
        base_dir = os.path.join(dcom_dir, '*', obs_dir)
        super().__init__(db_path, base_dir, pattern)

    def create_database(self):
        """Create the SQLite database and observation files table."""
        query = """
        CREATE TABLE IF NOT EXISTS obs_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE,
            obs_time TIMESTAMP,
            ingest_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            instrument TEXT,
            satellite TEXT,
            obs_type TEXT
        )
        """
        self.execute_query(query)

    def parse_filename(self, filename):
        """Extract metadata from filenames matching the expected pattern."""
        match = self.pattern.match(filename)
        if match:
            obs_time = datetime.strptime(match.group(1)[0:12], "%Y%m%d%H%M")
            obs_type = match.group(2) if len(match.groups()) > 1 else None
            instrument = match.group(3) if len(match.groups()) > 2 else None
            satellite = match.group(4) if len(match.groups()) > 3 else None
            return filename, obs_time, instrument, satellite, obs_type
        return None


# Example Usage
if __name__ == "__main__":
    db = GhrSstDatabase(db_path="sst_obs.db",
                        dcom_dir="/home/gvernier/Volumes/hera-s1/runs/realtimeobs/lfs/h1/ops/prod/dcom/",
                        obs_dir="sst",
                        pattern=re.compile(r"(\d{14})-OSPO-L3U_GHRSST-(\w+)-(\w+)_(\w+)-ACSPO.*\.nc"))

    # Check for new files
    #db.ingest_files()

    # Query files for a given DA cycle
    da_cycle = "20250316000000"
    cutoff_time = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)
    valid_files = db.get_valid_files(da_cycle,
                                     instrument="AVHRRF",
                                     satellite="MB",
                                     obs_type="SSTsubskin",
                                     cutoff_time=cutoff_time)
    print("Valid observation files:", valid_files)

    print(f"Found {len(valid_files)} valid files for DA cycle {da_cycle}")
    for valid_file in valid_files:
        print(valid_file)
