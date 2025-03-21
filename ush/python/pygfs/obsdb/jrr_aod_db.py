import os
import re
import glob
from datetime import datetime, timedelta
from obsdb import BaseDatabase

class JrrAodDatabase(BaseDatabase):
    """Class to manage an observation file database for JRR-AOD data."""

    def __init__(self, db_path="jrr_aod_obs.db",
                 dcom_dir="/home/gvernier/Volumes/hera-s1/runs/realtimeobs/lfs/h1/ops/prod/dcom/",
                 obs_dir="jrr_aod",
                 pattern=re.compile(r"JRR-AOD_v\d+r\d+_n\d+_s(\d{15})_e\d{15}_c\d{15}\.nc")):
        base_dir = os.path.join(dcom_dir, '*', obs_dir)
        super().__init__(db_path, base_dir, pattern)

    def create_database(self):
        """Create the SQLite database and observation files table."""
        query = """
        CREATE TABLE IF NOT EXISTS obs_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE,
            obs_time TIMESTAMP,
            ingest_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.execute_query(query)

    def parse_filename(self, filename):
        """Extract metadata from filenames matching the JRR-AOD pattern."""
        match = self.pattern.match(filename)
        if match:
            obs_time = datetime.strptime(match.group(1)[:12], "%Y%m%d%H%M")  # Extract only YYYYMMDDHHMM
            return filename, obs_time
        return None

    def ingest_files(self):
        """Scan the directory for new JRR-AOD observation files and insert them into the database."""
        obs_files = glob.glob(os.path.join(self.base_dir, "*.nc"))

        for file in obs_files:
            filename = os.path.basename(file)
            parsed_data = self.parse_filename(filename)
            if parsed_data:
                query = """
                    INSERT INTO obs_files (filename, obs_time)
                    VALUES (?, ?)
                """
                self.insert_record(query, parsed_data)

if __name__ == "__main__":
   db = JrrAodDatabase()

   # Check for new files
   db.ingest_files()

   # Query files for a given DA cycle
   da_cycle = "20250316120000"
   cutoff_time = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)
   valid_files = db.get_valid_files(da_cycle) #, cutoff_time=cutoff_time)

   print("Valid observation files:", valid_files)
   print(f"Found {len(valid_files)} valid files for DA cycle {da_cycle}")
   for valid_file in valid_files:
       print(valid_file)
