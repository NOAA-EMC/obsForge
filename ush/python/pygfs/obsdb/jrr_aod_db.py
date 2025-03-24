import os
import glob
from datetime import datetime
from obsdb import BaseDatabase
import re

class JrrAodDatabase(BaseDatabase):
    """Class to manage an observation file database for JRR-AOD data."""

    def __init__(self, db_name="jrr_aod_obs.db",
                 dcom_dir="/home/gvernier/Volumes/hera-s1/runs/realtimeobs/lfs/h1/ops/prod/dcom/",
                 obs_dir="jrr_aod"):
        base_dir = os.path.join(dcom_dir, '*', obs_dir)
        super().__init__(db_name, base_dir)

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
        # Make sure the filename matches the expected pattern
        # Pattern: JRR-AOD_v3r2_n21_sYYYYMMDDHHMMSS_eYYYYMMDDHHMMSS_cYYYYMMDDHHMMSS.nc
        basename = os.path.basename(filename)
        parts = basename.split('_')
        try:
            if len(parts) >= 4 and parts[0] == "JRR-AOD":
                obs_time = datetime.strptime(parts[3][1:13], "%Y%m%d%H%M")
                return filename, obs_time
        except ValueError:
            return None

        return None

    def ingest_files(self):
        """Scan the directory for new JRR-AOD observation files and insert them into the database."""
        obs_files = glob.glob(os.path.join(self.base_dir, "*.nc"))
        print(f"Found {len(obs_files)} new files to ingest")
        for file in obs_files:
            parsed_data = self.parse_filename(file)
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
   cutoff_delta = 5  #datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)
   valid_files = db.get_valid_files(da_cycle, cutoff_delta=cutoff_delta)

   print(f"Found {len(valid_files)} valid files for DA cycle {da_cycle}")
   for valid_file in valid_files:
       if os.path.exists(valid_file):
           print(f"Valid file: {valid_file}")
       else:
           print(f"File does not exist: {valid_file}")
