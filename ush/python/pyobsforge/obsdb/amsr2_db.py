import os
import glob
from datetime import datetime
from pyobsforge.obsdb import BaseDatabase


class Amsr2Database(BaseDatabase):
    """Class to manage an observation file database for data assimilation."""

    def __init__(self, db_name="amsr2.db",
                 dcom_dir="/lfs/h1/ops/prod/dcom/",
                 obs_dir="seaice/pda"):
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
        - `satellite`: The satellite from which the observation was collected (e.g., GW1).

        The table is created if it does not already exist.
        """
        query = """
        CREATE TABLE IF NOT EXISTS obs_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE,
            obs_time TIMESTAMP,
            receipt_time TIMESTAMP,
            satellite TEXT
        )
        """
        self.execute_query(query)

    def parse_filename(self, filename):
        basename = os.path.basename(filename)
        parts = basename.split('_')
        try:
            if len(parts) >= 6 and parts[0].startswith("AMSR2-SEAICE"):
                satellite = parts[2]  # "GW1"
                obs_time_str = parts[3][1:15]  # s202503160653240
                receipt_time_str = parts[5][1:15]  # c202503160902250
                # obs_type = parts[0].split('-')[2]  # "SH", "NH"
                if len(obs_time_str) == 15:
                    obs_time = datetime.strptime(obs_time_str, "%Y%m%d%H%M%S%f")
                else:
                    obs_time = datetime.strptime(obs_time_str, "%Y%m%d%H%M%S")


                if len(receipt_time_str) == 14:
                    receipt_time_str += "000000"  # add microseconds if missing

                receipt_time = datetime.strptime(receipt_time_str, "%Y%m%d%H%M%S%f")
                return filename, obs_time, receipt_time, satellite
        except ValueError as e:
            print(f"[DEBUG] Error parsing filename {filename}: {e}")
            return None


    def ingest_files(self):
        """Scan the directory for new observation files and insert them into the database."""
        obs_files = glob.glob(os.path.join(self.base_dir, "*.nc"))
        print(f"Found {len(obs_files)} new files to ingest")
        print(f"[DEBUG] Files found: {obs_files}")

        # Counter for successful ingestions
        ingested_count = 0

        for file in obs_files:
            parsed_data = self.parse_filename(file)
            if parsed_data:
                query = """
                    INSERT INTO obs_files (filename, obs_time, receipt_time, satellite)
                    VALUES (?, ?, ?, ?)
                """
                try:
                    self.insert_record(query, parsed_data)
                    ingested_count += 1
                except Exception as e:
                    print(f"Failed to insert record for {file}: {e}")
        print(f"################################ Successfully ingested {ingested_count} files into the database.")
