import os
import glob
from datetime import datetime
from pyobsforge.obsdb import BaseDatabase


class NesdisAmsr2Database(BaseDatabase):
    """Class to manage an observation file database for data assimilation."""

    def __init__(self, db_name="nesdis_amsr2.db",
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
        - `instrument`: The instrument used to collect the observation (e.g., AMSR2).
        - `satellite`: The satellite from which the observation was collected (e.g., GW1).
        - `obs_type`: The type of observation (e.g., SEAICE)

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
        """Extract metadata from filenames matching the AMSR2-SEAICE pattern."""
        # Make sure the filename matches the expected pattern
        # Pattern: AMSR2-SEAICE-NH_v2r2_GW1_s202503140032240_e202503140211220_c202503140245560.nc
        parts = os.path.basename(filename).split('_')

        # Pre-check: Must be an AMSR2-SEAICE file
        if not parts[0].startswith("AMSR2-SEAICE"):
            print(f"[DEBUG] Skipping non AMSR2-SEAICE file: {filename}")
            return None

        try:
            # Extract hemisphere from the first hyphen-separated segment
            name_parts = parts[0].split('-')
            instrument = name_parts[0]
            hemisphere = name_parts[2].lower()

            # Determine obs_type
            if hemisphere == "nh":
                obs_type = "icec_amsr2_north"
            elif hemisphere == "sh":
                obs_type = "icec_amsr2_south"
            else:
                print(f"[DEBUG] Unrecognized hemisphere in filename: {filename}")
                return None

            satellite = parts[2]
            obs_time = datetime.strptime(parts[3][1:16], "%Y%m%d%H%M%S%f")
            receipt_time = datetime.fromtimestamp(os.path.getctime(filename))
            return filename, obs_time, receipt_time, instrument, satellite, obs_type

        except Exception as e:
            print(f"[DEBUG] Error parsing filename {filename}: {e}")
            return None

    def ingest_files(self):
        """Scan the directory for new NESDIS AMSR2 observation files and insert them into the database."""
        obs_files = glob.glob(os.path.join(self.base_dir, "*.nc"))
        print(f"Found {len(obs_files)} new files to ingest")

        records_to_insert = []
        for file in obs_files:
            parsed_data = self.parse_filename(file)
            if parsed_data:
                records_to_insert.append(parsed_data)

        if records_to_insert:
            query = """
                INSERT INTO obs_files (filename, obs_time, receipt_time, instrument, satellite, obs_type)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            try:
                self.insert_records(query, records_to_insert)
                print(f"################################ Successfully ingested {len(records_to_insert)} files into the database.")
            except Exception as e:
                print(f"Failed to insert records: {e}")
