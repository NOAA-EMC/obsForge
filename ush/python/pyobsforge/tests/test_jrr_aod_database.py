import os
import tempfile
import shutil
import sqlite3
from datetime import datetime

import pytest

from pyobsforge.obsdb.jrr_aod_db import JrrAodDatabase


@pytest.fixture
def temp_obs_dir():
    """Create a temporary directory with mock JRR-AOD NetCDF files."""
    base_dir = tempfile.mkdtemp()
    sub_dir = os.path.join(base_dir, "some_subdir", "jrr_aod")
    os.makedirs(sub_dir)

    # Create mock NetCDF files (content doesn't matter)
    filenames = [
        "JRR-AOD_v3r2_n21_s202503161000000_e202503161030000_c202503161045000.nc",
        "JRR-AOD_v3r2_n21_s202503161200000_e202503161230000_c202503161245000.nc",
        "JRR-AOD_v3r2_n21_s202503161500000_e202503161530000_c202503161545000.nc",
        "invalid_file.nc"
    ]
    for fname in filenames:
        with open(os.path.join(sub_dir, fname), "w") as f:
            f.write("dummy")

    yield base_dir
    shutil.rmtree(base_dir)


@pytest.fixture
def db(temp_obs_dir):
    """
    Create an instance of JrrAodDatabase using in-memory SQLite and
    the temp_obs_dir, then initialize the database.
    """
    db_path = os.path.join(temp_obs_dir, "test_jrr_aod.db")
    database = JrrAodDatabase(
        db_name=db_path,
        dcom_dir=temp_obs_dir,
        obs_dir="jrr_aod"
    )
    return database


def test_create_database(db):
    """
    Test the creation of the database and the 'obs_files' table.

    This test performs the following steps:
    1. Creates the database.
    2. Ingests files into the database.
    3. Connects to the database.
    4. Checks for the existence of the 'obs_files' table.

    Args:
        db: The database object to be tested.

    Asserts:
        - The 'obs_files' table is created in the database.
    """
    db.create_database()
    db.ingest_files()
    db.connect()
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='obs_files'")
    result = cursor.fetchall()
    conn.close()

    assert result is not None, "obs_files table should be created"


def test_parse_valid_filename(db):
    """
    Test the parsing of a valid filename in the database.

    This test performs the following steps:
    1. Creates the database.
    2. Ingests files into the database.
    3. Connects to the database.
    4. Checks the tables in the database.
    5. Parses a given filename and verifies the parsed output.

    Args:
        db: The database object to be tested.

    Asserts:
        - The parsed filename is not None.
        - The first element of the parsed result matches the original filename.
        - The second element of the parsed result matches the expected datetime.
    """
    db.create_database()
    db.ingest_files()
    db.connect()
    fname = "JRR-AOD_v3r2_n21_s202503161234567_e202503161300000_c202503161315000.nc"
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    conn.close()
    parsed = db.parse_filename(fname)

    assert parsed is not None
    assert parsed[0] == fname
    assert parsed[1] == datetime(2025, 3, 16, 12, 34)


def test_parse_invalid_filename(db):
    """
    Test the `parse_filename` method of the database with invalid filenames.

    This test ensures that the `parse_filename` method returns `None` when provided
    with filenames that do not conform to the expected format.

    Args:
        db: The database object that contains the `parse_filename` method.

    Assertions:
        - Asserts that `parse_filename` returns `None` for a completely invalid filename.
        - Asserts that `parse_filename` returns `None` for a filename that partially
          matches the expected format but contains invalid components.
    """
    assert db.parse_filename("garbage.nc") is None
    assert db.parse_filename("JRR-AOD_v3r2_n21_invalid.nc") is None


def test_ingest_files(db):
    """
    Test the ingestion of files into the database.

    This test checks if the `ingest_files` method of the `db` object correctly ingests files into the database.
    It connects to the database, queries the `obs_files` table to count the number of ingested files, and asserts
    that the count is 3, indicating that 3 valid JRR-AOD files should be ingested.
    """
    db.ingest_files()
    conn = sqlite3.connect(db.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM obs_files")
    count = cursor.fetchone()[0]
    conn.close()
    assert count == 3, "Should ingest 3 valid JRR-AOD files"


def test_get_valid_files(db):
    """
    Test the `get_valid_files` method of the database.

    This test ingests files into the database and then retrieves the valid files
    for a given data assimilation (DA) cycle time and cutoff delta. It checks that
    only the files within the specified cutoff delta are returned as valid.

    Steps:
    1. Ingest files into the database.
    2. Define a DA cycle time (`da_cycle`) and a cutoff delta in hours (`cutoff_delta`).
    3. Retrieve the valid files using the `get_valid_files` method.
    4. Assert that files at 10:00 and 12:00 are within +/- 3 hours of 12:00.
    5. Assert that files at 15:00 are not within the valid range.
    6. Assert that the total number of valid files is 2.

    Args:
        db: The database object to be tested.
    """
    db.ingest_files()
    da_cycle = "20250316120000"
    cutoff_delta = 3  # hours

    valid_files = db.get_valid_files(da_cycle, cutoff_delta=cutoff_delta)

    # Only files at 10:00 and 12:00 should be within +/- 3 hours of 12:00
    assert any("202503161000" in f for f in valid_files)
    assert any("202503161200" in f for f in valid_files)
    assert all("202503161500" not in f for f in valid_files)
    assert len(valid_files) == 2
