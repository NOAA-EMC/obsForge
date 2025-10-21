import os
import glob
import tempfile
import shutil
import sqlite3
from datetime import datetime, timedelta

import pytest

from pyobsforge.obsdb.jrr_aod_db import JrrAodDatabase


@pytest.fixture
def temp_obs_dir():
    """Create a temporary directory with mock JRR-AOD NetCDF files."""
    base_dir = tempfile.mkdtemp()
    sub_dir = os.path.join(base_dir, "some_subdir", "jrr_aod")
    os.makedirs(sub_dir)

    # Desired datetime for file timestamps
    mock_time = datetime(2025, 3, 16, 12, 0, 0).timestamp()

    # Create mock NetCDF files (content doesn't matter)
    filenames = [
        "JRR-AOD_v3r2_n21_s202503161000000_e202503161030000_c202503161045000.nc",
        "JRR-AOD_v3r2_n21_s202503161200000_e202503161230000_c202503161245000.nc",
        "JRR-AOD_v3r2_n21_s202503161500000_e202503161530000_c202503161545000.nc",
        "invalid_file.nc",
        "JRR-AOD_v3r2_n20_s202503161000000_e202503161030000_c202503161045000.nc",
        "JRR-AOD_v3r2_n20_s202503161200000_e202503161230000_c202503161245000.nc",
        "JRR-AOD_v3r2_n20_s202503161500000_e202503161530000_c202503161545000.nc"
    ]
    for fname in filenames:
        fname_tmp = os.path.join(sub_dir, fname)
        with open(fname_tmp, "w") as f:
            f.write("dummy")
        os.utime(fname_tmp, (mock_time, mock_time))  # (access_time, modification_time)

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
    conn = sqlite3.connect(db.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='obs_files'")
    assert cursor.fetchone() is not None
    conn.close()


def test_parse_valid_filename(db):
    """
    Test the parsing of a valid filename in the database.
    """
    print(glob.glob(os.path.join(db.base_dir, "*")))
    fname = "JRR-AOD_v3r2_n20_s202503161200000_e202503161230000_c202503161245000.nc"
    fname = glob.glob(os.path.join(db.base_dir, fname))[0]
    parsed = db.parse_filename(fname)
    creation_time = datetime.fromtimestamp(os.path.getctime(fname))

    assert parsed is not None
    assert parsed[0] == fname
    assert parsed[1] == datetime(2025, 3, 16, 12, 0)
    assert parsed[2] == creation_time
    assert parsed[3] == "n20"


def test_parse_invalid_filename(db):
    """
    Test the `parse_filename` method of the database with invalid filenames.
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
    assert count == 6, "Should ingest 3 valid JRR-AOD files"


def test_get_valid_files(db):
    """
    Test the `get_valid_files` method of the database.
    """
    db.ingest_files()
    da_cycle = "20250316120000"
    window_begin = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") - timedelta(hours=3)
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)
    dst_dir = 'jrr_aod'
    valid_files = db.get_valid_files(window_begin=window_begin,
                                     window_end=window_end,
                                     dst_dir=dst_dir,
                                     satellite="n21")

    assert any("202503161000" in f for f in valid_files)
    assert any("202503161200" in f for f in valid_files)
    assert all("202603161500" not in f for f in valid_files)
    assert len(valid_files) == 3


def test_get_valid_files_receipt(db):
    """
    Test the `get_valid_files` method of the database with exclude_after_receipt.
    """
    db.ingest_files()
    da_cycle = "20250316120000"
    window_begin = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") - timedelta(hours=3)
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)
    dst_dir = 'jrr_aod'
    valid_files = db.get_valid_files(window_begin=window_begin,
                                     window_end=window_end,
                                     dst_dir=dst_dir,
                                     satellite="n21",
                                     check_receipt='gfs')

    print(valid_files)
    # TODO (G): Giving up for now on trying to mock the receipt time, will revisit later
    assert len(valid_files) == 3
