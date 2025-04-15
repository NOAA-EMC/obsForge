import os
import glob
import tempfile
import shutil
import sqlite3
from datetime import datetime, timedelta

import pytest

from pyobsforge.obsdb.amsr2_db import Amsr2Database  # Adjust as needed


@pytest.fixture
def temp_obs_dir():
    """Create a temp directory with mock AMSR2 NetCDF files."""
    base_dir = tempfile.mkdtemp()
    sub_dir = os.path.join(base_dir, "some_subdir", "seaice/pda")
    os.makedirs(sub_dir)

    # Desired datetime for file timestamps
    mock_time = datetime(2025, 3, 16, 0, 0, 0).timestamp()

    # Create mock NetCDF files
    filenames = [
        "AMSR2-SEAICE-NH_v2r2_GW1_s202503160514240_e202503160653220_c202503160725420.nc",
        "AMSR2-SEAICE-NH_v2r2_GW1_s202503160653240_e202503160829230_c202503160902250.nc",
        "AMSR2-SEAICE-NH_v2r2_GW1_s202503161326240_e202503161502220_c202503161540340.nc",
        "invalid_file.nc",
        "AMSR2-SEAICE-SH_v2r2_GW1_s202503160514240_e202503160653220_c202503160725420.nc",
        "AMSR2-SEAICE-SH_v2r2_GW1_s202503160653240_e202503160829230_c202503160902250.nc",
        "AMSR2-SEAICE-SH_v2r2_GW1_s202503161326240_e202503161502220_c202503161540340.nc"
    ]
    for fname in filenames:
        fname_tmp = os.path.join(sub_dir, fname)
        with open(fname_tmp, "w") as f:
            f.write("fake content")
        os.utime(fname_tmp, (mock_time, mock_time))  # (access_time, modification_time)

    yield base_dir
    shutil.rmtree(base_dir)


@pytest.fixture
def db(temp_obs_dir):
    """Initialize test database."""
    db_path = os.path.join(temp_obs_dir, "amsr2_test.db")
    database = Amsr2Database(
        db_name=db_path,
        dcom_dir=temp_obs_dir,
        obs_dir="seaice/pda"
    )
    return database


def test_create_database(db):
    db.create_database()
    conn = sqlite3.connect(db.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='obs_files'")
    assert cursor.fetchone() is not None
    conn.close()


def test_parse_valid_filename(db):
    print(glob.glob(os.path.join(db.base_dir, "*")))
    fname = "AMSR2-SEAICE-NH_v2r2_GW1_s202503160653240_e202503160829230_c202503160902250.nc"
    fname = glob.glob(os.path.join(db.base_dir, fname))[0]
    parsed = db.parse_filename(fname)
    creation_time = datetime.fromtimestamp(os.path.getctime(fname))
    
    assert parsed is not None
    assert parsed[0] == fname
    assert parsed[1] == datetime(2025, 3, 16, 6, 53, 24)  # Start time
    # assert parsed[2] == creation_time
    assert parsed[2] == datetime(2025, 3, 16, 9, 2, 25)
    assert parsed[3] == "GW1"   


def test_parse_invalid_filename(db):
    assert db.parse_filename("junk.nc") is None
    assert db.parse_filename("AMSR2-SEAICE-NH_v2r2_GW1_invalid.nc") is None


def test_ingest_files(db):
    db.ingest_files()
    conn = sqlite3.connect(db.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM obs_files")
    count = cursor.fetchone()[0]
    conn.close()
    assert count == 6, "Should ingest 3 valid AMSR2 files"


def test_get_valid_files(db):
    db.ingest_files()
    da_cycle = "20250316060000"
    window_begin = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") - timedelta(hours=3)
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)
    dst_dir = 'seaice/pda'
    # Test for AVHRRF_MB
    valid_files = db.get_valid_files(window_begin=window_begin,
                                     window_end=window_end,
                                     dst_dir=dst_dir,
    #                                 instrument="AMSR2",
                                     satellite="GW1") #,
    #                                 obs_type="SEAICE")

    # Files at 10:00 and 12:00 are within +/- 3h of 00:00
    assert any("202503160514" in f for f in valid_files)
    assert any("202503160653" in f for f in valid_files)
    assert all("202503161326" not in f for f in valid_files)
    assert len(valid_files) == 4


def test_get_valid_files_receipt(db):
    db.ingest_files()
    da_cycle = "20250316060000"
    window_begin = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") - timedelta(hours=3)
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)
    dst_dir = 'seaice/pda'

    # Test for AVHRRF_MB
    valid_files = db.get_valid_files(window_begin=window_begin,
                                     window_end=window_end,
                                     dst_dir=dst_dir,
    #                                 instrument="AMSR2",
                                     satellite="GW1",
     #                                obs_type="SEAICE",
                                     check_receipt='gfs')

    # TODO (G): Giving up for now on trying to mock the receipt time, will revisit later
    assert len(valid_files) == 2
