import os
import glob
import tempfile
import shutil
import sqlite3
from datetime import datetime, timedelta

import pytest

from pyobsforge.obsdb.rads_db import RADSDatabase


@pytest.fixture
def temp_obs_dir():
    """Create a temp directory with mock RADS NetCDF files."""
    base_dir = tempfile.mkdtemp()
    sub_dir = os.path.join(base_dir, "some_subdir", "wgrdbul", "adt")
    os.makedirs(sub_dir)

    # Desired datetime for file timestamps
    mock_time = datetime(2025, 3, 16, 11, 0, 0).timestamp()

    # Create mock NetCDF files
    filenames = [
        "rads_adt_3a_2025075.nc",
        "rads_adt_3b_2025075.nc",
        "rads_adt_6a_2025075.nc",
        "rads_adt_c2_2025075.nc",
        "rads_adt_j3_2025075.nc",
        "rads_adt_ncoda_3a_2025075.nc",
        "rads_adt_ncoda_3b_2025075.nc",
        "rads_adt_ncoda_6a_2025075.nc",
        "rads_adt_ncoda_c2_2025075.nc",
        "rads_adt_ncoda_j3_2025075.nc",
        "rads_adt_ncoda_sa_2025075.nc",
        "rads_adt_ncoda_sw_2025075.nc",
        "rads_adt_sa_2025075.nc",
        "rads_adt_sw_2025075.nc"
        "invalid_file.nc"
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
    db_path = os.path.join(temp_obs_dir, "rads_test.db")
    return RADSDatabase(db_name=db_path, dcom_dir=temp_obs_dir, obs_dir="wgrdbul/adt")


def test_create_database(db):
    db.create_database()
    conn = sqlite3.connect(db.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='obs_files'")
    assert cursor.fetchone() is not None
    conn.close()


def test_parse_valid_filename(db):
    fname = "rads_adt_j3_2025075.nc"
    fname = glob.glob(os.path.join(db.base_dir, fname))[0]
    parsed = db.parse_filename(fname)
    creation_time = datetime.fromtimestamp(os.path.getctime(fname))

    assert parsed is not None
    assert parsed[0] == fname
    assert parsed[1] == datetime(2025, 3, 16, 12, 0)
    assert parsed[2] == creation_time
    assert parsed[3] == "j3"


def test_parse_invalid_filename(db):
    assert db.parse_filename("rads_adt_ncoda_sw_2025073.nc") is None
    assert db.parse_filename("20250316_invalid_filename.nc") is None


def test_ingest_files(db):
    db.ingest_files()
    conn = sqlite3.connect(db.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM obs_files")
    count = cursor.fetchone()[0]
    conn.close()
    assert count == 6, "Should ingest 6 valid RADS files"


def test_get_valid_files(db):
    db.ingest_files()
    da_cycle = "20250316120000"
    window_begin = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") - timedelta(hours=3)
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=1)
    dst_dir = 'rads'
    valid_files = db.get_valid_files(window_begin=window_begin,
                                     window_end=window_end,
                                     dst_dir=dst_dir,
                                     satellite="c2")

    assert any("2025075" in f for f in valid_files)
    assert len(valid_files) == 1
