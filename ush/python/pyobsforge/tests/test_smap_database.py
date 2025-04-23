import os
import glob
import tempfile
import shutil
import sqlite3
from datetime import datetime, timedelta

import pytest

from pyobsforge.obsdb.smap_db import SmapDatabase  # Adjust as needed


@pytest.fixture
def temp_obs_dir():
    """Create a temp directory with mock SMAP_SSS h5 files."""
    base_dir = tempfile.mkdtemp()
    sub_dir = os.path.join(base_dir, "some_subdir", "wtxtbul/satSSS/SMAP")
    os.makedirs(sub_dir)

    # Desired datetime for file timestamps
    mock_time = datetime(2025, 3, 16, 6, 0, 0).timestamp()

    # Create mock NetCDF files
    filenames = [
        "SMAP_L2B_SSS_NRT_54061_A_20250316T001612.h5",
        "SMAP_L2B_SSS_NRT_54061_D_20250316T001612.h5",
        "SMAP_L2B_SSS_NRT_54062_A_20250316T015440.h5",
        "SMAP_L2B_SSS_NRT_54062_D_20250316T015440.h5",
        "SMAP_L2B_SSS_NRT_54063_A_20250316T033308.h5",
        "SMAP_L2B_SSS_NRT_54063_D_20250316T033308.h5",
        "SMAP_L2B_SSS_NRT_54064_A_20250316T051136.h5",
        "SMAP_L2B_SSS_NRT_54064_D_20250316T051136.h5",
        "SMAP_L2B_SSS_NRT_54065_A_20250316T065004.h5",
        "SMAP_L2B_SSS_NRT_54065_D_20250316T065004.h5",
        "SMAP_L2B_SSS_NRT_54066_A_20250316T082832.h5",
        "SMAP_L2B_SSS_NRT_54066_D_20250316T082832.h5",
        "SMAP_L2B_SSS_NRT_54067_A_20250316T100700.h5",
        "SMAP_L2B_SSS_NRT_54067_D_20250316T100700.h5",
        "SMAP_L2B_SSS_NRT_54068_D_20250316T114527.h5",
        "SMAP_L2B_SSS_NRT_54069_A_20250316T132356.h5",
        "SMAP_L2B_SSS_NRT_54069_D_20250316T132356.h5",
        "SMAP_L2B_SSS_NRT_54070_A_20250316T150223.h5",
        "SMAP_L2B_SSS_NRT_54070_D_20250316T150223.h5",
        "SMAP_L2B_SSS_NRT_54071_A_20250316T164051.h5",
        "SMAP_L2B_SSS_NRT_54071_D_20250316T164051.h5",
        "SMAP_L2B_SSS_NRT_54072_A_20250316T181918.h5",
        "SMAP_L2B_SSS_NRT_54072_D_20250316T181918.h5",
        "SMAP_L2B_SSS_NRT_54073_A_20250316T195746.h5",
        "SMAP_L2B_SSS_NRT_54073_D_20250316T195746.h5",
        "SMAP_L2B_SSS_NRT_54074_A_20250316T213615.h5",
        "SMAP_L2B_SSS_NRT_54074_D_20250316T213615.h5",
        "SMAP_L2B_SSS_NRT_54075_A_20250316T231442.h5"
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
    db_path = os.path.join(temp_obs_dir, "smap_test.db")
    database = SmapDatabase(
        db_name=db_path,
        dcom_dir=temp_obs_dir,
        obs_dir="wtxtbul/satSSS/SMAP"
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
    fname = "SMAP_L2B_SSS_NRT_54065_A_20250316T065004.h5"
    fname = glob.glob(os.path.join(db.base_dir, fname))[0]
    parsed = db.parse_filename(fname)
    creation_time = datetime.fromtimestamp(os.path.getctime(fname))

    assert parsed is not None
    assert parsed[0] == fname
    assert parsed[1] == datetime(2025, 3, 16, 6, 50, 4)
    assert parsed[2] == creation_time
    assert parsed[3] == "SMAP"


def test_parse_invalid_filename(db):
    assert db.parse_filename("junk.nc") is None
    assert db.parse_filename("SMAP_L2B_SSS_NRT_invalid.nc") is None


def test_ingest_files(db):
    db.ingest_files()
    conn = sqlite3.connect(db.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM obs_files")
    count = cursor.fetchone()[0]
    conn.close()
    assert count == 28, "Should ingest 28 valid SMAP files"


def test_get_valid_files(db):
    db.ingest_files()
    da_cycle = "20250316060000"
    window_begin = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") - timedelta(hours=3)
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)
    dst_dir = 'sss'
    # Test for SMAP SSS
    valid_files = db.get_valid_files(window_begin=window_begin,
                                     window_end=window_end,
                                     dst_dir=dst_dir,
                                     satellite="SMAP")

    print("Valid files in window:", valid_files)

    # Files at 03:00 and 09:00 are within +/- 3h of 06:00
    assert any("20250316T0511" in f for f in valid_files)
    assert any("20250316T0650" in f for f in valid_files)
    assert all("20250316T1007" not in f for f in valid_files)
    assert len(valid_files) == 8


def test_get_valid_files_receipt(db):
    db.ingest_files()
    da_cycle = "20250316060000"
    window_begin = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") - timedelta(hours=3)
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)
    dst_dir = 'sss'

    # Test for SMAP SSS
    valid_files = db.get_valid_files(window_begin=window_begin,
                                     window_end=window_end,
                                     dst_dir=dst_dir,
                                     satellite="SMAP",
                                     check_receipt='gfs')

    # TODO (G): Giving up for now on trying to mock the receipt time, will revisit later
    assert len(valid_files) == 8
