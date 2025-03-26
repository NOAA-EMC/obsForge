import os
import glob
import tempfile
import shutil
import sqlite3
from datetime import datetime, timedelta

import pytest

from pyobsforge.obsdb.ghrsst_db import GhrSstDatabase  # Adjust as needed


@pytest.fixture
def temp_obs_dir():
    """Create a temp directory with mock GHRSST NetCDF files."""
    base_dir = tempfile.mkdtemp()
    sub_dir = os.path.join(base_dir, "some_subdir", "sst")
    os.makedirs(sub_dir)

    # Desired datetime for file timestamps
    mock_time = datetime(2025, 3, 16, 11, 0, 0).timestamp()

    # Create mock NetCDF files
    filenames = [
        "20250316100000-OSPO-L3U_GHRSST-SSTsubskin-AVHRRF_MB-ACSPO.nc",
        "20250316120000-OSPO-L3U_GHRSST-SSTsubskin-AVHRRF_MB-ACSPO.nc",
        "20250316150000-OSPO-L3U_GHRSST-SSTsubskin-AVHRRF_MB-ACSPO.nc",
        "20250316100000-OSPO-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO.nc",
        "20250316120000-OSPO-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO.nc",
        "20250316150000-OSPO-L3U_GHRSST-SSTsubskin-AVHRRF_MC-ACSPO.nc",
        "20250316100000-OSPO-L3U_GHRSST-SSTsubskin-VIIRS_NPP-ACSPO.nc",
        "20250316120000-OSPO-L3U_GHRSST-SSTsubskin-VIIRS_NPP-ACSPO.nc",
        "20250316150000-OSPO-L3U_GHRSST-SSTsubskin-VIIRS_NPP-ACSPO.nc",
        "20250316100000-OSPO-L3U_GHRSST-SSTsubskin-VIIRS_N20-ACSPO.nc",
        "20250316120000-OSPO-L3U_GHRSST-SSTsubskin-VIIRS_N20-ACSPO.nc",
        "20250316150000-OSPO-L3U_GHRSST-SSTsubskin-VIIRS_N20-ACSPO.nc",
        "20250316100000-OSPO-L3U_GHRSST-SSTsubskin-VIIRS_N21-ACSPO.nc",
        "20250316120000-OSPO-L3U_GHRSST-SSTsubskin-VIIRS_N21-ACSPO.nc",
        "20250316150000-OSPO-L3U_GHRSST-SSTsubskin-VIIRS_N21-ACSPO.nc",
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
    db_path = os.path.join(temp_obs_dir, "ghrsst_test.db")
    return GhrSstDatabase(db_name=db_path, dcom_dir=temp_obs_dir, obs_dir="sst")


def test_create_database(db):
    db.create_database()
    conn = sqlite3.connect(db.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='obs_files'")
    assert cursor.fetchone() is not None
    conn.close()


def test_parse_valid_filename(db):
    fname = "20250316120000-OSPO-L3U_GHRSST-SSTsubskin-AVHRRF_MB-ACSPO.nc"
    fname = glob.glob(os.path.join(db.base_dir, fname))[0]
    parsed = db.parse_filename(fname)
    creation_time = datetime.fromtimestamp(os.path.getctime(fname))

    assert parsed is not None
    assert parsed[0] == fname
    assert parsed[1] == datetime(2025, 3, 16, 12, 00)
    assert parsed[2] == creation_time
    assert parsed[3] == "AVHRRF"
    assert parsed[4] == "MB"
    assert parsed[5] == "SSTsubskin"


def test_parse_invalid_filename(db):
    assert db.parse_filename("junk.nc") is None
    assert db.parse_filename("20250316_invalid_filename.nc") is None


def test_ingest_files(db):
    db.ingest_files()
    conn = sqlite3.connect(db.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM obs_files")
    count = cursor.fetchone()[0]
    conn.close()
    assert count == 15, "Should ingest 3 valid GHRSST files"


def test_get_valid_files(db):
    db.ingest_files()
    da_cycle = "20250316120000"
    window_begin = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") - timedelta(hours=3)
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=1)

    # Test for AVHRRF_MB
    valid_files = db.get_valid_files(window_begin=window_begin,
                                     window_end=window_end,
                                     instrument="AVHRRF",
                                     satellite="MB",
                                     obs_type="SSTsubskin")

    # Files at 10:00 and 12:00 are within +/- 3h of 12:00
    assert any("202503161000" in f for f in valid_files)
    assert any("202503161200" in f for f in valid_files)
    assert all("202503161500" not in f for f in valid_files)
    assert len(valid_files) == 2

    # Test for VIIRS_NPP
    valid_files = db.get_valid_files(window_begin=window_begin,
                                     window_end=window_end,
                                     instrument="VIIRS",
                                     satellite="NPP",
                                     obs_type="SSTsubskin")

    # Files at 10:00 and 12:00 are within +/- 3h of 12:00
    assert any("202503161000" in f for f in valid_files)
    assert any("202503161200" in f for f in valid_files)
    assert all("202503161500" not in f for f in valid_files)
    assert len(valid_files) == 2

    # Test for VIIRS_N20
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)
    valid_files = db.get_valid_files(window_begin=window_begin,
                                     window_end=window_end,
                                     instrument="VIIRS",
                                     satellite="N20",
                                     obs_type="SSTsubskin")

    # Files at 10:00 and 12:00 are within +/- 3h of 12:00
    assert any("202503161000" in f for f in valid_files)
    assert any("202503161200" in f for f in valid_files)
    assert len(valid_files) == 3

#    # Test for VIIRS_N21
    valid_files = db.get_valid_files(window_begin=window_begin,
                                     window_end=window_end,
                                     instrument="VIIRS",
                                     satellite="N21",
                                     obs_type="SSTsubskin")

    # Files at 10:00 and 12:00 are within +/- 3h of 12:00
    assert any("202503161000" in f for f in valid_files)
    assert any("202503161200" in f for f in valid_files)
    assert len(valid_files) == 3


def test_get_valid_files_receipt(db):
    db.ingest_files()
    da_cycle = "20250316120000"
    window_begin = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") - timedelta(hours=3)
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=1)

    # Test for AVHRRF_MB
    valid_files = db.get_valid_files(window_begin=window_begin,
                                     window_end=window_end,
                                     instrument="AVHRRF",
                                     satellite="MB",
                                     obs_type="SSTsubskin",
                                     check_receipt='gfs')

    # TODO (G): Giving up for now on trying to mock the receipt time, will revisit later
    assert len(valid_files) == 2
