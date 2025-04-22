import os
import glob
import tempfile
import shutil
import sqlite3
from datetime import datetime, timedelta

import pytest

from pyobsforge.obsdb.smos_db import SmosDatabase  # Adjust as needed


@pytest.fixture
def temp_obs_dir():
    """Create a temp directory with mock SMOS_SSS nc files."""
    base_dir = tempfile.mkdtemp()
    sub_dir = os.path.join(base_dir, "some_subdir", "wtxtbul/satSSS/SMOS")
    os.makedirs(sub_dir)

    # Desired datetime for file timestamps
    mock_time = datetime(2025, 3, 16, 6, 0, 0).timestamp()

    # Create mock NetCDF files
    filenames = [
        "SM_OPER_MIR_OSUDP2_20250316T002309_20250316T011621_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T011306_20250316T020624_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T020312_20250316T025626_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T025309_20250316T034629_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T034319_20250316T043631_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T043313_20250316T052634_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T052327_20250316T061636_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T061318_20250316T070637_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T070327_20250316T075640_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T075327_20250316T084642_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T084330_20250316T093645_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T093328_20250316T102647_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T102335_20250316T111649_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T111332_20250316T120652_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T120340_20250316T125654_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T125337_20250316T134656_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T134343_20250316T143658_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T143340_20250316T152700_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T152349_20250316T161703_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T161346_20250316T170705_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T170353_20250316T175708_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T175351_20250316T184710_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T184359_20250316T193713_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T193354_20250316T202714_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T202402_20250316T211716_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T211359_20250316T220719_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T220407_20250316T225721_700_001_1.nc",
        "SM_OPER_MIR_OSUDP2_20250316T225404_20250316T234724_700_001_1.nc"
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
    db_path = os.path.join(temp_obs_dir, "smos_test.db")
    database = SmosDatabase(
        db_name=db_path,
        dcom_dir=temp_obs_dir,
        obs_dir="wtxtbul/satSSS/SMOS"
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
    fname = "SM_OPER_MIR_OSUDP2_20250316T061318_20250316T070637_700_001_1.nc"
    fname = glob.glob(os.path.join(db.base_dir, fname))[0]
    parsed = db.parse_filename(fname)
    creation_time = datetime.fromtimestamp(os.path.getctime(fname))
    
    assert parsed is not None
    assert parsed[0] == fname
    assert parsed[1] == datetime(2025, 3, 16, 6, 13, 18)  # Start time
    assert parsed[2] == creation_time
    assert parsed[3] == "SMOS"   


def test_parse_invalid_filename(db):
    assert db.parse_filename("junk.nc") is None
    assert db.parse_filename("SM_OPER_MIR_OSUDP2_invalid.nc") is None


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
    # Test for SMOS SSS
    valid_files = db.get_valid_files(window_begin=window_begin,
                                     window_end=window_end,
                                     dst_dir=dst_dir,
                                     satellite="SMOS")

    print("Valid files in window:", valid_files)

    # Files at 03:00 and 09:00 are within +/- 3h of 06:00
    assert any("20250316T0523" in f for f in valid_files)
    assert any("20250316T0613" in f for f in valid_files)
    assert all("20250316T1023" not in f for f in valid_files)
    assert len(valid_files) == 7


def test_get_valid_files_receipt(db):
    db.ingest_files()
    da_cycle = "20250316060000"
    window_begin = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") - timedelta(hours=3)
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)
    dst_dir = 'sss'

    # Test for SMOS SSS
    valid_files = db.get_valid_files(window_begin=window_begin,
                                     window_end=window_end,
                                     dst_dir=dst_dir,
                                     satellite="SMOS",
                                     check_receipt='gfs')

    # TODO (G): Giving up for now on trying to mock the receipt time, will revisit later
    assert len(valid_files) == 7
