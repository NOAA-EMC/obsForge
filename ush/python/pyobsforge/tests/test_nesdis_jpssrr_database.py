import os
import glob
import tempfile
import shutil
import sqlite3
from datetime import datetime, timedelta

import pytest

from pyobsforge.obsdb.nesdis_jpssrr_db import NesdisJpssrrDatabase  # Adjust as needed


@pytest.fixture
def temp_obs_dir():
    """Create a temp directory with mock NESDIS JPSSRR NetCDF files sorted by satellite."""
    base_dir = tempfile.mkdtemp()
    sub_dir = os.path.join(base_dir, "some_subdir", "wgrdbul", "IST")
    os.makedirs(sub_dir)

    # Desired datetime for file timestamps
    mock_time = datetime(2025, 6, 1, 6, 0, 0).timestamp()

    # List of mock files
    filenames = [
        "JRR-IceConcentration_v3r3_j01_s202506010556296_e202506010557541_c202506010638413.nc",
    	"JRR-IceConcentration_v3r3_j01_s202506010557554_e202506010559199_c202506010646457.nc",
	"JRR-IceConcentration_v3r3_j01_s202506010559211_e202506010600456_c202506010634437.nc",
	"JRR-IceConcentration_v3r3_j01_s202506010600469_e202506010602114_c202506010634216.nc",
	"JRR-IceConcentration_v3r3_j01_s202506010602126_e202506010603354_c202506010634530.nc",
	"JRR-IceConcentration_v3r3_j01_s202506010914143_e202506010915389_c202506010950105.nc",
	"JRR-IceConcentration_v3r3_j01_s202506010914143_e202506010915389_c202506010950105.nc",
	"JRR-IceConcentration_v3r3_n21_s202506010556308_e202506010557537_c202506010636302.nc",
	"JRR-IceConcentration_v3r3_n21_s202506010557549_e202506010559196_c202506010647002.nc",
	"JRR-IceConcentration_v3r3_n21_s202506010559208_e202506010600455_c202506010638557.nc",
	"JRR-IceConcentration_v3r3_n21_s202506010600468_e202506010602114_c202506010644446.nc",
	"JRR-IceConcentration_v3r3_n21_s202506010602127_e202506010603355_c202506010638205.nc",
	"JRR-IceConcentration_v3r3_n21_s202506010914132_e202506010915378_c202506010954436.nc",
	"JRR-IceConcentration_v3r3_n21_s202506010915391_e202506010917037_c202506011004299.nc",
	"JRR-IceConcentration_v3r3_npp_s202506010556308_e202506010557550_c202506010741499.nc",
	"JRR-IceConcentration_v3r3_npp_s202506010557563_e202506010559204_c202506010742588.nc",
	"JRR-IceConcentration_v3r3_npp_s202506010559217_e202506010600458_c202506010743198.nc",
	"JRR-IceConcentration_v3r3_npp_s202506010600471_e202506010602112_c202506010742342.nc",
	"JRR-IceConcentration_v3r3_npp_s202506010602125_e202506010603366_c202506010742378.nc",
	"JRR-IceConcentration_v3r3_npp_s202506010922457_e202506010924098_c202506011106145.nc",
        "JRR-IceConcentration_v3r3_npp_s202506010924111_e202506010925352_c202506011106490.nc",
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
    db_path = os.path.join(temp_obs_dir, "nesdis_jpssrr_test.db")
    return NesdisJpssrrDatabase(db_name=db_path, dcom_dir=temp_obs_dir, obs_dir="wgrdbul/IST")


def test_create_database(db):
    db.create_database()
    conn = sqlite3.connect(db.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='obs_files'")
    assert cursor.fetchone() is not None
    conn.close()


def test_parse_valid_filename(db):
    fname = "JRR-IceConcentration_v3r3_npp_s202506010602125_e202506010603366_c202506010742378.nc"
    fname = glob.glob(os.path.join(db.base_dir, fname))[0]
    parsed = db.parse_filename(fname)
    creation_time = datetime.fromtimestamp(os.path.getctime(fname))

    # Assertions
    assert parsed is not None
    assert parsed[0] == fname
    assert parsed[1] == datetime(2025, 6, 1, 6, 2, 12)
    assert parsed[2] == creation_time
    assert parsed[3] == "npp"


def test_parse_invalid_filename(db):
    assert db.parse_filename("junk.nc") is None
    assert db.parse_filename("JRR-IceConcentration_v3r3_npp_invalid.nc") is None


def test_ingest_files(db):
    db.ingest_files()
    conn = sqlite3.connect(db.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM obs_files")
    count = cursor.fetchone()[0]
    conn.close()
    assert count == 20, "Should ingest 20 valid JRR files"


def test_get_valid_files(db):
    db.ingest_files()
    da_cycle = "20250601060000"
    window_begin = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") - timedelta(hours=3)
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)
    dst_dir = 'icec'
    # Test for MIRS ICEC
    valid_files_j01 = db.get_valid_files(window_begin=window_begin,
                                         window_end=window_end,
                                         dst_dir=dst_dir,
                                         satellite="j01")

    valid_files_n21 = db.get_valid_files(window_begin=window_begin,
                                         window_end=window_end,
                                         dst_dir=dst_dir,
                                         satellite="n21")

    valid_files_npp = db.get_valid_files(window_begin=window_begin,
                                         window_end=window_end,
                                         dst_dir=dst_dir,
                                         satellite="npp")

    valid_files = (
        valid_files_j01
        + valid_files_n21
        + valid_files_npp
    )

    # Files at 10:00 and 12:00 are within +/- 3h of 00:00
    assert any("202506010557" in f for f in valid_files)
    assert any("202506010600" in f for f in valid_files)
    assert any("202506010602" in f for f in valid_files)
    assert any("202506010559" in f for f in valid_files)
    assert any("202506010924" not in f for f in valid_files)
    assert all("202506010914" not in f for f in valid_files)

    print("Valid files found:", len(valid_files))
    for f in valid_files:
        print(" -", f)
    assert len(valid_files) == 15


def test_get_valid_files_receipt(db):
    db.ingest_files()
    da_cycle = "20250601060000"
    window_begin = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") - timedelta(hours=3)
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)
    dst_dir = 'icec'

    # Test for MIRS ICEC
    valid_files_j01 = db.get_valid_files(window_begin=window_begin,
                                         window_end=window_end,
                                         dst_dir=dst_dir,
                                         satellite="j01",
                                         check_receipt="gfs")

    valid_files_n21 = db.get_valid_files(window_begin=window_begin,
                                         window_end=window_end,
                                         dst_dir=dst_dir,
                                         satellite="n21",
                                         check_receipt="gfs")

    valid_files_npp = db.get_valid_files(window_begin=window_begin,
                                         window_end=window_end,
                                         dst_dir=dst_dir,
                                         satellite="npp",
                                         check_receipt="gfs")

    valid_files = (
        valid_files_j01
        + valid_files_n21
        + valid_files_npp
    )

    print("Valid files found:", len(valid_files))
    for f in valid_files:
        print(" -", f)

    # TODO (G): Giving up for now on trying to mock the receipt time, will revisit later
    assert len(valid_files) == 15
