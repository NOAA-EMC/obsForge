import os
import glob
import tempfile
import shutil
import sqlite3
from datetime import datetime, timedelta

import pytest

from pyobsforge.obsdb.nesdis_amsr3_db import NesdisAmsr3Database  # Adjust as needed


@pytest.fixture
def temp_obs_dir():
    """Create a temp directory with mock NESDIS AMSR3 NetCDF files."""
    base_dir = tempfile.mkdtemp()
    sub_dir = os.path.join(base_dir, "some_subdir", "seaice/pda")
    os.makedirs(sub_dir)

    # Desired datetime for file timestamps
    mock_time = datetime(2026, 6, 25, 0, 0, 0).timestamp()

    # Create mock NetCDF files
    filenames = [
        "AMSR3-SEAICE-NH_v1r0_ggw_s202606250117246_e202606260327311_c202606260345590.nc",
        "AMSR3-SEAICE-NH_v1r0_ggw_s202606250255070_e202606260504285_c202606260524550.nc",
        "AMSR3-SEAICE-NH_v1r0_ggw_s202606250432134_e202606260641184_c202606260655260.nc",
        "AMSR3-SEAICE-NH_v1r0_ggw_s202606250609033_e202606260808203_c202606260831280.nc",
        "AMSR3-SEAICE-NH_v1r0_ggw_s202606250745562_e202606260955282_c202606261011340.nc",
        "AMSR3-SEAICE-NH_v1r0_ggw_s202606250923041_e202606261132526_c202606261154430.nc",
        "AMSR3-SEAICE-NH_v1r0_ggw_s202606251100255_e202606261310290_c202606261325490.nc",
        "AMSR3-SEAICE-NH_v1r0_ggw_s202606251237589_e202606261448159_c202606261511350.nc",
        "AMSR3-SEAICE-NH_v1r0_ggw_s202606251415413_e202606261626149_c202606261649310.nc",
        "AMSR3-SEAICE-NH_v1r0_ggw_s202606251553357_e202606261804542_c202606261829470.nc",
        "AMSR3-SEAICE-NH_v1r0_ggw_s202606251731541_e202606261945082_c202606262002280.nc",
        "AMSR3-SEAICE-NH_v1r0_ggw_s202606251911366_e202606262125461_c202606262157510.nc",
        "AMSR3-SEAICE-NH_v1r0_ggw_s202606252052190_e202606262305435_c202606262328180.nc",
        "AMSR3-SEAICE-NH_v1r0_ggw_s202606252232344_e202606270044379_c202606270107430.nc",
        "AMSR3-SEAICE-SH_v1r0_ggw_s202606250117246_e202606260327311_c202606260345590.nc",
        "AMSR3-SEAICE-SH_v1r0_ggw_s202606250255070_e202606260504285_c202606260524550.nc",
        "AMSR3-SEAICE-SH_v1r0_ggw_s202606250432134_e202606260641184_c202606260655260.nc",
        "AMSR3-SEAICE-SH_v1r0_ggw_s202606250609033_e202606260808203_c202606260831280.nc",
        "AMSR3-SEAICE-SH_v1r0_ggw_s202606250745562_e202606260955282_c202606261011340.nc",
        "AMSR3-SEAICE-SH_v1r0_ggw_s202606250923041_e202606261132526_c202606261154430.nc",
        "AMSR3-SEAICE-SH_v1r0_ggw_s202606251100255_e202606261310290_c202606261325490.nc",
        "AMSR3-SEAICE-SH_v1r0_ggw_s202606251237589_e202606261448159_c202606261511350.nc",
        "AMSR3-SEAICE-SH_v1r0_ggw_s202606251415413_e202606261626149_c202606261649310.nc",
        "AMSR3-SEAICE-SH_v1r0_ggw_s202606251553357_e202606261804542_c202606261829470.nc",
        "AMSR3-SEAICE-SH_v1r0_ggw_s202606251731541_e202606261945082_c202606262002280.nc",
        "AMSR3-SEAICE-SH_v1r0_ggw_s202606251911366_e202606262125461_c202606262157510.nc",
        "AMSR3-SEAICE-SH_v1r0_ggw_s202606252052190_e202606262305435_c202606262328180.nc",
        "AMSR3-SEAICE-SH_v1r0_ggw_s202606252232344_e202606270044379_c202606270107430.nc",
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
    db_path = os.path.join(temp_obs_dir, "nesdis_amsr3_test.db")
    database = NesdisAmsr3Database(
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
    fname = "AMSR3-SEAICE-NH_v1r0_ggw_s202606250117246_e202606260327311_c202606260345590.nc"
    fname = glob.glob(os.path.join(db.base_dir, fname))[0]
    parsed = db.parse_filename(fname)
    creation_time = datetime.fromtimestamp(os.path.getctime(fname))

    assert parsed is not None
    assert parsed[0] == fname
    assert parsed[1] == datetime(2026, 6, 25, 1, 17, 24)
    assert parsed[2] == creation_time
    assert parsed[3] == "AMSR3"
    assert parsed[4] == "ggw"
    # assert parsed[5] == "SEAICE"
    assert parsed[5] == "icec_amsr3_north"


def test_parse_invalid_filename(db):
    assert db.parse_filename("junk.nc") is None
    assert db.parse_filename("AMSR3-SEAICE-NH_v1r0_ggw_invalid.nc") is None


def test_ingest_files(db):
    db.ingest_files()
    conn = sqlite3.connect(db.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM obs_files")
    count = cursor.fetchone()[0]
    conn.close()
    assert count == 28, "Should ingest 28 valid AMSR3 files"


def test_get_valid_files(db):
    db.ingest_files()
    da_cycle = "20260625060000"
    window_begin = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") - timedelta(hours=3)
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)
    dst_dir = 'icec'
    # Test for AMSR3 ICEC
    valid_files_north = db.get_valid_files(window_begin=window_begin,
                                           window_end=window_end,
                                           dst_dir=dst_dir,
                                           instrument="AMSR3",
                                           satellite="ggw",
                                           obs_type="icec_amsr2_north")

    valid_files_south = db.get_valid_files(window_begin=window_begin,
                                           window_end=window_end,
                                           dst_dir=dst_dir,
                                           instrument="AMSR3",
                                           satellite="ggw",
                                           obs_type="icec_amsr2_south")

    valid_files = valid_files_north + valid_files_south

    # Files at 10:00 and 12:00 are within +/- 3h of 00:00
    assert any("202606250609" in f for f in valid_files)
    assert any("202606250745" in f for f in valid_files)
    assert all("202606250923" not in f for f in valid_files)

    print("Valid files found:", len(valid_files))
    for f in valid_files:
        print(" -", f)
    assert len(valid_files) == 6


def test_get_valid_files_receipt(db):
    db.ingest_files()
    da_cycle = "20260625060000"
    window_begin = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") - timedelta(hours=3)
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)
    dst_dir = 'icec'

    # Test for AMSR3 ICEC
    valid_files_north = db.get_valid_files(window_begin=window_begin,
                                           window_end=window_end,
                                           dst_dir=dst_dir,
                                           instrument="AMSR3",
                                           satellite="ggw",
                                           obs_type="icec_amsr2_north",
                                           check_receipt="gfs")

    valid_files_south = db.get_valid_files(window_begin=window_begin,
                                           window_end=window_end,
                                           dst_dir=dst_dir,
                                           instrument="AMSR3",
                                           satellite="ggw",
                                           obs_type="icec_amsr2_south",
                                           check_receipt="gfs")

    valid_files = valid_files_north + valid_files_south

    print("Valid files found:", len(valid_files))
    for f in valid_files:
        print(" -", f)

    # TODO (G): Giving up for now on trying to mock the receipt time, will revisit later
    assert len(valid_files) == 6
