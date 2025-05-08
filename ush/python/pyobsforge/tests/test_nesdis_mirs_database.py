import os
import glob
import tempfile
import shutil
import sqlite3
from datetime import datetime, timedelta

import pytest

from pyobsforge.obsdb.nesdis_mirs_db import NesdisMirsDatabase  # Adjust as needed


@pytest.fixture
def temp_obs_dir():
    """Create a temp directory with mock NESDIS MIRS NetCDF files sorted by satellite."""
    base_dir = tempfile.mkdtemp()

    # Folder mapping from satellite name in filename
    sat_folder_map = {
        "ma1": "seaice_amsu",
        "n20": "seaice_atms_j1",
        "n21": "seaice_atms_j2",
        "npp": "seaice_atms_snpp",
        "gpm": "seaice_mirs"
    }

    # Create all needed subdirectories
    for folder in sat_folder_map.values():
        os.makedirs(os.path.join(base_dir, "some_subdir", folder), exist_ok=True)

    mock_time = datetime(2025, 4, 30, 6, 0, 0).timestamp()

    # List of mock files
    filenames = [
        "NPR-MIRS-IMG_v11r9_ma1_s202504300706550_e202504300756360_c202504300838450.nc",
        "NPR-MIRS-IMG_v11r9_ma1_s202504300752070_e202504300847560_c202504300922220.nc",
        "NPR-MIRS-IMG_v11r9_ma1_s202504300847510_e202504300937400_c202504301019390.nc",
        "NPR-MIRS-IMG_v11r9_ma1_s202504300933110_e202504301028440_c202504301105480.nc",
        "NPR-MIRS-IMG_v11r9_ma1_s202504301028550_e202504301118440_c202504301201320.nc",
        "NPR-MIRS-IMG_v11r9_ma1_s202504301114230_e202504301208280_c202504301243330.nc",
        "NPR-MIRS-IMG_v11r9_n20_s202504300858350_e202504300859066_c202504300933000.nc",
        "NPR-MIRS-IMG_v11r9_n20_s202504300859070_e202504300859386_c202504300931380.nc",
        "NPR-MIRS-IMG_v11r9_n20_s202504300859390_e202504300900106_c202504300932300.nc",
        "NPR-MIRS-IMG_v11r9_n20_s202504300900110_e202504300900426_c202504300931530.nc",
        "NPR-MIRS-IMG_v11r9_n20_s202504300900430_e202504300901146_c202504300933000.nc",
        "NPR-MIRS-IMG_v11r9_n20_s202504300901150_e202504300901466_c202504300932000.nc",
        "NPR-MIRS-IMG_v11r9_n21_s202504300858324_e202504300859040_c202504300935130.nc",
        "NPR-MIRS-IMG_v11r9_n21_s202504300859044_e202504300859360_c202504300934410.nc",
        "NPR-MIRS-IMG_v11r9_n21_s202504300859364_e202504300900080_c202504300934330.nc",
        "NPR-MIRS-IMG_v11r9_n21_s202504300900084_e202504300900400_c202504300933390.nc",
        "NPR-MIRS-IMG_v11r9_n21_s202504300900404_e202504300901120_c202504300933450.nc",
        "NPR-MIRS-IMG_v11r9_n21_s202504300901124_e202504300901440_c202504300934590.nc",
        "NPR-MIRS-IMG_v11r9_npp_s202504300858336_e202504300859053_c202504300916400.nc",
        "NPR-MIRS-IMG_v11r9_npp_s202504300859056_e202504300859373_c202504300916500.nc",
        "NPR-MIRS-IMG_v11r9_npp_s202504300859376_e202504300900093_c202504300916510.nc",
        "NPR-MIRS-IMG_v11r9_npp_s202504300900096_e202504300900413_c202504300917320.nc",
        "NPR-MIRS-IMG_v11r9_npp_s202504300900416_e202504300901133_c202504300917350.nc",
        "NPR-MIRS-IMG_v11r9_npp_s202504300901136_e202504300901453_c202504301103340.nc",
        "NPR-MIRS-IMG_v11r9_gpm_s202504300848270_e202504300853250_c202504300912100.nc",
        "NPR-MIRS-IMG_v11r9_gpm_s202504300853270_e202504300858250_c202504300918440.nc",
        "NPR-MIRS-IMG_v11r9_gpm_s202504300858270_e202504300903250_c202504300924230.nc",
        "NPR-MIRS-IMG_v11r9_gpm_s202504300903270_e202504300908250_c202504300935120.nc",
        "NPR-MIRS-IMG_v11r9_gpm_s202504300908270_e202504300913250_c202504300936130.nc",
        "NPR-MIRS-IMG_v11r9_gpm_s202504300913270_e202504300918250_c202504300940510.nc"
        "invalid_file.nc"
    ]

    # Create valid files in correct subdirs
    for fname in filenames:
        try:
            # Extract satellite identifier from filename
            sat = fname.split("_")[2]
            folder = sat_folder_map.get(sat)

            if folder is None:
                print(f"[WARNING] Skipping unrecognized satellite in: {fname}")
                continue

            # Create full path
            path = os.path.join(base_dir, "some_subdir", folder, fname)
            with open(path, "w") as f:
                f.write("fake content")
            os.utime(path, (mock_time, mock_time))
        except IndexError:
            print(f"[ERROR] Failed to parse satellite from filename: {fname}")
            continue

        for folder in sat_folder_map.values():
            invalid_path = os.path.join(base_dir, "some_subdir", folder, "invalid_file.nc")
            with open(invalid_path, "w") as f:
                f.write("invalid content")
            os.utime(invalid_path, (mock_time, mock_time))

    yield base_dir
    shutil.rmtree(base_dir)


@pytest.fixture
def db(temp_obs_dir):
    """Initialize test database."""
    db_path = os.path.join(temp_obs_dir, "nesdis_mirs_test.db")

    # List of seaice-related subfolders to include
    obs_dirs = [
        "seaice_amsu",
        "seaice_atms_j1",
        "seaice_atms_j2",
        "seaice_atms_snpp",
        "seaice_mirs"
    ]

    database = NesdisMirsDatabase(
        db_name=db_path,
        dcom_dir=temp_obs_dir,
        obs_dirs=obs_dirs  # Pass list of directories
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
    fname = "NPR-MIRS-IMG_v11r9_n21_s202504300858324_e202504300859040_c202504300935130.nc"

    # Search through all base_dir entries
    found_files = []
    for base in db.base_dir:
        matches = glob.glob(os.path.join(base, fname))
        if matches:
            found_files.extend(matches)

    assert found_files, f"{fname} not found in any db.base_dir paths"
    fname = found_files[0]

    # Parse filename
    parsed = db.parse_filename(fname)
    creation_time = datetime.fromtimestamp(os.path.getctime(fname))

    # Assertions
    assert parsed is not None
    assert parsed[0] == fname
    assert parsed[1] == datetime(2025, 4, 30, 8, 58, 32)
    assert parsed[2] == creation_time
    assert parsed[3] == "MIRS"
    assert parsed[4] == "n21"
    assert parsed[5] == "icec_atms_n21_l2"


def test_parse_invalid_filename(db):
    assert db.parse_filename("junk.nc") is None
    assert db.parse_filename("NPR-MIRS-IMG_v11r9_n21_invalid.nc") is None


def test_ingest_files(db):
    db.ingest_files()

    # Debug: show number of files discovered for ingestion
    total_files = 0
    for base in db.base_dir:
        matched = glob.glob(os.path.join(base, "*.nc"))
        print(f"[DEBUG] {len(matched)} files found in {base}")
        total_files += len(matched)
    print(f"[DEBUG] Total NetCDF files found for ingestion: {total_files}")

    # Validate records written to database
    conn = sqlite3.connect(db.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM obs_files")
    count = cursor.fetchone()[0]
    conn.close()

    assert count == 30, "Should ingest 30 valid MIRS files"


def test_get_valid_files(db):
    db.ingest_files()
    da_cycle = "20250430060000"
    window_begin = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") - timedelta(hours=3)
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)
    dst_dir = 'icec'
    # Test for MIRS ICEC
    valid_files_ma1 = db.get_valid_files(window_begin=window_begin,
                                         window_end=window_end,
                                         dst_dir=dst_dir,
                                         instrument="MIRS",
                                         satellite="ma1",
                                         obs_type="icec_amsu_ma1_l2")

    valid_files_n20 = db.get_valid_files(window_begin=window_begin,
                                         window_end=window_end,
                                         dst_dir=dst_dir,
                                         instrument="MIRS",
                                         satellite="n20",
                                         obs_type="icec_atms_n20_l2")

    valid_files_n21 = db.get_valid_files(window_begin=window_begin,
                                         window_end=window_end,
                                         dst_dir=dst_dir,
                                         instrument="MIRS",
                                         satellite="n21",
                                         obs_type="icec_atms_n21_l2")

    valid_files_npp = db.get_valid_files(window_begin=window_begin,
                                         window_end=window_end,
                                         dst_dir=dst_dir,
                                         instrument="MIRS",
                                         satellite="npp",
                                         obs_type="icec_atms_npp_l2")

    valid_files_gpm = db.get_valid_files(window_begin=window_begin,
                                         window_end=window_end,
                                         dst_dir=dst_dir,
                                         instrument="MIRS",
                                         satellite="gpm",
                                         obs_type="icec_gmi_gpm_l2")

    valid_files = (
        valid_files_ma1
        + valid_files_n20
        + valid_files_n21
        + valid_files_npp
        + valid_files_gpm
    )

    # Files at 10:00 and 12:00 are within +/- 3h of 00:00
    assert any("202504300706" in f for f in valid_files)
    assert any("202504300859" in f for f in valid_files)
    assert any("202504300900" in f for f in valid_files)
    assert any("202504300853" in f for f in valid_files)
    assert any("202504300900" not in f for f in valid_files)
    assert all("202504300901" not in f for f in valid_files)

    print("Valid files found:", len(valid_files))
    for f in valid_files:
        print(" -", f)
    assert len(valid_files) == 15


def test_get_valid_files_receipt(db):
    db.ingest_files()
    da_cycle = "20250430060000"
    window_begin = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") - timedelta(hours=3)
    window_end = datetime.strptime(da_cycle, "%Y%m%d%H%M%S") + timedelta(hours=3)
    dst_dir = 'icec'

    # Test for MIRS ICEC
    valid_files_ma1 = db.get_valid_files(window_begin=window_begin,
                                         window_end=window_end,
                                         dst_dir=dst_dir,
                                         instrument="MIRS",
                                         satellite="ma1",
                                         obs_type="icec_amsu_ma1_l2",
                                         check_receipt="gfs")

    valid_files_n20 = db.get_valid_files(window_begin=window_begin,
                                         window_end=window_end,
                                         dst_dir=dst_dir,
                                         instrument="MIRS",
                                         satellite="n20",
                                         obs_type="icec_atms_n20_l2",
                                         check_receipt="gfs")

    valid_files_n21 = db.get_valid_files(window_begin=window_begin,
                                         window_end=window_end,
                                         dst_dir=dst_dir,
                                         instrument="MIRS",
                                         satellite="n21",
                                         obs_type="icec_atms_n21_l2",
                                         check_receipt="gfs")

    valid_files_npp = db.get_valid_files(window_begin=window_begin,
                                         window_end=window_end,
                                         dst_dir=dst_dir,
                                         instrument="MIRS",
                                         satellite="npp",
                                         obs_type="icec_atms_npp_l2",
                                         check_receipt="gfs")

    valid_files_gpm = db.get_valid_files(window_begin=window_begin,
                                         window_end=window_end,
                                         dst_dir=dst_dir,
                                         instrument="MIRS",
                                         satellite="gpm",
                                         obs_type="icec_gmi_gpm_l2",
                                         check_receipt="gfs")

    valid_files = (
        valid_files_ma1
        + valid_files_n20
        + valid_files_n21
        + valid_files_npp
        + valid_files_gpm
    )

    print("Valid files found:", len(valid_files))
    for f in valid_files:
        print(" -", f)

    # TODO (G): Giving up for now on trying to mock the receipt time, will revisit later
    assert len(valid_files) == 15
