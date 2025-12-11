#!/usr/bin/env python3

# import logging
import os
import re

from netCDF4 import Dataset

# logger = logging.getLogger(__name__)


def read_number_of_ioda_obs(ncfile):
    """
    Returns the number of observations in an IODA-style NetCDF file.
    Specifically, it reads the 'Location' dimension.
    """

    if not os.path.isfile(ncfile):
        raise FileNotFoundError(f"File not found: {ncfile}")

    with Dataset(ncfile, "r") as ds:
        if "Location" not in ds.dimensions:
            raise KeyError("The NetCDF file does not contain a 'Location' dimension.")
        return len(ds.dimensions["Location"])


def check_ioda_structure(ncfile):
    """
    Performs a stricter structural validation of an IODA-style NetCDF file.

    Requirements enforced:
      - Dimension "Location" must exist.
      - Groups MetaData, ObsValue, ObsError, PreQC must exist.
      - MetaData must contain variables: dateTime, latitude, longitude.
      - For each variable in ObsValue, the same variable must exist in ObsError and PreQC.

    Raises:
      ValueError or KeyError if structure is invalid.
    """

    if not os.path.isfile(ncfile):
        raise FileNotFoundError(f"File not found: {ncfile}")

    required_groups = ["MetaData", "ObsValue", "ObsError", "PreQC"]
    required_metadata_vars = ["dateTime", "latitude", "longitude"]

    try:
        with Dataset(ncfile, "r") as ds:

            # ----------------------------
            # 1. Check "Location" dimension
            # ----------------------------
            if "Location" not in ds.dimensions:
                raise KeyError(f"{ncfile} missing required 'Location' dimension")

            # ----------------------------
            # 2. Check required groups
            # ----------------------------
            for grp in required_groups:
                if grp not in ds.groups:
                    raise KeyError(f"{ncfile} missing required group: '{grp}'")

            # Shortcuts to group objects
            g_meta = ds.groups["MetaData"]
            g_val = ds.groups["ObsValue"]
            g_err = ds.groups["ObsError"]
            g_pqc = ds.groups["PreQC"]

            # ----------------------------
            # 3. Check required MetaData fields
            # ----------------------------
            for var in required_metadata_vars:
                if var not in g_meta.variables:
                    raise KeyError(
                        f"{ncfile}: MetaData group missing required variable '{var}'"
                    )

            # ----------------------------
            # 4. Consistency of variables across ObsValue, ObsError, PreQC
            # ----------------------------
            obsvalue_vars = set(g_val.variables.keys())
            obserror_vars = set(g_err.variables.keys())
            preqc_vars = set(g_pqc.variables.keys())

            for var in obsvalue_vars:
                missing = []
                if var not in obserror_vars:
                    missing.append("ObsError")
                if var not in preqc_vars:
                    missing.append("PreQC")

                if missing:
                    raise ValueError(
                        f"{ncfile}: variable '{var}' is in ObsValue "
                        f"but missing from: {', '.join(missing)}"
                    )

            # If all checks passed:
            return True

    except OSError as e:
        raise ValueError(f"Cannot open NetCDF file {ncfile}: {e}") from e


def get_obs_space_from_filename(filename: str):
    """
    Extracts the obs-space name from filenames of the form:
        gdas.t00z.aircraft.nc
        gfs.t18z.satwind.nc

    Returns:
        obs_space (str)

    Raises:
        ValueError on format errors or inconsistencies.
    """

    base = os.path.basename(filename)

    # Pattern:
    #   RUN  . tHH z .  OBSSPACE . nc
    #   gdas.t00z.aircraft.nc
    pattern = re.compile(
        r"^(gdas|gfs)\.t([0-9]{2})z\.([A-Za-z0-9_\-]+)\.nc$",
        re.IGNORECASE
    )

    m = pattern.match(base)
    if not m:
        raise ValueError(f"Filename does not match expected pattern: {filename}")

    # run_type = m.group(1).lower()
    cyc = m.group(2)
    obs_space = m.group(3)

    # Optional validation (can be removed if not wanted)
    valid_cycles = {"00", "06", "12", "18"}
    if cyc not in valid_cycles:
        raise ValueError(f"Invalid cycle hour '{cyc}' in filename: {filename}")

    return obs_space


def get_dir_obs_spaces(dir_name: str):
    """
    Scans a directory and returns a dict mapping:
        obs_space_name -> filename

    Only files matching the pattern for obs-space output files
    (as parsed by get_obs_space_from_filename) are included.

    Example return value:
        {
it will construct the collection of all the obs spaces which will get the name obs_type            "aircraft": "gdas.t00z.aircraft.nc",
            "satwnd"  : "gdas.t00z.satwnd.nc",
        }

    Raises:
        FileNotFoundError: directory does not exist
        ValueError: if multiple files correspond to the same obs space
        ValueError: if an invalid obs-space filename is encountered
    """

    if not os.path.isdir(dir_name):
        raise FileNotFoundError(f"Directory does not exist: {dir_name}")

    obs_map = {}

    for fname in os.listdir(dir_name):
        fullpath = os.path.join(dir_name, fname)

        # Skip directories
        if os.path.isdir(fullpath):
            continue

        # Try extracting obs space
        try:
            obs_space = get_obs_space_from_filename(fname)
        except ValueError:
            # Not an obs-space file → ignore
            continue

        # Ensure uniqueness
        if obs_space in obs_map:
            raise ValueError(
                f"Multiple files found for obs space '{obs_space}': "
                f"{obs_map[obs_space]} and {fname}"
            )

        obs_map[obs_space] = fname

    return obs_map


def print_obs_space_description(obs_type, obs_space, info):
    """
    Placeholder logging function.
    Instead of writing to a database, it prints all information nicely.
    """

    print("============================================")
    print("IODA Observation File Processed")
    print("--------------------------------------------")
    print(f"  Observation Type : {obs_type}")
    print(f"  Obs Space        : {obs_space}")
    print(f"  File Name        : {info['filename']}")
    print(f"  Number of Obs    : {info['n_obs']}")
    print("============================================\n")


def parse_obs_dir(obs_type, obs_dir):
    """
    Processes all obs-space files in a directory:

      - Finds obs-space files
      - Validates IODA structure
      - Reads number of observations

    Returns:
        dict: { obs_space : { filename, n_obs } }
    """

    obs_map = get_dir_obs_spaces(obs_dir)
    results = {}

    for obs_space, filename in obs_map.items():
        fullpath = os.path.join(obs_dir, filename)

        # 1. Structural validation
        try:
            check_ioda_structure(fullpath)
        except Exception as e:
            raise ValueError(
                f"Obs-space '{obs_space}' failed IODA structural validation: {e}"
            ) from e

        # 2. Read number of observations
        try:
            nobs = read_number_of_ioda_obs(fullpath)
        except Exception as e:
            raise ValueError(
                f"Obs-space '{obs_space}' failed to read number of obs: {e}"
            ) from e

        # 3. debug print
        # print_obs_space_description(obs_type, obs_space, filename, nobs)

        results[obs_space] = {
            "filename": filename,
            "n_obs": nobs,
        }

    return results


def detect_categories(obs_root: str) -> dict:
    """
    Auto-discover categories under an obs_root directory.

    Example layout:
        /.../ocean/
           sst/
           icec/
           insitu/

    Returns:
        {"sst": "/.../ocean/sst", ...}
    """

    if not os.path.isdir(obs_root):
        return {}

    categories = {}
    for name in sorted(os.listdir(obs_root)):
        full = os.path.join(obs_root, name)
        if os.path.isdir(full):
            categories[name] = full

    return categories
