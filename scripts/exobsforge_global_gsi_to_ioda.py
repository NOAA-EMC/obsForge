#!/usr/bin/env python3
# exobsforge_global_gsi_to_ioda.py
# This script will collect and preprocess
# observations and bias correction files
# in the GSI output formats and convert
# them for use by JEDI
# - IODA files for observations
# - UFO readable netCDF files for bias correction
import os

from wxflow import Logger, cast_strdict_as_dtypedict
from pyobsforge.task.gsi_to_ioda import GsiToIoda

# Initialize root logger
logger = Logger(level='DEBUG', colored_log=True)


if __name__ == '__main__':

    # Take configuration from environment and cast it as python dictionary
    config = cast_strdict_as_dtypedict(os.environ)

    # Instantiate the task
    GsiToIodaTask = GsiToIoda(config)

    # Convert GSI diag files to IODA files
    GsiToIodaTask.convert_gsi_diags()

    # Convert bias correction files
    GsiToIodaTask.convert_bias_correction_files()
