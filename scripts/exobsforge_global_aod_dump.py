#!/usr/bin/env python3
# exobsforge_global_aod_dump.py
# This script will collect and preprocess
# aerosol optical depth observations for
# global aerosol assimilation 
import os

from wxflow import Logger, cast_strdict_as_dtypedict
from pyobsforge.task.aero_prepobs import AerosolObsPrep

# Initialize root logger
logger = Logger(level='DEBUG', colored_log=True)


if __name__ == '__main__':

    # Take configuration from environment and cast it as python dictionary
    config = cast_strdict_as_dtypedict(os.environ)

    AeroObs = AerosolObsPrep(config)
    AeroObs.initialize()
    AeroObs.runConverter()
    AeroObs.finalize()
