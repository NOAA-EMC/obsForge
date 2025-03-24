#!/usr/bin/env python3
# exobsforge_global_aod_dump.py
# This script will collect and preprocess
# aerosol optical depth observations for
# global aerosol assimilation 
import os

from wxflow import AttrDict, Logger, cast_strdict_as_dtypedict, parse_j2yaml
from pyobsforge.task.aero_prepobs import AerosolObsPrep

# Initialize root logger
logger = Logger(level='DEBUG', colored_log=True)


if __name__ == '__main__':

    # Take configuration from environment and cast it as python dictionary
    config_env = cast_strdict_as_dtypedict(os.environ)
    # Take configuration from YAML file to augment/append config dict
    config_yaml = parse_j2yaml(os.path.join(config_env['HOMEobsforge'], 'parm', 'config.yaml'), config_env)
    # Combine configs together
    config = AttrDict(**config_env, **config_yaml['obsforge'])
    config = AttrDict(**config, **config_yaml['aoddump'])

    AeroObs = AerosolObsPrep(config)
    AeroObs.initialize()
    AeroObs.runConverter()
    AeroObs.finalize()
