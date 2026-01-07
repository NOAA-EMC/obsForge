#!/usr/bin/env python3
# exobsforge_global_gsi_to_ioda.py
# This script will collect and preprocess
# observations and bias correction files
# in the GSI output formats and convert
# them for use by JEDI
# - IODA files for observations
# - UFO readable netCDF files for bias correction
import os

from wxflow import AttrDict, Logger, cast_strdict_as_dtypedict, parse_j2yaml
from pyobsforge.task.gsi_to_ioda import GsiToIoda

# Initialize root logger
logger = Logger(level='DEBUG', colored_log=True)


if __name__ == '__main__':

    # Take configuration from environment and cast it as python dictionary
    config_env = cast_strdict_as_dtypedict(os.environ)
    # Take configuration from YAML file to augment/append config dict
    config_yaml = parse_j2yaml(os.path.join(config_env['HOMEobsforge'], 'parm', 'config.yaml'), config_env)
    # ensure we are not duplicating keys between the environment and the YAML config
    obsforge_dict = {}
    for key, value in config_yaml['obsforge'].items():
        if key not in config_env.keys():
            obsforge_dict[key] = value

    # Combine configs together
    config = AttrDict(**config_env, **obsforge_dict)

    # Instantiate the task
    GsiToIodaTask = GsiToIoda(config)

    # Convert GSI diag files to IODA files
    GsiToIodaTask.convert_gsi_diags()

    # Convert bias correction files
    GsiToIodaTask.convert_bias_correction_files()
