#!/usr/bin/env python3
# exobsforge_global_marine_bufr_dump.py
# This script will collect and preprocess
# the ocean and seaice observations for
# global marine assimilation
import os

from wxflow import AttrDict, Logger, cast_strdict_as_dtypedict, parse_j2yaml
from pyobsforge.task.marine_bufr_prepobs import MarineBufrObsPrep

# Initialize root logger
logger = Logger(level='DEBUG', colored_log=True)


if __name__ == '__main__':

    # Take configuration from environment and cast it as python dictionary
    config_env = cast_strdict_as_dtypedict(os.environ)

    # Take configuration from YAML file to augment/append config dict
    config_yaml = parse_j2yaml(os.path.join(config_env['HOMEobsforge'], 'parm', 'config.yaml'), config_env)
    # Extract obsforge specific configuration
    obsforge_dict = {}
    for key, value in config_yaml['obsforge'].items():
        if key not in config_env.keys():
            obsforge_dict[key] = value

    # Combine configs together
    config = AttrDict(**config_env, **obsforge_dict)
    config = AttrDict(**config, **config_yaml['marinebufrdump'])

    marineBufrObs = MarineBufrObsPrep(config)
    marineBufrObs.initialize()
    marineBufrObs.execute()
    marineBufrObs.finalize()
