#!/usr/bin/env python3

import glob
import os
from logging import getLogger
from typing import Dict, Any

from wxflow import (AttrDict, Task, add_to_datetime, to_timedelta,
                    logit, FileHandler, Executable, YAMLFile)
import pathlib

logger = getLogger(__name__.split('.')[-1])


class AtmosBufrObsPrep(Task):
    """
    Class for preparing and managing atmospheric BUFR observations
    """
    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        _window_begin = add_to_datetime(self.task_config.current_cycle, -to_timedelta(f"{self.task_config['assim_freq']}H") / 2)
        _window_end = add_to_datetime(self.task_config.current_cycle, +to_timedelta(f"{self.task_config['assim_freq']}H") / 2)

        local_dict = AttrDict(
            {
                'window_begin': _window_begin,
                'window_end': _window_end,
                'OPREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z.",
                'APREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z.",
                'COMIN_OBSPROC': os.path.join(self.task_config.OBSPROC_COMROOT,
                                              f"{self.task_config.RUN}.{self.task_config.current_cycle.strftime('%Y%m%d')}",
                                              f"{self.task_config.cyc:02d}",
                                              'atmos'),
            }
        )

        # task_config is everything that this task should need
        self.task_config = AttrDict(**self.task_config, **local_dict)

    @logit(logger)
    def initialize(self) -> None:
        """
        Initialize an atmospheric BUFR observation prep task

        This method will initialize an atmospheric BUFR observation prep task.
        This includes:
        - Staging input BUFR files
        - Staging configuration files
        - Staging any scripts needed to run the task
        """
        # create dictionary of observations to process using bufr2netcdf
        self.bufr2netcdf_obs = {}
        # Create dictionary of files to stage
        src_bufr_files = []
        dest_bufr_files = []
        src_mapping_files = []
        dest_mapping_files = []
        src_script_files = []
        dest_script_files = []
        for ob_name, ob_data in self.task_config.observations.items():
            if ob_data['method'] == 'bufr2netcdf':
                input_file = os.path.join(self.task_config.COMIN_OBSPROC, f"{self.task_config.OPREFIX}{ob_data['input file']}")
                output_file = os.path.join(self.task_config.DATA, ob_data['output file'])
                mapping_file = os.path.join(self.task_config.HOMEobsforge, "sorc", "spoc", "dump", "config", ob_data['mapping file'])
                src_bufr_files.append(input_file)
                dest_bufr_files.append(os.path.join(self.task_config.DATA, os.path.basename(input_file)))
                src_mapping_files.append(mapping_file)
                dest_mapping_files.append(os.path.join(self.task_config.DATA, os.path.basename(mapping_file)))
                self.bufr2netcdf_obs[ob_name] = {
                    'input_file': os.path.join(self.task_config.DATA, os.path.basename(input_file)),
                    'output_file': output_file,
                    'mapping_file': os.path.join(self.task_config.DATA, os.path.basename(mapping_file))
                    # TODO: MPI information here
                }
        # Stage the input files
        copylist = []
        for src, dest in zip(src_bufr_files, dest_bufr_files):
            copylist.append([src, dest])
        for src, dest in zip(src_mapping_files, dest_mapping_files):
            copylist.append([src, dest])
        for src, dest in zip(src_script_files, dest_script_files):
            copylist.append([src, dest])

        FileHandler({'copy_opt': copylist}).sync()

        # For now, as a hack, edit the mapping files to point to the correct reference time
        # We should eventually modify them in SPOC to use Jinja templates
        for dest in dest_mapping_files:
            yaml_file = YAMLFile(dest)
            try:
                yaml_file['bufr']['variables']['timestamp']['timeoffset']['referenceTime'] = \
                    self.task_config.current_cycle.strftime('%Y-%m-%dT%H:%M:%SZ')
                yaml_file.save(f"{dest}.tmp")
                os.replace(f"{dest}.tmp", dest)
            except Exception as e:
                logger.warning(f"Failed to update {dest}: {e}")

    @logit(logger)
    def execute(self) -> None:
        """
        Execute converters from BUFR to IODA format for atmospheric observations
        """
        #  ${obsforge_dir}/build/bin/bufr2netcdf.x "$input_file" "${mapping_file}" "$output_file"

        # Loop through BUFR to netCDF observations and convert them
        # TODO: Add MPI support

        for ob_name, ob_data in self.bufr2netcdf_obs.items():
            input_file = ob_data['input_file']
            output_file = ob_data['output_file']
            mapping_file = ob_data['mapping_file']
            logger.info(f"Converting {input_file} to {output_file} using {mapping_file}")
            exec_cmd = Executable(os.path.join(self.task_config.HOMEobsforge, "build", "bin", "bufr2netcdf.x"))
            exec_cmd.add_default_arg(input_file)
            exec_cmd.add_default_arg(mapping_file)
            exec_cmd.add_default_arg(output_file)
            try:
                logger.debug(f"Executing {exec_cmd}")
                exec_cmd()
            except Exception as e:
                logger.warning(f"Conversion failed for {ob_name}")
                logger.warning(f"Execution failed for {exec_cmd}: {e}")
                logger.debug("Exception details", exc_info=True)
                continue  # skip to the next observation

    @logit(logger)
    def finalize(self) -> None:
        """
        Finalize an atmospheric BUFR observation prep task

        This method will finalize an atmospheric BUFR observation prep task.
        This includes:
        - Creating an output directory in COMOUT
        - Copying output IODA files to COMOUT
        - Creating a "ready" file in COMOUT to signal that the observations are ready
        """
        comout = os.path.join(self.task_config['COMROOT'],
                              self.task_config['PSLOT'],
                              f"{self.task_config.RUN}.{self.task_config.current_cycle.strftime('%Y%m%d')}",
                              f"{self.task_config.cyc:02d}",
                              'atmos')
        # get a list of files to copy out
        output_files = glob.glob(os.path.join(self.task_config.DATA, "*.nc"))
        copy_list = []
        for output_file in output_files:
            filename = os.path.basename(output_file)
            destination_file = os.path.join(comout, f"{self.task_config['OPREFIX']}{filename}")
            copy_list.append([output_file, destination_file])
        FileHandler({'mkdir': [comout], 'copy_opt': copy_list}).sync()

        # create an empty file to tell external processes the obs are ready
        ready_file = pathlib.Path(os.path.join(comout, f"{self.task_config['OPREFIX']}obsforge_atmos_bufr_status.log"))
        ready_file.touch()
