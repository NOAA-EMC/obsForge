#!/usr/bin/env python3

import glob
import json
import multiprocessing as mp
import os
import pathlib
from logging import getLogger
from typing import Dict, Any

from wxflow import (AttrDict, Task, add_to_datetime, to_timedelta,
                    logit, FileHandler, Executable, YAMLFile, save_as_yaml)


logger = getLogger(__name__.split('.')[-1])


def mp_bufr_converter(ob_name, exec_cmd):
    try:
        logger.debug(f"Executing {exec_cmd}")
        exec_cmd()
    except Exception as e:
        logger.warning(f"Conversion failed for {ob_name}")
        logger.warning(f"Execution failed for {exec_cmd}: {e}")
        logger.debug("Exception details", exc_info=True)


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
        Initialize an atmospheric BUFR observation prep task.

        Steps:
        - Collect input BUFR, mapping, and script files
        - Stage files into the working directory
        - Register observations for bufr2netcdf conversion
        """
        self.script2netcdf_obs = {}
        copylist = []
        sub_dir_list = []

        for ob_name, ob_data in self.task_config.observations.items():
            logger.debug(f"Processing observation: {ob_name}: {ob_data}")

            # Normalize fields to lists
            input_files = ob_data.get('input_file', [])
            mapping_files = ob_data.get('mapping_file', [])
            script_files = ob_data.get('script_file', [])
            aux_files = ob_data.get('aux_file', [])
            preserve_rel_path = ob_data.get('preserve_rel_path', None)

            if isinstance(input_files, str):
                input_files = [input_files]
            if isinstance(mapping_files, str):
                mapping_files = [mapping_files]
            if isinstance(script_files, str):
                script_files = [script_files]

            staged_inputs, staged_mappings, staged_scripts = [], [], []

            # Stage BUFR input files
            for f in input_files:
                src = os.path.join(self.task_config.COMIN_OBSPROC, f"{self.task_config.OPREFIX}{f}")
                dest = os.path.join(self.task_config.DATA, os.path.basename(src))
                if preserve_rel_path:  # TODO A hack temporary to preserve directory structure if needed
                    comin_obsproc = self.task_config.COMIN_OBSPROC
                    # Take the last 3 parts of the comin_obsproc path
                    last_three = os.path.join(*comin_obsproc.split(os.sep)[-3:])
                    sub_dir_tmp = os.path.join(self.task_config.DATA, last_three)
                    logger.debug(f"Creating subdirectory for input path: {sub_dir_tmp}")
                    if sub_dir_tmp not in sub_dir_list:
                        sub_dir_list.append(sub_dir_tmp)
                    dest = os.path.join(sub_dir_tmp, os.path.basename(src))
                copylist.append([src, dest])
                staged_inputs.append(dest)

            # Stage mapping files
            for f in mapping_files:
                src = os.path.join(
                    self.task_config.HOMEobsforge, "sorc", "spoc", "dump", "config", "atmosphere", f
                )
                dest = os.path.join(self.task_config.DATA, os.path.basename(src))
                copylist.append([src, dest])
                staged_mappings.append(dest)

            # Stage script files
            for f in script_files:
                src = os.path.join(
                    self.task_config.HOMEobsforge, "sorc", "spoc", "dump", "scripts", "atmosphere", f
                )
                dest = os.path.join(self.task_config.DATA, os.path.basename(src))
                copylist.append([src, dest])
                staged_scripts.append(dest)

            # Stage auxiliary files if any
            for f in aux_files:
                src = os.path.join(self.task_config.HOMEobsforge, "sorc", "spoc", "dump", "aux", f)
                dest = os.path.join(self.task_config.DATA, os.path.basename(src))
                copylist.append([src, dest])

            # Prepare input string for the script
            input_str = staged_inputs
            input_dict = ob_data.get('input')
            logger.debug(input_dict)
            if input_dict:
                input_tmp = {}
                for key, val in input_dict.items():
                    input_tmp[key] = staged_inputs[int(val)]
                input_str = json.dumps(input_tmp)

            # Register observation config (always as lists)
            self.script2netcdf_obs[ob_name] = {
                'input_str': input_str,
                'output_file': [os.path.join(self.task_config.DATA, ob_data['output_file'])],
                'script_file': staged_scripts,
                'mpi': ob_data.get('mpi', 1),
            }

        # Stage all files
        if sub_dir_list:
            logger.debug(f"Creating subdirectories: {sub_dir_list}")
            FileHandler({'mkdir': sub_dir_list}).sync()
        FileHandler({'copy_opt': copylist}).sync()

        # For now, as a hack, edit the mapping files to point to the correct reference time
        # We should eventually modify them in SPOC to use Jinja templates
        for dest in staged_mappings:
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
        #  ${obsforge_dir}/build/bin/script2netcdf.x "$input_file" "$output_file"

        # Loop through BUFR to netCDF observations and convert them

        exec_cmd_list = []
        mpi_count = 0
        for ob_name, ob_data in self.script2netcdf_obs.items():
            input_str = ob_data['input_str']
            output_file = ob_data['output_file']
            script_files = ob_data.get('script_file', [])
            if not script_files:
                logger.error(f"No script_file provided for observation '{ob_name}'. Skipping.")
                continue
            script_file = script_files[0]
            mpi = ob_data.get('mpi', 1)
            logger.info(f"Converting {input_str} to {output_file} using {script_file} and MPI={mpi}")
            if mpi > 1:
                mpi_count += int(mpi)
                logger.info(f"Using MPI with {mpi} ranks for {ob_name}")
                if self.task_config.MPI_LAUNCHER.lower() == 'mpiexec':
                    exec_cmd = Executable("mpiexec")
                    args = [
                        "-n", str(mpi),
                        "python", script_file,
                        "--input", input_str,
                        "--output", output_file,
                    ]
                else:  # default to srun
                    exec_cmd = Executable("srun")
                    args = [
                        "--export", "All",
                        "-n", str(mpi),
                        "--mem", "0G",              # no memory limit
                        "--time", "00:30:00",
                        "python", script_file,
                        "--input", input_str,
                        "--output", output_file,
                    ]
            else:
                exec_cmd = Executable('python')
                args = [script_file, '--input', input_str, '--output', output_file]

            for arg in args:
                exec_cmd.add_default_arg(arg)

            exec_cmd_list.append((ob_name, exec_cmd))

        # get parallel processing info
        num_workers = min(len(exec_cmd_list) + mpi_count + 5, max(1, mp.cpu_count() - 1))
        logger.info(f"Number of worker processes to use: {num_workers} (CPU cores available: {mp.cpu_count()})")
        # run everything in parallel
        with mp.Pool(num_workers) as pool:
            pool.starmap(mp_bufr_converter, exec_cmd_list)

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
            if 'Coeff' not in filename:
                destination_file = os.path.join(comout, f"{self.task_config['OPREFIX']}{filename}")
                copy_list.append([output_file, destination_file])
        FileHandler({'mkdir': [comout], 'copy_opt': copy_list}).sync()

        # create a summary stats file to tell external processes the obs are ready
        ready_file = pathlib.Path(os.path.join(comout, f"{self.task_config['OPREFIX']}obsforge_atmos_bufr_status.log"))
        summary_dict = {
            'time window': {
                'begin': self.task_config.window_begin.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'end': self.task_config.window_end.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'bound to include': 'begin',
            },
            'input directory': str(comout),
            'output file': str(ready_file),
        }
        save_as_yaml(summary_dict, os.path.join(self.task_config.DATA, "stats.yaml"))
        exec_cmd = Executable(os.path.join(self.task_config.HOMEobsforge, "build", "bin", "ioda-dump.x"))
        exec_cmd.add_default_arg(os.path.join(self.task_config.DATA, "stats.yaml"))
        try:
            logger.info(f"Creating summary file {ready_file}")
            exec_cmd()
        except Exception as e:
            logger.warning(f"Failed to create summary file {ready_file}: {e}")
            logger.warning("Creating an empty ready file instead")
            ready_file.touch()
