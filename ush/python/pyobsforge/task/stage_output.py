import os
from logging import getLogger
from wxflow import Task, AttrDict, add_to_datetime, to_timedelta, FileHandler

logger = getLogger(__name__.split('.')[-1])


class StageOutput(Task):
    """
    Task to create output directories and stage files from other jobs
    as configured to support cycling JEDI-based data assimilation prototype systems.
    """

    def __init__(self, config):
        """
        Initialize the StageOutput task.

        Args:
            config (AttrDict): Configuration dictionary for the task.
        """
        super().__init__(config)
        _window_begin = add_to_datetime(self.task_config.current_cycle, -to_timedelta(f"{self.task_config['assim_freq']}H") / 2)
        _window_end = add_to_datetime(self.task_config.current_cycle, +to_timedelta(f"{self.task_config['assim_freq']}H") / 2)

        local_dict = AttrDict(
            {
                'window_begin': _window_begin,
                'window_end': _window_end,
                'OPREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z.",
                'APREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z.",
                # 'COMIN_OBSPROC': os.path.join(self.task_config.OBSPROC_COMROOT,
                #                               f"{self.task_config.RUN}.{self.task_config.current_cycle.strftime('%Y%m%d')}",
                #                               f"{self.task_config.cyc:02d}",
                #                               'atmos'),
                'COMIN_ATMOS_GSI': os.path.join(self.task_config['COMROOT'],
                                                self.task_config['PSLOT'],
                                                f"{self.task_config.RUN}.{self.task_config.current_cycle.strftime('%Y%m%d')}",
                                                f"{self.task_config.cyc:02d}",
                                                'atmos_gsi'),
                'COMIN_ATMOS_OBSFORGE': os.path.join(self.task_config['COMROOT'],
                                                     self.task_config['PSLOT'],
                                                     f"{self.task_config.RUN}.{self.task_config.current_cycle.strftime('%Y%m%d')}",
                                                     f"{self.task_config.cyc:02d}",
                                                     'atmos'),
                'COMOUT_ATMOS_OBS': os.path.join(self.task_config['COMROOT'],
                                                 self.task_config['PSLOT'],
                                                 'output',
                                                 f"{self.task_config.RUN}.{self.task_config.current_cycle.strftime('%Y%m%d')}",
                                                 f"{self.task_config.cyc:02d}",
                                                 'atmos'),
                'COMOUT_ATMOS_BC': os.path.join(self.task_config['COMROOT'],
                                                self.task_config['PSLOT'],
                                                'output',
                                                f"{self.task_config.RUN}.{self.task_config.current_cycle.strftime('%Y%m%d')}",
                                                f"{self.task_config.cyc:02d}",
                                                'analysis', 'atmos'),
            }
        )

        # task_config is everything that this task should need
        self.task_config = AttrDict(**self.task_config, **local_dict)
        logger.debug("StageOutput task initialized with configuration.")

    def run(self):
        """
        Execute the staging of output files as per the configuration.

        This method loops through all observations defined in self.task_config.observations
        and copies them from a source location to a destination:
        - If source is GSI: source directory is COMIN_ATMOS_GSI, files have suffix .gsi.nc
        - If source is BUFR: source directory is COMIN_ATMOS_OBSFORGE, files have suffix .nc
        - Destination is COMOUT_ATMOS_OBS with suffix .nc
        """
        logger.info("Starting the staging of output files.")

        copy_list = []
        obs_source_log = []  # Track observation names and their sources

        for obs in self.task_config.observations:
            obs_name = obs.get('name')
            obs_source = obs.get('source')

            if obs_source == 'GSI':
                src_dir = self.task_config.COMIN_ATMOS_GSI
                src_suffix = '.gsi.nc'
            elif obs_source == 'BUFR':
                src_dir = self.task_config.COMIN_ATMOS_OBSFORGE
                src_suffix = '.nc'
            else:
                logger.warning(f"Unknown source type '{obs_source}' for observation '{obs_name}'. Skipping.")
                continue

            src_file = os.path.join(src_dir, f"{self.task_config.OPREFIX}{obs_name}{src_suffix}")
            dest_file = os.path.join(self.task_config.COMOUT_ATMOS_OBS, f"{self.task_config.OPREFIX}{obs_name}.nc")

            if os.path.exists(src_file):
                copy_list.append([src_file, dest_file])
                obs_source_log.append((f"{self.task_config.OPREFIX}{obs_name}.nc", obs_source))
                logger.info(f"Staging {obs_name} from {src_file} to {dest_file}")
            else:
                logger.warning(f"Source file not found for observation '{obs_name}': {src_file}")

        if copy_list:
            FileHandler({'mkdir': [self.task_config.COMOUT_ATMOS_OBS], 'copy': copy_list}).sync()
            logger.info(f"Copied {len(copy_list)} observation files to {self.task_config.COMOUT_ATMOS_OBS}")

            # Write observation source log file
            log_file_path = os.path.join(self.task_config.COMOUT_ATMOS_OBS,
                                         f"{self.task_config.OPREFIX}observation_source.log")
            with open(log_file_path, 'w') as log_file:
                log_file.write("# Observation Source Log\n")
                log_file.write("# File Name, Source\n")
                for filename, source in obs_source_log:
                    log_file.write(f"{filename}, {source}\n")
            logger.info(f"Wrote observation source log to {log_file_path}")
        else:
            logger.warning("No observation files were copied.")

        # Copy bias correction files if source is GSI
        bias_correction_config = self.task_config.get('bias correction', {})
        if bias_correction_config.get('source') == 'GSI':
            bc_src_file = os.path.join(self.task_config.COMIN_ATMOS_GSI,
                                       f"{self.task_config.OPREFIX}rad_varbc_params.tar")
            bc_dest_file = os.path.join(self.task_config.COMOUT_ATMOS_BC,
                                        f"{self.task_config.OPREFIX}rad_varbc_params.tar")
            if os.path.exists(bc_src_file):
                FileHandler({'mkdir': [self.task_config.COMOUT_ATMOS_BC],
                             'copy': [[bc_src_file, bc_dest_file]]}).sync()
                logger.info(f"Copied bias correction file from {bc_src_file} to {bc_dest_file}")
            else:
                logger.warning(f"Bias correction file not found: {bc_src_file}")

        logger.info("Completed the staging of output files.")
