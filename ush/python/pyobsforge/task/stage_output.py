from logging import getLogger
from wxflow import Task

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
                                                     'atmos')
                'COMOUT_ATMOS_OBS': os.path.join(self.task_config['COMROOT'],
                                                 self.task_config['PSLOT'],
                                                 'output',
                                                 f"{self.task_config.RUN}.{self.task_config.current_cycle.strftime('%Y%m%d')}",
                                                 f"{self.task_config.cyc:02d}",
                                                 'atmos')
                'COMOUT_ATMOS_BC': os.path.join(self.task_config['COMROOT'],
                                                self.task_config['PSLOT'],
                                                'output',
                                                f"{self.task_config.RUN}.{self.task_config.current_cycle.strftime('%Y%m%d')}",
                                                f"{self.task_config.cyc:02d}",
                                                'analysis', 'atmos')
            }
        )

        # task_config is everything that this task should need
        self.task_config = AttrDict(**self.task_config, **local_dict)
        logger.debug("StageOutput task initialized with configuration.")

    def run(self):
        """
        Execute the staging of output files as per the configuration.
        """
        logger.info("Starting the staging of output files.")
        # Implementation of staging logic goes here
        # This could involve creating directories, copying files, etc.
        logger.info("Completed the staging of output files.")
