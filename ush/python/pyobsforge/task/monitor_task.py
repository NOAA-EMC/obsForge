from wxflow import Task
from pyobsforge.monitor.obsforge_monitor import ObsforgeMonitor

class ObsforgeMonitorTask(Task):
    """
    The Workflow Adapter.
    Inherits from wxflow.Task to satisfy system requirements.
    Wraps ObsforgeMonitor to run within the workflow environment.
    """
    def __init__(self, config):
        # 1. Initialize wxflow Task 
        # (This validates PDY, cycle, etc. required by the workflow)
        super().__init__(config)
        
        # 2. Instantiate the real monitor
        # We pass the fully formed config (which now includes PDY/cyc)
        self.monitor = ObsforgeMonitor(config)

    def run(self):
        # Delegate execution to the core logic
        self.monitor.run()
