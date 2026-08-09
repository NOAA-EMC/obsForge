import os
import logging

from processing.data_service import ComputeDataService
from processing.plotting.plot_generator import PlotGenerator

from .registry import register_product, DataProduct

logger = logging.getLogger(__name__)


@register_product
class TaskRuntimePlotProduct(DataProduct):
    name = "task_runtime"
    ext = "png"
    scope = "task"

    def generate(self, product_path, run_type, data_object_name, cycle, reader, data_root):
        # if data_object_name is not a name of an obs_space, return
        tasks = reader.get_all_task_names(run_type)
        if not data_object_name in tasks:
            return
        task = data_object_name

        data = reader.get_task_timing_series(
            run_type, 
            task, 
            days=None
        )
        if not data:
            logger.error(f"NO DATA for {data_object_name}")
            return

        plot_output_dir = ""
        plotter = PlotGenerator(plot_output_dir)

        title = f"{task}"
        val_key = "mean_runtime"
        std_key = None
        y_label = "Seconds"

        # plotter.generate_history_plot(
        plotter.generate_history_plot_with_moving_avg(
            title,
            data,
            val_key,
            std_key,
            product_path,
            y_label=y_label,
            days=None,
            clamp_bottom=False
        )
