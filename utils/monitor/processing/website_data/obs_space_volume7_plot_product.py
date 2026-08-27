import os
import logging

from processing.data_service import ComputeDataService
from processing.plotting.plot_generator import PlotGenerator

from .registry import register_product, DataProduct

logger = logging.getLogger(__name__)


@register_product
class ObsSpaceVolume7PlotProduct(DataProduct):
    name = "obs_space_volume7"
    ext = "png"
    scope = "obs_space"

    def generate(self, product_path, run_type, data_object_name, cycle, reader, data_root):

        # if data_object_name is not a name of an obs_space, return
        data = reader.get_obs_space_counts(run_type, data_object_name, days=None)
        if not data:
            return

        obs_space = data_object_name
        safe_name = obs_space.replace("/", "_").replace(" ", "_")

        plot_output_dir = ""
        plotter = PlotGenerator(plot_output_dir)

        title = "Total Obs (± Historical \u03C3)"
        fname_base = f"{run_type}_{safe_name}_cnt"
        val_key = "total_obs"
        std_key = None      # could be std deviation
        y_label = "Count"

        # plotter.generate_history_plot(
        plotter.generate_history_plot_with_moving_avg(
            title,
            data,
            val_key,
            std_key,
            # fname=f"{fname_base}_all.png",
            product_path,
            y_label=y_label,
            days=7,
        )
