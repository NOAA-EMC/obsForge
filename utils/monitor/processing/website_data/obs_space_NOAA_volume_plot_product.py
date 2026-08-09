import os
import logging

from processing.data_service import ComputeDataService
from processing.plotting.plot_generator import PlotGenerator

from .registry import register_product, DataProduct

logger = logging.getLogger(__name__)


@register_product
class ObsSpaceNOAAVolumePlotProduct(DataProduct):
    name = "obs_space_NOAA_volume"
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

        title = "Number of Observations"
        val_key = "total_obs"
        y_label = "Count"

        logger.info(f"Generating {product_path}")

        plotter.generate_NOAA_Obs_count_plot(
            title,
            data,
            val_key,
            product_path,
            y_label="Obs per Hour",
        )
