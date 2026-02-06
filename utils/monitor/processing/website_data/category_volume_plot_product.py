import os
import logging

from processing.data_service import ComputeDataService
from processing.plotting.plot_generator import PlotGenerator

from .registry import register_product, DataProduct

logger = logging.getLogger(__name__)


@register_product
class CategoryVolumePlotProduct(DataProduct):
    name = "category_volume"
    ext = "png"
    scope = "category"

    def generate(self, product_path, run_type, data_object_name, cycle, reader, data_root):
        data = reader.get_category_counts(run_type, data_object_name, days=None)
        if not data:
            return

        category = data_object_name

        plot_output_dir = ""
        plotter = PlotGenerator(plot_output_dir)

        title = "{cat} Total Obs"
        val_key = "total_obs"
        std_key = None      # could be std deviation
        y_label = "Count"

        plotter.generate_history_plot(
            title,
            data,
            val_key,
            std_key,
            product_path,
            y_label=y_label,
            days=None,
        )
