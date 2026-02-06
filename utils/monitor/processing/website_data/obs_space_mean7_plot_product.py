import os
import logging

from processing.data_service import ComputeDataService
from processing.plotting.plot_generator import PlotGenerator

from .registry import register_product, DataProduct

logger = logging.getLogger(__name__)


@register_product
class ObsSpaceMean7PlotProduct(DataProduct):
    name = "obs_space_mean7"
    ext = "png"
    scope = "obs_space"

    def generate(self, product_path, run_type, data_object_name, cycle, reader, data_root):
        schema = reader.get_obs_space_schema(data_object_name)
        if not schema:
            return

        v = next((r['name'] for r in schema if r.get('group_name') == 'ObsValue'), None)
        if not v:
            return

        data = reader.get_variable_physics_series(
            run_type, 
            data_object_name, 
            v, 
            days=None
        )
        # the obs_space may not be processed by this run type
        if not data:
            return

        obs_space = data_object_name

        plot_output_dir = ""
        plotter = PlotGenerator(plot_output_dir)

        title = f"{v} (Mean ± Spatial \u03C3)"
        val_key = "mean_val"
        std_key = "std_dev"
        y_label = "Value"

        plotter.generate_history_plot(
            title,
            data,
            val_key,
            std_key,
            product_path,
            y_label=y_label,
            days=7,
            clamp_bottom=False
        )
