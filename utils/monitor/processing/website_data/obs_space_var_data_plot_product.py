import os
import logging

from processing.data_service import ComputeDataService
from processing.ioda_reader.reader import IodaReader
from processing.plotting.plot_generator import PlotGenerator

from .registry import register_product, DataProduct

logger = logging.getLogger(__name__)


@register_product
class ObsSpaceVarDataPlotProduct(DataProduct):
    name = "obs_space_var_data"
    ext = "png"
    scope = "obs_space"

    def generate(self, product_path, run_type, data_object_name, cycle, reader, data_root):
        cycle_name = cycle["cycle_name"]

        # generate surface plot
        # 1. Find file
        file_info = reader.get_obs_space_file_for_cycle(
            run_type,
            data_object_name,
            cycle["date"],
            cycle["cycle"]
        )
        # logger.info(f"Plot for : {file_info}")

        # if data_object_name is not a name of an obs_space, return
        if not file_info:
            return
            # logger.error(f"Plot not generated, file not found: {product_path}")
            # self._create_placeholder_plot(product_path, obs_space, cycle_name)
            # return product_path

        obs_space = data_object_name

        if file_info["obs_count"] == 0:
            logger.warning(f"Zero obs file: {product_path}")
            self._create_placeholder_plot(product_path, obs_space, cycle_name)
            return product_path

        rel_file_path = file_info["file_path"]
        data_file_path = os.path.join(data_root, rel_file_path)

        self.obs_reader = IodaReader(data_file_path)

        # dim = self.obs_reader.get_obsvalue_dim(data_file_path)
        dim = self.obs_reader.get_effective_dim()
        # logger.info(f"dimension = {dim} for  {data_file_path}")
        if dim != 2:
            self._create_placeholder_plot(product_path, obs_space, cycle_name)
            return product_path

        # 2. Read surface data from NetCDF
        data = self.obs_reader.get_surface_data()
        if not data:
            logger.error(f"NetCDF found no data in file: {file_path}")
            self._create_placeholder_plot(product_path, obs_space, cycle_name)
            return product_path

        # 3. Generate plot
        variable = data['var_name']
        logger.info(f"generating plot for = {variable} in {obs_space} data file:  {data_file_path}")
        # plot_output_dir needs to go
        plot_output_dir = ""
        plotter = PlotGenerator(plot_output_dir)
        plotter.generate_surface_map(
            product_path,
            run_type,
            obs_space,
            data["lats"],
            data["lons"],
            data["values"],
            data["var_name"],
            data["units"]
        )

        if not os.path.exists(product_path):
            logger.error(f"Expected plot not created: data = {data_file_path}, plot ={product_path}")
            self._create_placeholder_plot(product_path, obs_space, cycle_name)

    def _create_placeholder_plot(self, path, obs_space, cycle_name):
        """Create a tiny placeholder PNG"""
        try:
            from PIL import Image, ImageDraw, ImageFont
        except ImportError:
            # Just create an empty file if Pillow not available
            with open(path, "w") as f:
                f.write(f"{obs_space} | {cycle_name}")
            logger.warning("Pillow not installed, creating text placeholder")
            return

        # Create a small PNG with text
        img = Image.new("RGB", (300, 200), color=(220, 220, 220))
        d = ImageDraw.Draw(img)
        try:
            font = ImageFont.load_default()
            d.text((10, 80), f"{obs_space}\n{cycle_name}", fill=(0, 0, 0), font=font)
        except Exception:
            d.text((10, 80), f"{obs_space}\n{cycle_name}", fill=(0, 0, 0))

        img.save(path)
        # logger.info(f"Created placeholder plot: {path}")
