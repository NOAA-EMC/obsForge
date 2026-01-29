import os
import logging

from .data_service import ComputeDataService
from .obs_space_reader import ObsSpaceReader
from processing.plotting.plot_generator import PlotGenerator

logger = logging.getLogger(__name__)


class DataProducts:
    def __init__(self, db_path, data_root, output_dir):
        self.data_root = data_root
        self.output_dir = output_dir

        self.reader = ComputeDataService(db_path)
        self.obs_reader = ObsSpaceReader()

        self.run_types = self.reader.get_all_run_types()
        if not self.run_types:
            logger.warning("No run types found in DB")
            return

        self.debug_obs_spaces = {
            "insitu_salt_surface_drifter",
            "insitu_salt_surface_ndbc",
            "insitu_salt_surface_trkob",
            "insitu_temp_surface_drifter",
            "insitu_temp_surface_ndbc",
            "insitu_temp_surface_trkob",
        }

    # --- Recent Data Products (last 4 cycles) ---
    def generate(self):
        for run_type in self.run_types:
            # Get all observation spaces for this run type
            categories = self.reader.get_all_categories()
            for category in categories:
                data = self.reader.get_category_counts(run_type, category, days=None)
                if not data:
                    continue

                spaces_in_cat = self.reader.get_obs_spaces_for_category(category)
                for obs_space in spaces_in_cat:
                    self._generate_obs_products(run_type, obs_space)

    def _generate_obs_products(self, run_type, obs_space):

        """
        cycle = {
            "date": "YYYYMMDD",
            "cycle": 6,
            "cycle_name": "YYYYMMDD_06"
        }
        """
        cycles = self.reader.get_cycles_for_run(run_type)
        last_cycles = cycles[-4:]  # last 4 cycles

        for cycle in last_cycles:
            # if obs_space not in self.debug_obs_spaces:
                # self._create_placeholder_plot(plot_path, obs_space, cycle_name)
                # return plot_path
            plot_path = self._create_plot_path(run_type, obs_space, cycle)
            # do not regenerate
            if os.path.exists(plot_path):
                continue
            else:
                self.generate_obs_space_plot(plot_path, run_type, obs_space, cycle)

    def _create_plot_path(self, run_type, obs_space, cycle):
        cycle_name = cycle["cycle_name"]

        plot_output_dir = os.path.join(
            self.output_dir,
            run_type,
            cycle_name,
            "obs_spaces"
        )
        os.makedirs(plot_output_dir, exist_ok=True)

        safe_space = obs_space.replace("/", "_").replace(" ", "_")
        plot_file = f"{safe_space}.png"
        plot_path = os.path.join(plot_output_dir, plot_file)
        return plot_path

    def get_obs_space_plot(self, run_type, obs_space, cycle):
        plot_path = self._create_plot_path(run_type, obs_space, cycle)
        if os.path.exists(plot_path):
            return plot_path
        else:
            return None

    def _create_placeholder_plot(self, path, space, cycle_name):
        """Create a tiny placeholder PNG"""
        try:
            from PIL import Image, ImageDraw, ImageFont
        except ImportError:
            # Just create an empty file if Pillow not available
            with open(path, "w") as f:
                f.write(f"{space} | {cycle_name}")
            logger.warning("Pillow not installed, creating text placeholder")
            return

        # Create a small PNG with text
        img = Image.new("RGB", (300, 200), color=(220, 220, 220))
        d = ImageDraw.Draw(img)
        try:
            font = ImageFont.load_default()
            d.text((10, 80), f"{space}\n{cycle_name}", fill=(0, 0, 0), font=font)
        except Exception:
            d.text((10, 80), f"{space}\n{cycle_name}", fill=(0, 0, 0))

        img.save(path)
        # logger.info(f"Created placeholder plot: {path}")

    def generate_obs_space_plot(self, plot_path, run_type, obs_space, cycle):
        cycle_name = cycle["cycle_name"]

        # generate surface plot
        # 1. Find file
        file_info = self.reader.get_obs_space_file_for_cycle(
            run_type,
            obs_space,
            cycle["date"],
            cycle["cycle"]
        )
        # logger.info(f"Plot for : {file_info}")

        if not file_info:
            logger.error(f"Plot not generated, file not found: {plot_path}")
            self._create_placeholder_plot(plot_path, obs_space, cycle_name)
            return plot_path

        if file_info["obs_count"] == 0:
            logger.warning(f"Zero obs file: {plot_path}")
            self._create_placeholder_plot(plot_path, obs_space, cycle_name)
            return plot_path

        rel_file_path = file_info["file_path"]
        data_file_path = os.path.join(self.data_root, rel_file_path)

        # dim = self.obs_reader.get_obsvalue_dim(data_file_path)
        dim = self.obs_reader.get_effective_dim(data_file_path)
        logger.info(f"dimension = {dim} for  {data_file_path}")
        if dim != 2:
            self._create_placeholder_plot(plot_path, obs_space, cycle_name)
            return plot_path

        # 2. Read surface data from NetCDF
        data = self.obs_reader.get_surface_data(data_file_path)
        if not data:
            logger.error(f"NetCDF found no data in file: {file_path}")
            self._create_placeholder_plot(plot_path, obs_space, cycle_name)
            return plot_path

        # 3. Generate plot
        variable = data['var_name']
        logger.info(f"generating plot for = {variable} in {obs_space} data file:  {data_file_path}")
        # plot_output_dir needs to go
        plot_output_dir = ""
        plotter = PlotGenerator(plot_output_dir)
        plotter.generate_surface_map(
            plot_path,
            run_type,
            obs_space,
            data["lats"],
            data["lons"],
            data["values"],
            data["var_name"],
            data["units"]
        )

        if not os.path.exists(plot_path):
            logger.error(f"Expected plot not created: data = {data_file_path}, plot ={plot_path}")
            self._create_placeholder_plot(plot_path, obs_space, cycle_name)
