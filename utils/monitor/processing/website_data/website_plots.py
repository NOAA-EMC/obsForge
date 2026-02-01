import os
import logging
import json

from processing.data_service import ComputeDataService
from processing.ioda_reader.reader import IodaReader
# from processing.ioda_reader.summary import IodaSummaryProduct
from processing.ioda_reader.obs_space_ioda_structure import ObsSpaceIodaStructure
from processing.plotting.plot_generator import PlotGenerator


logger = logging.getLogger(__name__)


DATA_PRODUCTS = [
    {
        "product_name": "obs_spaces",
        "product_type": "png",
        "generator": "generate_obs_space_plot",
    },
    {
        "product_name": "ioda_summary",
        "product_type": "json",
        "generator": "generate_obs_space_ioda_summary",
    },
]




class WebsitePlots:
    def __init__(self, db_path, data_root, output_dir):
        self.data_root = data_root
        self.output_dir = output_dir

        self.reader = ComputeDataService(db_path)
        # self.obs_reader = ObsSpaceReader()

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
                    # self._generate_obs_products(run_type, obs_space)
                    self._generate_data_products(run_type, obs_space)


    def _generate_data_products(self, run_type, data_object_name):
        cycles = self.reader.get_cycles_for_run(run_type)

        for spec in DATA_PRODUCTS:
            self._generate_product(
                product_name=spec["product_name"],
                product_type=spec["product_type"],
                product_generator=getattr(self, spec["generator"]),
                run_type=run_type,
                cycles=cycles,
                data_object_name=data_object_name,
            )

    def _generate_product(
        self,
        product_name,
        product_type,
        product_generator,
        run_type,
        cycles,
        data_object_name,
    ):
        """
        Generic product generator for cycle-based products.

        Args:
            product_name (str): logical name, e.g. "obs_spaces", "ioda_summary"
            product_type (str): file extension, e.g. "png", "json"
            product_generator (callable): function that generates the product
            cycles: cycle objects
            run_type (str): gfs, gdas, ...
            data_object_name (str): obs_space or obs_category name
        """
        # Debug / placeholder logic stays outside the generator
        # if data_object_name not in self.debug_obs_spaces:
            # return

        for cycle in cycles:
            product_path = self._create_product_path(
                product_name,
                product_type,
                run_type,
                data_object_name,
                cycle,
            )

            # do not regenerate
            if os.path.exists(product_path):
                continue

            product_generator(
                product_path=product_path,
                run_type=run_type,
                data_object_name=data_object_name,
                cycle=cycle,
            )


    def _create_product_path(self, product_name, product_type, run_type, obs_space, cycle):
        cycle_name = cycle["cycle_name"]

        product_output_dir = os.path.join(
            self.output_dir,
            run_type,
            cycle_name,
            product_name
        )
        os.makedirs(product_output_dir, exist_ok=True)

        safe_space = obs_space.replace("/", "_").replace(" ", "_")
        product_file = f"{safe_space}.{product_type}"
        product_path = os.path.join(product_output_dir, product_file)
        return product_path



    def get_obs_space_plot(self, run_type, obs_space, cycle):
        # product_path = self._create_plot_path(run_type, obs_space, cycle)
        product_path = self._create_product_path("obs_spaces", "png", run_type, obs_space, cycle)
        if os.path.exists(product_path):
            return product_path
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

    def generate_obs_space_plot(self, product_path, run_type, data_object_name, cycle):
        obs_space = data_object_name
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
            logger.error(f"Plot not generated, file not found: {product_path}")
            self._create_placeholder_plot(product_path, obs_space, cycle_name)
            return product_path

        if file_info["obs_count"] == 0:
            logger.warning(f"Zero obs file: {product_path}")
            self._create_placeholder_plot(product_path, obs_space, cycle_name)
            return product_path

        rel_file_path = file_info["file_path"]
        data_file_path = os.path.join(self.data_root, rel_file_path)

        self.obs_reader = IodaReader(data_file_path)

        # dim = self.obs_reader.get_obsvalue_dim(data_file_path)
        dim = self.obs_reader.get_effective_dim()
        logger.info(f"dimension = {dim} for  {data_file_path}")
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


    def _find_ioda_file(self, run_type, obs_space, cycle):
        file_info = self.reader.get_obs_space_file_for_cycle(
            run_type,
            obs_space,
            cycle["date"],
            cycle["cycle"]
        )

        if not file_info:
            return None

        # if file_info["obs_count"] == 0:
            # return None

        rel_file_path = file_info["file_path"]
        data_file_path = os.path.join(self.data_root, rel_file_path)

        return data_file_path


    def generate_obs_space_ioda_summary(self, product_path, run_type, data_object_name, cycle):
        obs_space = data_object_name
        cycle_id = cycle["cycle_name"]
        obs_category = "undefined"

        ioda_file_path = self._find_ioda_file(run_type, obs_space, cycle)
        if not ioda_file_path:
            return

        # product = IodaSummaryProduct(
            # run_type,
            # cycle_id,
            # obs_category,
            # obs_space,
            # ioda_file_path
        # )

        # summary = product.generate()
        # json_file = product.save_to_file(product_path)

        struct = ObsSpaceIodaStructure()
        struct.read_ioda(ioda_file_path)
        struct.write_json(product_path)


        logger.info(f"Generated IODA info summary for {obs_space} from {ioda_file_path} in {product_path}")

        # Later, website can read it
        # loaded_summary = IodaSummaryProduct.load_from_file(json_file)
        # print(loaded_summary)
