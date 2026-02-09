import os
import shutil
import logging

from processing.data_service import ComputeDataService

from .registry import PRODUCT_REGISTRY

# Import products to trigger registration
from .category_volume_plot_product import CategoryVolumePlotProduct
from .category_volume7_plot_product import CategoryVolume7PlotProduct
from .obs_space_var_data_plot_product import ObsSpaceVarDataPlotProduct
from .obs_space_volume_plot_product import ObsSpaceVolumePlotProduct
from .obs_space_volume7_plot_product import ObsSpaceVolume7PlotProduct
from .obs_space_mean_plot_product import ObsSpaceMeanPlotProduct
from .obs_space_mean7_plot_product import ObsSpaceMean7PlotProduct
from .ioda_structure_product import IodaStructureProduct
from .task_runtime_plot_product import TaskRuntimePlotProduct
from .task_runtime7_plot_product import TaskRuntime7PlotProduct

logger = logging.getLogger(__name__)


class WebDataServer:
    def __init__(self, products_root):
        self.products_root = products_root

    def fetch_all_products(self, run_type, cycle_id, dst):
        src = os.path.join(self.products_root, run_type, cycle_id)

        if not os.path.isdir(src):
            return
            # raise FileNotFoundError(f"Products not found: {src}")

        # shutil.copytree(src, dst)
        shutil.copytree(src, dst, dirs_exist_ok=True)

    def get_product_relative_path(
        self,
        product_name,
        data_object_name,
        run_type,
        cycle_id,
    ):
        """
        Returns the relative path to a specific data product.

        Example:
        run_type/cycle_id/ioda_structure/insitu_temp_surface.json
        """
        product = PRODUCT_REGISTRY.get(product_name)
        if not product:
            raise KeyError(f"Unknown product: {product_name}")

        safe_name = data_object_name.replace("/", "_").replace(" ", "_")

        return os.path.join(
            run_type,
            cycle_id,
            product.name,
            f"{safe_name}.{product.ext}",
        )


class WebsiteDataProducts:
    def __init__(self, db_path, data_root, output_dir, limit_cycles):
        self.data_root = data_root
        self.output_dir = output_dir
        self.limit_cycles = limit_cycles

        self.reader = ComputeDataService(db_path)

        self.run_types = self.reader.get_all_run_types()

        self.all_tasks = sorted({
            task
            for run_type in self.run_types
            for task in self.reader.get_all_task_names(run_type)
        })

        self.all_categories = self.reader.get_all_categories()

        self.all_obs_spaces = sorted({
            obs_space
            for category in self.all_categories
            for obs_space in self.reader.get_obs_spaces_for_category(category)
        })

    def generate(self):
        if not self.run_types:
            logger.warning("No run types found in DB")
            return

        for run_type in self.run_types:
            logger.info(f"Generating task data products for {run_type}")
            for task in self.all_tasks:
                # logger.info(f"--> {task}")
                self._generate_data_products(run_type, "task", task)

            logger.info(f"Generating category data products for {run_type}")
            for category in self.all_categories:
                # logger.info(f"--> {category}")
                self._generate_data_products(run_type, "category", category)

            logger.info(f"Generating obs space data products for {run_type}")
            for obs_space in self.all_obs_spaces:
                # logger.info(f"--> {obs_space}")
                self._generate_data_products(run_type, "obs_space", obs_space)

    def _generate_data_products(self, run_type, scope, data_object_name):
        cycles = self.reader.get_cycles_for_run(run_type)
        # logger.info(f"Generating data products for {run_type}, cycles = {cycles}")

        for name, product in PRODUCT_REGISTRY.items():
            if product.scope != scope:
                continue

            for cycle in cycles:
                product_path = product.get_path(
                    self.output_dir, 
                    run_type, 
                    cycle["cycle_name"], 
                    data_object_name
                )
                
                if os.path.exists(product_path):
                    continue

                product.generate(
                    product_path=product_path,
                    run_type=run_type,
                    data_object_name=data_object_name,
                    cycle=cycle,
                    reader=self.reader,
                    data_root=self.data_root
                )
