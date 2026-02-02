import os
import logging

from processing.data_service import ComputeDataService

from .registry import PRODUCT_REGISTRY

# Import products to trigger registration
from .ioda_structure_product import IodaStructureProduct
from .obs_space_plot_product import ObsSpacePlotProduct
from .obs_space_volume_plot_product import ObsSpaceVolumePlotProduct
from .obs_space_volume7_plot_product import ObsSpaceVolume7PlotProduct

logger = logging.getLogger(__name__)


class WebsiteDataProducts:
    def __init__(self, db_path, data_root, output_dir):
        self.data_root = data_root
        self.output_dir = output_dir
        self.reader = ComputeDataService(db_path)

    def generate(self):
        run_types = self.reader.get_all_run_types()
        if not run_types:
            logger.warning("No run types found in DB")
            return

        for run_type in run_types:
            # Get all observation spaces for this run type
            categories = self.reader.get_all_categories()
            for category in categories:
                data = self.reader.get_category_counts(run_type, category, days=None)
                if not data:
                    continue

                spaces_in_cat = self.reader.get_obs_spaces_for_category(category)
                for obs_space in spaces_in_cat:
                    self._generate_data_products(run_type, obs_space)


    def _generate_data_products(self, run_type, data_object_name):
        cycles = self.reader.get_cycles_for_run(run_type)
        
        for name, product in PRODUCT_REGISTRY.items():
            for cycle in cycles:
                product_path = product.get_path(
                    self.output_dir, run_type, cycle["cycle_name"], data_object_name
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
