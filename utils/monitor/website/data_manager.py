import os
import shutil
from collections import defaultdict

from processing.website_data.products import WebDataServer


class DataManager:
    def __init__(self, products_root, web_data_root, max_cycles=4):
        self.products_root = products_root
        self.web_data_root = web_data_root
        self.max_cycles = max_cycles

        # Track what we have locally
        self.cycles = defaultdict(set)  # run_type -> {cycle_id}

        self.server = WebDataServer(self.products_root)

    # --------------------------------------------------
    # Public API
    # --------------------------------------------------

    def fetch_all_products(self, run_type, cycle_id):
        """
        Copy products_root/run_type/cycle_id
        to web_data_root/run_type/cycle_id
        """
        dst = os.path.join(self.web_data_root, run_type, cycle_id)
        os.makedirs(os.path.dirname(dst), exist_ok=True)

        # Copy only if not already present
        if not os.path.exists(dst):
            self.server.fetch_all_products(run_type, cycle_id, dst)

        self.cycles[run_type].add(cycle_id)

        # Enforce retention policy
        self.prune(run_type)

    def prune(self, run_type):
        """
        Keep only the most recent max_cycles for this run_type
        """
        run_dir = os.path.join(self.web_data_root, run_type)
        if not os.path.isdir(run_dir):
            return

        cycles = sorted(os.listdir(run_dir))
        if len(cycles) <= self.max_cycles:
            return

        to_remove = cycles[:-self.max_cycles]

        for cycle_id in to_remove:
            path = os.path.join(run_dir, cycle_id)
            shutil.rmtree(path, ignore_errors=True)
            self.cycles[run_type].discard(cycle_id)

    def get_product_relative_path(self, product_name, data_object_name, run_type, cycle_id):
        rel_path = self.server.get_product_relative_path(
            product_name, 
            data_object_name,
            run_type, 
            cycle_id, 
        )
        data_dir_name = os.path.basename(self.web_data_root.rstrip(os.sep))
        return os.path.join(data_dir_name, rel_path)

    def get_product_absolute_path(self, product_name, data_object_name, run_type, cycle_id):
        rel_path = self.server.get_product_relative_path(
            product_name, 
            data_object_name,
            run_type, 
            cycle_id, 
        )
        return os.path.join(self.web_data_root, rel_path)
