import os
import logging

logger = logging.getLogger(__name__)

class DataProduct:
    """Base class for all data products."""
    name = None
    ext = None
    scope = None

    def generate(self, product_path, run_type, data_object_name, cycle, reader, data_root):
        raise NotImplementedError

    @classmethod
    def get_path(cls, output_dir, run_type, cycle_name, data_object_name):
        """Standardized path generation."""
        product_dir = os.path.join(output_dir, run_type, cycle_name, cls.name)
        os.makedirs(product_dir, exist_ok=True)
        
        safe_name = data_object_name.replace("/", "_").replace(" ", "_")
        return os.path.join(product_dir, f"{safe_name}.{cls.ext}")

# The global registry
PRODUCT_REGISTRY = {}

def register_product(cls):
    PRODUCT_REGISTRY[cls.name] = cls()
    return cls
