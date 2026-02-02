import os

from .registry import register_product, DataProduct
from processing.ioda_reader.obs_space_ioda_structure import ObsSpaceIodaStructure


@register_product
class IodaStructureProduct(DataProduct):
    name = "ioda_summary" # to be renamed to ioda_structure or simply ioda
    ext = "json"

    def generate(self, product_path, run_type, data_object_name, cycle, reader, data_root):
        # if data_object_name is not a name of an obs_space, return
        file_info = reader.get_obs_space_file_for_cycle(
            run_type, data_object_name, cycle["date"], cycle["cycle"]
        )
        if not file_info:
            return

        full_path = os.path.join(data_root, file_info["file_path"])
        struct = ObsSpaceIodaStructure()
        struct.read_ioda(full_path)
        struct.write_json(product_path)
