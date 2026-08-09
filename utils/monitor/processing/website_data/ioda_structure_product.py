import os

from .registry import register_product, DataProduct
from processing.ioda_reader.ioda_structure import IodaStructure
# from processing.ioda_reader.ioda_content import IodaContent
from processing.ioda_reader.ioda_file import IodaFile


@register_product
class IodaStructureProduct(DataProduct):
    name = "ioda_structure"
    ext = "json"
    scope = "obs_space"

    def generate(self, product_path, run_type, data_object_name, cycle, reader, data_root):
        # if data_object_name is not a name of an obs_space, return
        file_info = reader.get_obs_space_file_for_cycle(
            run_type, data_object_name, cycle["date"], cycle["cycle"]
        )
        if not file_info:
            return

        full_path = os.path.join(data_root, file_info["file_path"])
        # struct = IodaStructure()
        # struct.read_ioda(full_path)
        # content = IodaContent(full_path)
        file = IodaFile(full_path)
        structure = file.extract_structure()
        # structure = content.extract_structure()
        structure.write_json(product_path)
