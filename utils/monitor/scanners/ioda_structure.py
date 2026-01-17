import logging
import os
from netCDF4 import Dataset

logger = logging.getLogger(__name__)

class IODAStructureReader:
    """
    Phase 1 Component: IODA Structure Scanner.
    
    Responsibility:
    - Open NetCDF file.
    - Extract Global Attributes.
    - Extract Group/Variable Hierarchy (Schema).
    - Extract Variable Attributes (Units, etc.).
    - STRICTLY NO DATA READING (No array access).
    """

    def scan_structure(self, file_path: str) -> dict:
        """
        Scans the file and returns a simplified dictionary of its structure.
        """
        if not os.path.exists(file_path):
            return {"valid_ioda": False, "error": "File not found"}

        try:
            with Dataset(file_path, 'r') as ds:
                # 1. Base Structure
                info = {
                    "valid_ioda": True,
                    "global_attrs": self._get_attrs(ds),
                    "groups": [],
                    "variables": []
                }

                # 2. Walk the file
                self._walk_group(ds, info)
                
                return info

        except Exception as e:
            return {"valid_ioda": False, "error": str(e)}

    def _get_attrs(self, obj) -> dict:
        """Helper to safely get attributes as strings."""
        attrs = {}
        for k in obj.ncattrs():
            try:
                val = getattr(obj, k)
                # Convert numpy types to native python for JSON/DB safety
                if hasattr(val, "item"): 
                    val = val.item()
                attrs[k] = str(val)
            except Exception:
                attrs[k] = "ERROR"
        return attrs

    def _walk_group(self, group_obj, info_dict, parent_path=""):
        """Recursive walker."""
        # Record Variables in this group
        for var_name, var_obj in group_obj.variables.items():
            full_path = f"{parent_path}/{var_name}" if parent_path else var_name
            
            var_meta = {
                "name": var_name,
                "full_path": full_path,
                "group": parent_path or "root",
                "dtype": str(var_obj.dtype),
                "dims": var_obj.dimensions,
                "shape": var_obj.shape,
                "attrs": self._get_attrs(var_obj)
            }
            info_dict["variables"].append(var_meta)

        # Recurse into subgroups
        for sub_grp_name, sub_grp_obj in group_obj.groups.items():
            full_path = f"{parent_path}/{sub_grp_name}" if parent_path else sub_grp_name
            info_dict["groups"].append(full_path)
            self._walk_group(sub_grp_obj, info_dict, full_path)
