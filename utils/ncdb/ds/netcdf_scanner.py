import numpy as np
import netCDF4
import logging
from typing import Dict, Tuple, Any
from .netcdf_node import NetcdfNode

logger = logging.getLogger(__name__)



def json_safe(value):
    """Convert value to JSON-safe Python type."""
    if isinstance(value, (np.integer,)):
        return int(value)
    elif isinstance(value, (np.floating,)):
        return float(value)
    elif isinstance(value, (np.bool_, bool)):
        return bool(value)
    elif isinstance(value, (np.bytes_, bytes)):
        return value.decode("utf-8")
    elif isinstance(value, np.ndarray):
        return value.tolist()
    elif hasattr(value, "tolist"):
        return value.tolist()
    elif value is None:
        return None
    else:
        return value


class NetcdfScanner:
    """
    A standalone service to walk NetCDF files.
    Can be run in 'metadata-only' mode (for structures) or 
    'value' mode (for physical files).
    """

    @classmethod
    def scan(cls, file_path: str, read_values: bool = False) -> Tuple[NetcdfNode, Dict[str, NetcdfNode]]:
        """
        Scans a NetCDF file and returns (root_node, nodes_by_path).
        """
        nodes_by_path: Dict[str, NetcdfNode] = {}
        
        try:
            with netCDF4.Dataset(file_path, 'r') as ds:
                root = cls._walk_group(ds, "/", nodes_by_path, read_values)
                return root, nodes_by_path
        except Exception as e:
            logger.error(f"Scanner failed to read {file_path}: {e}")
            # raise

    @classmethod
    def _walk_group(cls, group: Any, path: str, registry: Dict[str, NetcdfNode], read_values: bool) -> NetcdfNode:
        # 1. Create the Group node
        current_node = NetcdfNode(full_path=path, node_type="GROUP")
        registry[path] = current_node

        # 2. Attributes (Keys always, Values optional)
        for attr_name in group.ncattrs():
            val = group.getncattr(attr_name)
            current_node.attributes[attr_name] = json_safe(val) if read_values else None

        # 3. Dimensions
        for dim_name in group.dimensions:
            d_path = f"{path.rstrip('/')}/{dim_name}"
            # print(f"DEBUG [Scanner]: Registered DIMENSION '{dim_name}' at path '{d_path}'")
            d_node = NetcdfNode(full_path=d_path, node_type="DIMENSION")
            registry[d_path] = d_node
            current_node.add_child(d_node)

        # 4. Variables
        for var_name, var_obj in group.variables.items():
            v_path = f"{path.rstrip('/')}/{var_name}"
            v_node = NetcdfNode(
                full_path=v_path, 
                node_type="VARIABLE", 
                dtype=str(var_obj.dtype)
            )
            v_node.dim_names = list(var_obj.dimensions)
            
            for v_attr in var_obj.ncattrs():
                v_val = var_obj.getncattr(v_attr)
                v_node.attributes[v_attr] = json_safe(v_val) if read_values else None
            
            registry[v_path] = v_node
            current_node.add_child(v_node)
        # print(f"DEBUG [Scanner]: Registered VARIABLE '{var_name}' at path '{v_path}' with dims {list(var_obj.dimensions)}")

        # 5. Subgroups
        for sub_name, sub_group in group.groups.items():
            sub_path = f"{path.rstrip('/')}/{sub_name}/"
            child_group = cls._walk_group(sub_group, sub_path, registry, read_values)
            current_node.add_child(child_group)

        return current_node
