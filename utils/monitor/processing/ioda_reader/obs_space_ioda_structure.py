import json
import numpy as np

'''
The Reconstruction Workflow
If you have this JSON spec and the raw data elsewhere (e.g., in a separate NumPy array or a database), the reconstruction logic would look like this:

Initialize: Open a new netCDF4.Dataset in write mode.

Global Metadata: Loop through global_attributes and apply them using setncattr.

Dimensions: Create dimensions using createDimension(name, size).

Groups & Variables: * Create the group.

Create the variable inside the group using the dtype, dimensions, and storage (compression/chunks) found in your spec.

Data Injection: Load your external data and assign it: var[:] = external_data.

Limitations to Watch For
The "Jedi" Library: IODA files are often written using the IODA-C++ API (part of JEDI). If your file has specialized "ObsSpace" features specific to JEDI (like complex indexing), a pure NetCDF reconstruction might miss some of the internal IODA engine's expected "magic" if the attributes aren't exactly right.

String Lengths: If your IODA file uses fixed-length strings (vlen), you'll need to ensure the dtype string conversion captures the length correctly.
'''


class IodaNumpyEncoder(json.JSONEncoder):
    """Custom encoder to handle NumPy types found in IODA/NetCDF metadata."""
    def default(self, obj):
        if isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)

class ObsSpaceIodaStructure:
    def __init__(self):
        self._schema = {
            "global_attributes": {},
            "dimensions": {},
            "groups": {}
        }

    def read_ioda(self, file_path):
        from .reader import IodaReader  
        
        with IodaReader(file_path) as reader:
            ds = reader.ds 
            
            # 1. Capture Global Attributes (NEW: Critical for reconstruction)
            self._schema["global_attributes"] = {
                attr: ds.getncattr(attr) for attr in ds.ncattrs()
            }
            
            # 2. Capture Global Dimensions
            for dim_name, dim_obj in ds.dimensions.items():
                self._schema["dimensions"][dim_name] = {
                    "size": len(dim_obj),
                    "isunlimited": dim_obj.isunlimited()
                }

            # 3. Capture Groups and Variables
            for group_name, group_obj in ds.groups.items():
                self._schema["groups"][group_name] = {}
                
                for var_name, var_obj in group_obj.variables.items():
                    attrs = {attr: var_obj.getncattr(attr) for attr in var_obj.ncattrs()}
                    
                    # NEW: Capture storage filters (compression, chunking)
                    filters = var_obj.filters() # returns dict like {'zlib': True, 'complevel': 4}
                    chunking = var_obj.chunking()
                    
                    self._schema["groups"][group_name][var_name] = {
                        "dtype": str(var_obj.dtype),
                        "dimensions": var_obj.dimensions,
                        "attributes": attrs,
                        "storage": {
                            "filters": filters,
                            "chunks": chunking
                        }
                    }

    def load_from_dict(self, data_dict):
        self._schema = data_dict

    def as_dict(self):
        return self._schema

    def write_json(self, output_path, indent=2):
        """Serializes the current schema to a JSON file, handling NumPy types."""
        with open(output_path, 'w') as f:
            json.dump(self._schema, f, indent=indent, cls=IodaNumpyEncoder)

    def read_json(self, input_path):
        """Hydrates the schema from a JSON file."""
        with open(input_path, 'r') as f:
            self._schema = json.load(f)

    # --- Inquiry Services ---
    def get_groups(self):
        return list(self._schema["groups"].keys())

    def get_vars_in_group(self, group):
        return list(self._schema["groups"].get(group, {}).keys())

    def get_var_spec(self, group, var):
        return self._schema["groups"].get(group, {}).get(var, {})

    # --- HTML Service ---

    def old_as_html(self):
        """Generates a technical specification fragment for the website."""
        html = ["<div class='ioda-structure-container'>", "<h3>IODA Structure Definition</h3>"]
        
        for group in self.get_groups():
            html.append(f"<details open><summary><strong>Group: {group}</strong></summary>")
            html.append("<table class='structure-table'>")
            html.append("<thead><tr><th>Variable</th><th>Type</th><th>Dims</th><th>Attributes</th></tr></thead><tbody>")
            
            for var in self.get_vars_in_group(group):
                spec = self.get_var_spec(group, var)
                attr_str = "<br>".join([f"<i>{k}</i>: {v}" for k, v in spec['attributes'].items()])
                
                html.append("<tr>")
                html.append(f"<td><code>{var}</code></td>")
                html.append(f"<td>{spec['dtype']}</td>")
                html.append(f"<td>{', '.join(spec['dimensions'])}</td>")
                html.append(f"<td><small>{attr_str}</small></td>")
                html.append("</tr>")
            
            html.append("</tbody></table></details><br>")
        
        html.append("</div>")
        return "\n".join(html)


    def as_html(self):
        """Generates a comprehensive, fully collapsible technical specification."""
        
        # Start the main collapsible container
        html = [
            "<div class='ioda-structure-container'>",
            "<details class='main-spec-toggle'>",
            "<summary style='font-size: 1.2em; font-weight: bold; cursor: pointer;'>",
            "📊 IODA Specification (Click to Expand/Collapse)",
            "</summary>",
            "<div style='padding: 15px; border-left: 3px solid #005eb8; margin-top: 10px;'>"
        ]

        # 1. Global Attributes Section
        if self._schema.get("global_attributes"):
            html.append("<h4>Global Attributes</h4>")
            html.append("<table class='structure-table'>")
            for k, v in self._schema["global_attributes"].items():
                html.append(f"<tr><td><strong>{k}</strong></td><td>{v}</td></tr>")
            html.append("</table><br>")

        # 2. Dimensions Section
        if self._schema.get("dimensions"):
            html.append("<h4>Dimensions</h4>")
            html.append("<ul>")
            for name, d in self._schema["dimensions"].items():
                html.append(f"<li><code>{name}</code>: {d['size']} {'(Unlimited)' if d['isunlimited'] else ''}</li>")
            html.append("</ul><br>")

        # 3. Groups and Variables (The detailed part)
        html.append("<h4>Groups & Variables</h4>")
        for group in self.get_groups():
            html.append(f"<details style='margin-left: 10px; margin-bottom: 5px;'>")
            html.append(f"<summary><strong>Group: {group}</strong></summary>")
            html.append("<table class='structure-table' style='width: 100%; font-size: 0.9em;'>")
            html.append("<thead><tr><th>Variable</th><th>Type</th><th>Dims</th><th>Attributes</th></tr></thead><tbody>")
     
            for var in self.get_vars_in_group(group):
                spec = self.get_var_spec(group, var)
                # Filter out None/empty attributes
                attr_list = [f"<i>{k}</i>: {v}" for k, v in spec.get('attributes', {}).items()]
                attr_str = "<br>".join(attr_list) if attr_list else "<i>None</i>"
     
                html.append("<tr>")
                html.append(f"<td style='vertical-align: top;'><code>{var}</code></td>")
                html.append(f"<td style='vertical-align: top;'>{spec['dtype']}</td>")
                html.append(f"<td style='vertical-align: top;'>{', '.join(spec['dimensions'])}</td>")
                html.append(f"<td><small>{attr_str}</small></td>")
                html.append("</tr>")

            html.append("</tbody></table></details>")

        # Close all the tags
        html.append("</div></details></div>")
        return "\n".join(html)
