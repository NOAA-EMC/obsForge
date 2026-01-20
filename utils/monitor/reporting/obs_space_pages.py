import os
import logging

from .css_styles import CSS_STYLES
from .obs_space_reader import ObsSpaceReader

logger = logging.getLogger(__name__)


class ObsSpaceGenerator:
    def __init__(self, obs_dir, reader, plotter, data_root):
        """
        Initializes the Observation Detail Generator.
        
        :param obs_dir: Path to the 'observations' directory.
        :param reader: Instance of ReportDataService.
        :param plotter: Instance of PlotGenerator.
        """
        self.obs_dir = obs_dir
        self.reader = reader
        self.plotter = plotter
        self.data_root = data_root

        self.obs_reader = ObsSpaceReader()

    def generate(self, run_type):
        """
        Main entry point called by WebGenerator.
        Iterates through all obs spaces and creates individual deep-dive pages.
        """
        # Get all unique observation spaces for this run type
        categories = self.reader.get_all_categories()
     
        for category in categories:
            data = self.reader.get_category_counts(run_type, category, days=None)
            if not data:
                continue

            spaces_in_cat = self.reader.get_obs_spaces_for_category(category)

            for space in spaces_in_cat:
                safe_name = space.replace("/", "_").replace(" ", "_")
                filename = f"obs_{run_type}_{safe_name}.html"
                self._write_detail_page(run_type, space, filename)
        
    def _write_detail_page(self, run_type, space, filename):
        """Generates a dedicated deep-dive page for a specific Obs Space."""
        schema = self.reader.get_obs_space_schema(space)
        dom = self.reader.get_obs_space_domains(run_type, space)
     
        # HTML Header & Title
        html = ( 
            f"<!DOCTYPE html><html><head><title>{space} Details</title>"
            f"<style>{CSS_STYLES}</style></head><body>"
            f"<header><h1>{space} <span style='font-weight:normal'>| {run_type.upper()} Deep Dive</span></h1>"
            f"<a href='javascript:history.back()' style='color:white; font-weight:bold'>&larr; Back</a></header>"
            f"<div class='container'>"
        )   

        # --- NEW SECTION: General Information (Metadata Table) ---
        html += "<div class='section'><h2>General Information</h2>"
        html += "<table class='flag-table' style='width: auto; min-width: 400px;'>"
        html += f"<tr><th>Observation Space</th><td>{space}</td></tr>"
        if dom:
            html += f"<tr><th>Latitude Range</th><td>[{dom.get('min_lat', 0):.1f}, {dom.get('max_lat', 0):.1f}]</td></tr>"
            html += f"<tr><th>Longitude Range</th><td>[{dom.get('min_lon', 0):.1f}, {dom.get('max_lon', 0):.1f}]</td></tr>"
            
            p_min = dom.get('pressure_min') or dom.get('air_pressure_min')
            p_max = dom.get('pressure_max') or dom.get('air_pressure_max')
            if p_min is not None:
                html += f"<tr><th>Pressure Range</th><td>{p_min:.1f} to {p_max:.1f} hPa</td></tr>"
        html += "</table></div>"

        # --- SECTION A: Full Variable Analysis (From your code) ---
        html += "<div class='section'><h2>All Observed Variables (Physics)</h2><div class='plot-grid'>"
     
        # Loop through all variables in the 'ObsValue' group
        for var_info in schema:
            if var_info['group_name'] == 'ObsValue':
                var_name = var_info['name']
                p_data = self.reader.get_variable_physics_series(run_type, space, var_name)
     
                if p_data:
                    # Uses your signature for generate_dual_plots
                    f_full, _ = self.plotter.generate_dual_plots(
                        f"{var_name} (Mean ± \u03C3)", p_data, "mean_val", "std_dev",
                        f"deep_{run_type}_{space}_{var_name}", "Value", clamp_bottom=False
                    )
                    html += f"<div class='plot-card'><h3>{var_name}</h3>"
                    if f_full:
                        # Path points up to /plots/
                        html += f"<img src='../plots/{f_full}' style='width:100%'>"
                    else:
                        html += "<div class='no-plot'>No plot data</div>"
                    html += "</div>"

        html += "</div></div>"

        # --- NEW SECTION: Surface Distribution (Requirement) ---
        # Logic: If it's a surface obs (not 3D), show the earth plot placeholder
        schema_details = self.reader.get_obs_space_schema_details(space)
        is_3d = any(r.get('dimensionality', 0) >= 3 for r in schema_details)
        
        if not is_3d:
            # html += "<div class='section'><h2>Global Surface Distribution</h2>"
            # html += "<div class='plot-card' style='max-width: 900px; margin: auto;'>"
            # html += "<p style='text-align:center; padding: 20px; color:#666;'>[Static Surface Map: Past Cycle Distribution]</p>"
            # # Future image tag: 
            # # html += f"<img src='../plots/surface_{run_type}_{space}.png' style='width:100%'>"
            # html += "</div></div>"




            recent_files = self.reader.get_recent_files_info(run_type, space, limit=4)

            if recent_files:
                html += "<div class='section'><h2>Spatial Distribution (Past 4 Cycles)</h2>"
                html += "<div class='plot-grid'>"

                for file_info in recent_files:
                    full_nc_path = os.path.join(self.data_root, file_info['file_path'])
                    
                    data = self.obs_reader.get_surface_data(full_nc_path)
                    
                    if data:
                        # Generate unique filename for the web plots
                        cycle_label = f"{file_info['date']}_{file_info['cycle']:02d}"
                        plot_filename = self.plotter.generate_surface_map(
                            run_type, 
                            f"{space}_{cycle_label}", 
                            data['lats'], 
                            data['lons'], 
                            data['values'], 
                            units=data['units']
                        )

                        html += f"""
                        <div class='plot-card'>
                            <h3>Cycle: {file_info['date']} {file_info['cycle']:02d}z</h3>
                            <img src='../plots/{plot_filename}' style='width:100%'>
                        </div>
                        """
                    else:
                        # Error handling if the file is still missing on disk
                        html += f"""
                        <div class='plot-card'>
                            <h3>Cycle: {file_info['date']} {file_info['cycle']:02d}z</h3>
                            <div style='height:200px; display:flex; align-items:center; justify-content:center; background:#f8d7da; color:#721c24; border-radius:4px;'>
                                File Not Found on Disk
                            </div>
                        </div>
                        """












        # --- SECTION B: Recent File Inventory (From your code) ---
        html += "<div class='section'><h2>Recent File History</h2><table class='flag-table'>"
        html += "<thead><tr><th>Cycle</th><th>Observations</th><th>Integrity</th></tr></thead><tbody>"

        # Get history (last 5 days) and show last 10 entries
        history = self.reader.get_obs_space_counts(run_type, space, days=5)
        if history:
            for h in reversed(history[-10:]):
                status_cls = "status-OK" if h['total_obs'] > 0 else "status-WARNING"
                html += (
                    f"<tr><td>{h['date']} {h['cycle']:02d}z</td>"
                    f"<td>{h['total_obs']:,}</td>"
                    f"<td><span class='{status_cls}'>VALID</span></td></tr>"
                )
        else:
            html += "<tr><td colspan='3'>No history available</td></tr>"

        html += "</tbody></table></div></div></body></html>"

        # Write file to /observations/ folder
        with open(os.path.join(self.obs_dir, filename), "w") as f:
            f.write(html)
