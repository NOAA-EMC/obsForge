import os
import logging
import json

from .css_styles import CSS_STYLES
from .ioda_html import IodaHTML
from processing.ioda_reader.ioda_structure import IodaStructure

logger = logging.getLogger(__name__)


class ObsSpaceGenerator:
    def __init__(self, output_dir, reader, data_root, website_data):
        self.output_dir = output_dir
        self.reader = reader
        self.data_root = data_root
        self.data = website_data

    def generate(self, run_type):
        """
        Main entry point called by WebGenerator.
        Iterates through all obs spaces and creates individual pages.
        """
        # Get all unique observation spaces for this run type
        categories = self.reader.get_all_categories()
     
        for category in categories:
            data = self.reader.get_category_counts(run_type, category, days=None)
            if not data:
                continue

            spaces_in_cat = self.reader.get_obs_spaces_for_category(category)

            for obs_space in spaces_in_cat:
                safe_name = obs_space.replace("/", "_").replace(" ", "_")
                filename = f"obs_{run_type}_{safe_name}.html"
                self._write_detail_page(run_type, obs_space, filename)

    def _write_detail_page(self, run_type, obs_space, filename):
        """Generates a dedicated page for a specific Obs Space."""

        cycles = self.reader.get_cycles_for_run(run_type)
        last_cycles = cycles[-4:]  # last 4 cycles
        current_cycle = cycles[-1] if cycles else None
        current_cycle_name = current_cycle["cycle_name"]


        page_path = os.path.join(self.output_dir, filename)
        # run_dashboard_path = os.path.join(self.output_dir, "..", f"index.html")
        run_dashboard_path = os.path.join("html", run_type, f"index.html")
        # plots_dir = os.path.join(self.output_dir, "..", "plots")

        # def _rel_path(target_file):
            # """Compute relative path from this page to the target."""
            # return os.path.relpath(target_file, start=os.path.dirname(page_path))

        schema = self.reader.get_obs_space_schema(obs_space)
        dom = self.reader.get_obs_space_domains(run_type, obs_space)

        # HTML Header & Title
        # <base> is needed for web and local links
        # back_link = _rel_path(run_dashboard_path)
        back_link = run_dashboard_path
        html = (
            f"<!DOCTYPE html><html><head><title>{obs_space} Details</title>"
            f'<base href="../../../">'
            f"<style>{CSS_STYLES}</style></head><body>"
            f"<header><h1>{run_type.upper()} <span style='font-weight:normal'>| {obs_space}</span></h1>"
            # f"<header><h1>{obs_space} <span style='font-weight:normal'>| {run_type.upper()}</span></h1>"
            f"<a href='{back_link}' style='color:white; font-weight:bold'>&larr; Back</a></header>"
            f"<div class='container'>"
        )

        # --- General Information (Metadata Table) ---
        html += "<div class='section'><h2>General Information</h2>"
        html += "<table class='flag-table' style='width: auto; min-width: 400px;'>"
        html += f"<tr><th>Observation Space</th><td>{obs_space}</td></tr>"
        html += f"<tr><th>Cycle</th><td>{current_cycle_name}</td></tr>"
        if dom:
            html += f"<tr><th>Latitude Range</th><td>[{dom.get('min_lat', 0):.1f}, {dom.get('max_lat', 0):.1f}]</td></tr>"
            html += f"<tr><th>Longitude Range</th><td>[{dom.get('min_lon', 0):.1f}, {dom.get('max_lon', 0):.1f}]</td></tr>"

            p_min = dom.get('pressure_min') or dom.get('air_pressure_min')
            p_max = dom.get('pressure_max') or dom.get('air_pressure_max')
            if p_min is not None:
                html += f"<tr><th>Pressure Range</th><td>{p_min:.1f} to {p_max:.1f} hPa</td></tr>"
        html += "</table></div>"

        # --- Physics Variables Section ---
        '''
        html += "<div class='section'><h2>All Observed Variables (Physics)</h2><div class='plot-grid'>"

        for var_info in schema:
            if var_info['group_name'] == 'ObsValue':
                var_name = var_info['name']
                # Commented out actual plot generation for now
                # p_data = self.reader.get_variable_physics_series(run_type, obs_space, var_name)
                # if p_data:
                #     f_full, _ = self.plotter.generate_dual_plots(
                #         f"{var_name} (Mean ± \u03C3)", p_data, "mean_val", "std_dev",
                #         f"deep_{run_type}_{obs_space}_{var_name}", "Value", clamp_bottom=False
                #     )
                html += f"<div class='plot-card'><h3>{var_name}</h3>"
                html += "<div class='no-plot'>Plot generation currently disabled</div>"
                html += "</div>"

        html += "</div></div>"
        '''

        # --- IODA Structure (JSON) ---

        ioda_info_file = self.data.get_product_absolute_path(
            "ioda_structure",
            obs_space,
            run_type,
            current_cycle_name,
        )

        if os.path.exists(ioda_info_file):
            ioda_structure = IodaStructure()
            ioda_structure.read_json(ioda_info_file)
            # html_fragment = ioda_struct.as_html()
            html_fragment = IodaHTML().render(ioda_structure)
            html += html_fragment
            # logger.info(f"Found ioda structure file {ioda_info_file}")
        else:
            logger.error(f"Missing ioda structure file {ioda_info_file}")


        # --- Recent Data Products (last 4 cycles) ---
        # if self.data_products:
        html += "<div class='section'><h2>Recent Data Products</h2><div class='plot-grid'>"

        for cycle in last_cycles:
            cycle_name = cycle["cycle_name"]
            plot_file = self.data.get_product_relative_path(
                "obs_space_var_data",
                obs_space,
                run_type,
                current_cycle_name,
            )   
            html += f"<div class='plot-card'><h3>Cycle {cycle_name}</h3>"
            html += f"<img src='{plot_file}' class='plot-img'></div>"

        html += "</div></div>"



        '''
            html += "<div class='section'><h2>IODA Summary</h2>"
            html += "<table class='flag-table' style='width:auto; min-width:300px;'>"

            surface = ioda_summary.get("surface")
            if surface:
                html += f"<tr><th>Variable</th><td>{surface.get('var_name', 'N/A')}</td></tr>"
                html += f"<tr><th>Units</th><td>{surface.get('units', 'N/A')}</td></tr>"
                html += f"<tr><th>Count</th><td>{surface.get('count', 0)}</td></tr>"
                html += f"<tr><th>Min</th><td>{surface.get('min', 'N/A')}</td></tr>"
                html += f"<tr><th>Max</th><td>{surface.get('max', 'N/A')}</td></tr>"
                html += f"<tr><th>Mean</th><td>{surface.get('mean', 'N/A')}</td></tr>"
                html += f"<tr><th>Std Dev</th><td>{surface.get('std', 'N/A')}</td></tr>"

            html += f"<tr><th>ObsValue Dim</th><td>{ioda_summary.get('obsvalue_dim', 'N/A')}</td></tr>"
            html += f"<tr><th>Effective Dim</th><td>{ioda_summary.get('effective_dim', 'N/A')}</td></tr>"
            html += "</table></div>"
        '''






        # --- Recent File History ---
        html += "<div class='section'><h2>Recent File History</h2><table class='flag-table'>"
        html += "<thead><tr><th>Cycle</th><th>Observations</th><th>Integrity</th></tr></thead><tbody>"

        history = self.reader.get_obs_space_counts(run_type, obs_space, days=5)
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

        os.makedirs(os.path.dirname(page_path), exist_ok=True)
        with open(page_path, "w") as f:
            f.write(html)
