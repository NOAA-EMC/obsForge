import os
import logging
from datetime import datetime
from .css_styles import CSS_STYLES

logger = logging.getLogger(__name__)

class CategoryGenerator:
    def __init__(self, output_dir, reader, website_data):
        self.output_dir = output_dir
        self.reader = reader
        self.data = website_data

    def generate(self, run_type):
        categories = self.reader.get_all_categories()
        
        for category in categories:
            data = self.reader.get_category_counts(run_type, category, days=None)
            if not data:
                continue

            safe_cat = category.replace("/", "_").replace(" ", "_")
            filename = f"{run_type}_{safe_cat}.html"
            self._write_category_page(run_type, category, filename)

    def _write_category_page(self, run_type, category, filename):
        """Generates the detail page for a category, listing all Obs Spaces with plots."""

        cycles = self.reader.get_cycles_for_run(run_type)
        last_cycles = cycles[-4:]  # last 4 cycles
        current_cycle = cycles[-1] if cycles else None
        current_cycle_name = current_cycle["cycle_name"]

        # Paths
        page_path = os.path.join(self.output_dir, filename)
        # run_dashboard_path = os.path.join(self.output_dir, "..", f"{run_type}.html")
        # run_dashboard_path = os.path.join(self.output_dir, "..", f"index.html")
        run_dashboard_path = os.path.join("html", run_type, f"index.html")
        # plots_dir = os.path.join(self.output_dir, "..", "plots")

        def _rel_path(target_file):
            """Helper to compute relative path from this page."""
            return os.path.relpath(target_file, start=os.path.dirname(page_path))

        # Obs spaces for this category
        obs_spaces = self.reader.get_obs_spaces_for_category(category)

        # HTML Header
        html = ( 
            f"<!DOCTYPE html><html><head><title>{category}</title>"
            f'<base href="../../../">'
            f"<style>{CSS_STYLES}</style></head><body>"
        )   

        # Header with dynamic Back Link
        back_link = _rel_path(run_dashboard_path)
        html += (
            f"<header><h1>{category} "
            f"<span style='font-weight:normal'>| {run_type.upper()}</span></h1>"
            f"<a href='{back_link}' style='color:white; font-weight:bold'>&larr; Back</a></header>"
        )   

        # Global Toggle Checkbox
        html += (
            "<input type='checkbox' id='global-history-toggle' "
            "class='history-toggle'><div class='container'>"
        )   
        
        # Toggle Switch UI
        html += """ 
        <div class='toggle-control'>
            <label for='global-history-toggle' class='toggle-label'>
                <span style='font-size:1.2em'>&#128197;</span>
                <span class='toggle-text-all'>View: Full History</span>
                <span class='toggle-text-7d'>View: Last 7 Days</span>
            </label>
        </div>
        """

        for obs_space in obs_spaces:
            # --- Domain Info ---
            dom = self.reader.get_obs_space_domains(run_type, obs_space)
            domain_html = ""

            schema_info = self.reader.get_obs_space_schema_details(obs_space)
            is_3d_profile = any(r.get('dimensionality', 0) >= 3 for r in schema_info)

            if dom:
                parts = []
                if dom.get('min_lat') is not None:
                    parts.append(f"<b>Lat:</b> [{dom['min_lat']:.1f}, {dom['max_lat']:.1f}] &nbsp; "
                                 f"<b>Lon:</b> [{dom['min_lon']:.1f}, {dom['max_lon']:.1f}]")
                if is_3d_profile and dom.get('depth_min') is not None:
                    parts.append(f"<b>Depth:</b> [{dom['depth_min']:.1f}, {dom['depth_max']:.1f}]")
                p_min = dom.get('pressure_min') if dom.get('pressure_min') is not None else dom.get('air_pressure_min')
                p_max = dom.get('pressure_max') if dom.get('pressure_max') is not None else dom.get('air_pressure_max')
                if p_min is not None:
                    parts.append(f"<b>Pressure:</b> [{p_min:.1f}, {p_max:.1f}]")
                if parts:
                    domain_html = f"<div class='domain-info'>{' &nbsp;|&nbsp; '.join(parts)}</div>"

            # Obs-space detail page filename
            safe_name = obs_space.replace("/", "_").replace(" ", "_")
            space_filename = f"obs_{run_type}_{safe_name}.html"
            # space_link = _rel_path(os.path.join(self.output_dir, "..", "observations", space_filename))
            space_link = os.path.join("html", run_type, "obs_spaces", space_filename)

            html += (
                f"<div class='section'>"
                f"<h2><a href='{space_link}'>{obs_space} &rarr;</a></h2>"
                f"{domain_html}<div class='plot-grid'>"
            )

            # --- Volume Plot ---
            cycle_id = current_cycle_name
            v_path = self.data.get_product_relative_path(
                "obs_space_volume",
                obs_space,
                run_type,
                cycle_id,
            )
            v7_path = self.data.get_product_relative_path(
                "obs_space_volume7",
                obs_space,
                run_type,
                cycle_id,
            )

            html += f"<div class='plot-card'><h3>Volume</h3>"
            html += (
                f"<img src='{v_path}' class='plot-img-all'>"
                f"<img src='{v7_path}' class='plot-img-7d'>"
            )
            html += "</div>"

            # --- Physics Plot ---
            m_path = self.data.get_product_relative_path(
                "obs_space_mean",
                obs_space,
                run_type,
                cycle_id,
            )
            m7_path = self.data.get_product_relative_path(
                "obs_space_mean7",
                obs_space,
                run_type,
                cycle_id,
            )

            html += f"<div class='plot-card'><h3>Mean/StdDev</h3>"
            html += (
                f"<img src='{v_path}' class='plot-img-all'>"
                f"<img src='{v7_path}' class='plot-img-7d'>"
            )
            html += "</div>"

            html += "</div></div>"  # Close plot-grid and section

        html += "</div></body></html>"

        # Write HTML file
        os.makedirs(os.path.dirname(page_path), exist_ok=True)
        with open(page_path, "w") as f:
            f.write(html)
