import os
import shutil
import logging
from datetime import datetime
from pyobsforge.monitor.reporting.data_service import ReportDataService
from pyobsforge.monitor.reporting.plot_generator import PlotGenerator

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("WebGen")

# --- CSS STYLES ---
CSS_STYLES = """
body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; background: #f4f7f6; color: #333; }
header { background: #2c3e50; color: white; padding: 15px 20px; display: flex; justify-content: space-between; align-items: center; }
h1 { margin: 0; font-size: 1.5em; }
a { text-decoration: none; color: inherit; }

/* TABS */
.nav-tabs { display: flex; gap: 10px; background: #34495e; padding: 10px 20px; }
.nav-btn { color: #ecf0f1; padding: 8px 16px; border-radius: 4px; background: #2c3e50; font-weight: bold; transition: background 0.2s; }
.nav-btn.active { background: #3498db; color: white; }
.nav-btn:hover { background: #2980b9; }

/* LAYOUT */
.container { max-width: 1400px; margin: 20px auto; padding: 0 20px; }
.section { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.05); margin-bottom: 20px; }
h2 { border-bottom: 2px solid #eee; padding-bottom: 10px; margin-top: 0; color: #2c3e50; }
h3 { margin: 0 0 10px 0; color: #555; font-size: 1.1em; }

/* INVENTORY MATRIX */
table.matrix { width: 100%; border-collapse: collapse; font-size: 0.85em; }
th, td { padding: 6px 10px; border: 1px solid #eee; text-align: left; }
th { background: #f8f9fa; color: #7f8c8d; }
.status-OK { color: #27ae60; font-weight: bold; }
.status-FAIL { color: #e74c3c; font-weight: bold; }
.status-MIS { color: #95a5a6; }
.group-row { background: #eafaf1; color: #27ae60; font-weight: bold; cursor: default; }

/* PLOT GRID */
.plot-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(450px, 1fr)); gap: 20px; }
.plot-card { background: #fff; border: 1px solid #eee; padding: 10px; border-radius: 4px; text-align: center; transition: box-shadow 0.2s; }
.plot-card:hover { box-shadow: 0 4px 8px rgba(0,0,0,0.1); }
.plot-card img { max-width: 100%; height: auto; }
.no-plot { color: #999; font-style: italic; padding: 40px; background: #fafafa; }

/* TOGGLE SWITCH (Pure CSS) */
.toggle-control { text-align: right; margin-bottom: 10px; font-size: 0.9em; user-select: none; }
.toggle-label { cursor: pointer; color: #3498db; font-weight: bold; display: inline-flex; align-items: center; gap: 5px; }
.toggle-label:hover { color: #2980b9; }
input[type="checkbox"].history-toggle { display: none; }

/* Default: Show 7-Day. Checked: Show All. */
.plot-img-all { display: none; }
.plot-img-7d { display: block; }

#global-history-toggle:checked ~ .container .plot-img-all { display: block; }
#global-history-toggle:checked ~ .container .plot-img-7d { display: none; }
#global-history-toggle:checked ~ .container .toggle-text-7d { display: none; }
#global-history-toggle:checked ~ .container .toggle-text-all { display: inline; }
.toggle-text-all { display: none; }
"""

class WebsiteGenerator:
    def __init__(self, db_path, output_dir):
        self.reader = ReportDataService(db_path)
        self.output_dir = output_dir
        self.plots_dir = os.path.join(output_dir, "plots")
        self.plotter = PlotGenerator(self.plots_dir)
        
        # Clean build
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        os.makedirs(self.plots_dir)

    def generate(self):
        """Main build process."""
        logger.info("Starting Website Generation...")
        
        run_types = self.reader.get_all_run_types() # ['gdas', 'gfs']
        if not run_types:
            logger.warning("No run types found. DB might be empty.")
            return

        # 1. Landing Page (Redirects to first run type)
        with open(os.path.join(self.output_dir, "index.html"), "w") as f:
            f.write(f'<meta http-equiv="refresh" content="0; url={run_types[0]}.html">')

        # 2. Dashboards
        for rt in run_types:
            logger.info(f"Building Dashboard: {rt}")
            self._generate_dashboard(rt, run_types)

        logger.info(f"Complete. Open {self.output_dir}/index.html")

    def _generate_dashboard(self, current_run, all_runs):
        html = f"<!DOCTYPE html><html><head><title>ObsForge: {current_run.upper()}</title><style>{CSS_STYLES}</style></head><body>"
        
        # Header
        html += f"<header><h1>ObsForge Monitor <span style='font-weight:normal; opacity:0.8'>| {current_run.upper()}</span></h1><span>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}</span></header>"
        
        # Tabs
        html += "<div class='nav-tabs'>"
        for rt in all_runs:
            cls = "active" if rt == current_run else ""
            html += f"<a href='{rt}.html' class='nav-btn {cls}'>{rt.upper()}</a>"
        html += "</div>"

        # Global Toggle Checkbox (Hidden)
        html += "<input type='checkbox' id='global-history-toggle' class='history-toggle'>"
        
        # Main Container
        html += "<div class='container'>"
        
        # Toggle UI
        html += """
        <div class='toggle-control'>
            <label for='global-history-toggle' class='toggle-label'>
                <span style='font-size:1.2em'>&#128197;</span> 
                <span class='toggle-text-7d'>View: Last 7 Days (Click for History)</span>
                <span class='toggle-text-all'>View: Full History (Click for Recent)</span>
            </label>
        </div>
        """

        # Section A: Inventory Matrix
        html += self._render_inventory_section(current_run)

        # Section B: Task Timing
        html += self._render_timing_section(current_run)

        # Section C: Observations (Entry Points)
        html += self._render_category_section(current_run)

        html += "</div></body></html>" # Close Container/Body
        
        with open(os.path.join(self.output_dir, f"{current_run}.html"), "w") as f:
            f.write(html)

    # --- SECTIONS ---

    def _render_inventory_section(self, run_type):
        html = "<div class='section'><h2>Inventory Status</h2><div style='overflow-x:auto'><table class='matrix'>"
        html += "<thead><tr><th style='width:150px'>Cycle</th><th>Task Details</th></tr></thead><tbody>"
        
        matrix = self.reader.get_compressed_inventory(run_type, limit=50)
        
        for row in matrix:
            if row['type'] == 'group':
                label = f"&#9660; {row['start_date']} {row['start_cycle']:02d} &mdash; {row['end_date']} {row['end_cycle']:02d}"
                html += f"<tr class='group-row'><td>{label}</td><td>{row['count']} Cycles - All Tasks OK</td></tr>"
            else:
                cycle_str = f"{row['date']} {row['cycle']:02d}"
                task_html = []
                # Sort tasks alphabetically
                for t_name in sorted(row['tasks'].keys()):
                    status = row['tasks'][t_name]
                    cls = f"status-{status}" if status in ['OK', 'FAIL'] else "status-MIS"
                    task_html.append(f"<span class='{cls}'>{t_name}</span>")
                
                html += f"<tr><td><b>{cycle_str}</b></td><td>{' &nbsp;|&nbsp; '.join(task_html)}</td></tr>"
        
        html += "</tbody></table></div></div>"
        return html

    def _render_timing_section(self, run_type):
        html = "<div class='section'><h2>Task Performance</h2><div class='plot-grid'>"
        tasks = self.reader.get_all_task_names(run_type)
        
        count = 0
        for task in tasks:
            data = self.reader.get_task_timing_series(run_type, task, days=90)
            if not data: continue
            
            f_full, f_7d = self.plotter.generate_dual_plots(
                f"{task}", data, "runtime_sec", None, f"time_{run_type}_{task}", "Seconds"
            )
            
            html += f"<div class='plot-card'><h3>{task}</h3>"
            if f_full:
                html += f"<img src='plots/{f_7d}' class='plot-img-7d'><img src='plots/{f_full}' class='plot-img-all'>"
            else:
                html += "<div class='no-plot'>Plot unavailable</div>"
            html += "</div>"
            count += 1
            
        if count == 0: html += "<p>No timing data available.</p>"
        html += "</div></div>"
        return html

    def _render_category_section(self, run_type):
        html = "<div class='section'><h2>Observation Categories</h2><div class='plot-grid'>"
        cats = self.reader.get_all_categories()
        
        for cat in cats:
            # 1. Summary Plot
            data = self.reader.get_category_obs_sums(run_type, cat, days=90)
            if not data: continue
            
            fname_base = f"cat_{run_type}_{cat}"
            f_full, f_7d = self.plotter.generate_dual_volume_plots(
                f"{cat} Total Obs", data, "total_obs", fname_base, "Count"
            )
            
            # 2. Detail Page Generation
            detail_filename = f"detail_{run_type}_{cat}.html"
            self._generate_detail_page(run_type, cat, detail_filename)
            
            html += f"""
            <div class='plot-card'>
                <a href='{detail_filename}' style='text-decoration:none; color:inherit'>
                    <h3>{cat} &rarr;</h3>
            """
            if f_full:
                html += f"<img src='plots/{f_7d}' class='plot-img-7d'><img src='plots/{f_full}' class='plot-img-all'>"
            else:
                html += "<div class='no-plot'>Plot unavailable</div>"
            html += "</a></div>"
        html += "</div></div>"
        return html

    def _generate_detail_page(self, run_type, category, filename):
        obs_spaces = self.reader.get_obs_spaces_for_category(category)
        
        html = f"<!DOCTYPE html><html><head><title>{category}</title><style>{CSS_STYLES}</style></head><body>"
        html += f"<header><h1>{category} <span style='font-weight:normal'>| {run_type.upper()}</span></h1><a href='{run_type}.html' style='color:white; font-weight:bold'>&larr; Back</a></header>"
        
        html += "<input type='checkbox' id='global-history-toggle' class='history-toggle'><div class='container'>"
        html += "<div class='toggle-control'><label for='global-history-toggle' class='toggle-label'><span style='font-size:1.2em'>&#128197;</span> <span class='toggle-text-7d'>View: Last 7 Days</span><span class='toggle-text-all'>View: Full History</span></label></div>"

        for space in obs_spaces:
            html += f"<div class='section'><h2>{space}</h2><div class='plot-grid'>"
            
            # A. Counts
            c_data = self.reader.get_obs_space_counts(run_type, space, days=90)
            if c_data:
                f_c_full, f_c_7d = self.plotter.generate_dual_volume_plots(
                    "Obs Count", c_data, "count", f"{run_type}_{space}_cnt", "Count"
                )
                html += f"<div class='plot-card'><h3>Volume</h3>"
                if f_c_full:
                    html += f"<img src='plots/{f_c_7d}' class='plot-img-7d'><img src='plots/{f_c_full}' class='plot-img-all'>"
                else:
                    html += "<div class='no-plot'>No plot</div>"
                html += "</div>"

            # B. Physics
            schema = self.reader.get_obs_space_schema(space)
            # Find first variable in ObsValue
            phys_var = next((r['name'] for r in schema if r['group_name'] == 'ObsValue'), None)
            
            if phys_var:
                p_data = self.reader.get_variable_physics_series(run_type, space, phys_var, days=90)
                if p_data:
                    f_p_full, f_p_7d = self.plotter.generate_dual_plots(
                        f"{phys_var}", p_data, "mean_val", "std_dev", f"{run_type}_{space}_phys", "Value"
                    )
                    html += f"<div class='plot-card'><h3>{phys_var} (Mean &plusmn; &sigma;)</h3>"
                    if f_p_full:
                        html += f"<img src='plots/{f_p_7d}' class='plot-img-7d'><img src='plots/{f_p_full}' class='plot-img-all'>"
                    else:
                        html += "<div class='no-plot'>No plot</div>"
                    html += "</div>"
            
            html += "</div></div>"

        html += "</div></body></html>"
        with open(os.path.join(self.output_dir, filename), "w") as f:
            f.write(html)
