import logging
import os
import shutil
from datetime import datetime

from .data_service import ReportDataService
from .plot_generator import PlotGenerator
from .css_styles import CSS_STYLES
from .category_pages import CategoryGenerator
from .obs_space_pages import ObsSpaceGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("WebGen")


class WebsiteGenerator:
    def __init__(self, *, db_path, data_root, output_dir):
        self.data_root = os.path.abspath(data_root)

        # 1. Services and Data
        self.reader = ReportDataService(db_path)
        
        # 2. Path Management
        self.output_dir = output_dir
        self.plots_dir = os.path.join(output_dir, "plots")
        self.cat_dir = os.path.join(output_dir, "categories")
        self.obs_dir = os.path.join(output_dir, "observations")

        # 3. Clean and Setup Directory Structure
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
        
        # Create all necessary subdirectories
        for directory in [self.plots_dir, self.cat_dir, self.obs_dir]:
            os.makedirs(directory, exist_ok=True)

        # 4. Initialize Sub-Generators (The Refactored Parts)
        # We pass the directory paths so they know where to save their files
        self.plotter = PlotGenerator(self.plots_dir)
        
        self.category_gen = CategoryGenerator(self.cat_dir, self.reader, self.plotter)
        self.obs_space_gen = ObsSpaceGenerator(self.obs_dir, self.reader, self.plotter, self.data_root)

    def generate(self):
        """Main execution method."""
        logger.info("Starting Website Generation...")

        # Get the unique run types (e.g., 'gdas', 'gfs')
        run_types = self.reader.get_all_run_types()
        if not run_types:
            logger.warning("No run types found. DB might be empty.")
            return

        # 1. Create index.html redirect to the first run type
        index_path = os.path.join(self.output_dir, "index.html")
        with open(index_path, "w") as f:
            f.write(
                f'<meta http-equiv="refresh" content="0; '
                f'url={run_types[0]}.html">'
            )   

        # 2. Build the site structure for each run type
        for rt in run_types:
            logger.info(f"Building Site for Run Type: {rt}")
            
            # A. Generate the Dashboard/Landing page for this run type
            # This is the top-level view showing all categories
            self._generate_dashboard(rt, run_types)

            # B. Generate the Category-specific pages
            # e.g., categories/gdas_conventional.html
            self.category_gen.generate(rt)

            # C. Generate the Observation Space detail pages
            # e.g., observations/gdas_temp_300.html
            # This is where your new Surface Plot logic will eventually sit
            self.obs_space_gen.generate(rt)

        logger.info(f"Complete. Open {self.output_dir}/index.html")



    def _generate_dashboard(self, current_run, all_runs):
        """Builds the main dashboard HTML for a specific run type."""

        # HTML Header
        html = ( 
            f"<!DOCTYPE html><html><head><title>ObsForge: {current_run.upper()}</title>"
            f"<style>{CSS_STYLES}</style></head><body>"
        )   

        # Title Bar with System Info
        gen_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        html += (
            f"<header>"
            f"<h1>ObsForge Monitor <span style='font-weight:normal; opacity:0.8'>| {current_run.upper()}</span></h1>"
            f"<div style='text-align: right; font-size: 0.85em; opacity: 0.9;'>"
            f"<div><b>Generated:</b> {gen_time}</div>"
            f"<div><b>Data Source:</b> <code style='background: rgba(255,255,255,0.1); padding: 2px 5px; border-radius: 3px;'>{self.data_root}</code></div>"
            f"</div>"
            f"</header>"
        )




        # # HTML Header
        # html = (
            # f"<!DOCTYPE html><html><head><title>"
            # f"ObsForge: {current_run.upper()}</title>"
            # f"<style>{CSS_STYLES}</style></head><body>"
        # )

        # # Title Bar
        # gen_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        # html += (
            # f"<header><h1>ObsForge Monitor "
            # f"<span style='font-weight:normal; opacity:0.8'>"
            # f"| {current_run.upper()}</span></h1>"
            # f"<span>Generated: {gen_time}</span></header>"
        # )

        # Navigation Tabs
        html += "<div class='nav-tabs'>"
        for rt in all_runs:
            cls = "active" if rt == current_run else ""
            html += f"<a href='{rt}.html' class='nav-btn {cls}'>{rt.upper()}</a>"
        html += "</div>"

        # Global Toggle Checkbox (Hidden state controller)
        html += (
            "<input type='checkbox' id='global-history-toggle' "
            "class='history-toggle'>"
        )

        # Main Content Container
        html += "<div class='container'>"

        # Toggle Switch UI
        html += """
        <div class='toggle-control'>
            <label for='global-history-toggle' class='toggle-label'>
                <span style='font-size:1.2em'>&#128197;</span>
                <span class='toggle-text-all'>
                    View: Full History (Click to zoom last 7 days)
                </span>
                <span class='toggle-text-7d'>
                    View: Last 7 Days (Click to see full history)
                </span>
            </label>
        </div>
        """

        # Section 1: Flagged Files (Anomalies)
        html += self._render_flagged_section(current_run)

        # Section 2: Inventory Matrix
        html += self._render_inventory_section(current_run)

        # Section 3: Task Performance Plots
        html += self._render_timing_section(current_run)

        # Section 4: Category Observation Plots
        html += self._render_category_section(current_run)

        # Footer/Close
        html += "</div></body></html>"

        # Write File
        with open(os.path.join(self.output_dir, f"{current_run}.html"), "w") as f:
            f.write(html)

    # --- SECTION RENDERERS ---

    def _render_flagged_section(self, run_type):
        """Generates the table of files with warnings/errors."""
        flagged = self.reader.get_flagged_files(run_type)
        if not flagged:
            return ""  # Hide section if clean

        html = (
            "<div class='section'><h2>&#9888; "
            "Flagged Files (Anomalies Detected)</h2>"
        )
        html += "<div class='flag-scroll-box'><table class='flag-table'>"
        html += (
            "<thead><tr><th>Cycle</th><th>File Path</th><th>Issue</th></tr>"
            "</thead><tbody>"
        )

        for f in flagged:
            cycle = f"{f['date']} {f['cycle']:02d}"
            short_path = f['file_path'].split('/')[-1]
            issue = f['error_message'] if f['error_message'] else "Unknown Issue"
            status = f['integrity_status']

            # Smart Color Mapping
            if status in ['CORRUPT', 'ERR_ACC', 'EMPTY']:
                css_class = "status-FAIL"  # Red
            elif status == 'WARNING':
                css_class = "status-WARNING"  # Yellow
            else:
                css_class = "status-MIS"  # Grey

            html += (
                f"<tr class='{css_class}'><td>{cycle}</td>"
                f"<td title='{f['file_path']}'>{short_path}</td>"
                f"<td><b>{issue}</b></td></tr>"
            )

        html += "</tbody></table></div></div>"
        return html

    def _render_inventory_section(self, run_type):
        """Generates the task status matrix with Legend."""
        html = "<div class='section'><h2>Inventory Status</h2>"

        # Legend
        html += """
        <div class='legend'>
            <span class='status-OK'>
                <span class='dot' style='background:#27ae60'></span>OK
            </span>
            <span class='status-WARNING'>
                <span class='dot' style='background:#f39c12'></span>
                Warning (Data Anomaly)
            </span>
            <span class='status-FAIL'>
                <span class='dot' style='background:#e74c3c'></span>
                Fail (Task Error)
            </span>
            <span class='status-MIS'>
                <span class='dot' style='background:#95a5a6'></span>
                Missing/Unknown
            </span>
        </div>
        """

        html += "<div style='overflow-x:auto'><table class='matrix'>"
        html += (
            "<thead><tr><th style='width:150px'>Cycle</th>"
            "<th>Task Details</th></tr></thead><tbody>"
        )

        # limit=None -> Fetch ALL history
        matrix = self.reader.get_compressed_inventory(run_type, limit=None)

        for row in matrix:
            if row['type'] == 'group':
                label = (
                    f"&#9660; {row['start_date']} {row['start_cycle']:02d} "
                    f"&mdash; {row['end_date']} {row['end_cycle']:02d}"
                )
                html += (
                    f"<tr class='group-row'><td>{label}</td>"
                    f"<td>{row['count']} Cycles - All Tasks OK & Files Valid</td>"
                    f"</tr>"
                )
            else:
                cycle_str = f"{row['date']} {row['cycle']:02d}"
                task_html = []
                for t_name in sorted(row['tasks'].keys()):
                    raw_status = row['tasks'][t_name]

                    # FIX: Map database status to display status
                    if raw_status == 'SUCCEEDED':
                        status = 'OK'
                    elif raw_status in ['FAILED', 'DEAD']:
                        status = 'FAIL'
                    elif raw_status == 'WARNING':
                        status = 'WARNING'
                    else:
                        status = raw_status

                    if status in ['OK', 'FAIL', 'WARNING']:
                        cls = f"status-{status}"
                    else:
                        cls = "status-MIS"  # Grey

                    task_html.append(f"<span class='{cls}'>{t_name}</span>")

                html += (
                    f"<tr><td><b>{cycle_str}</b></td>"
                    f"<td>{' &nbsp;|&nbsp; '.join(task_html)}</td></tr>"
                )

        html += "</tbody></table></div></div>"
        return html

    def _render_timing_section(self, run_type):
        """Generates Runtime performance plots (Mean ± σ)."""
        html = (
            "<div class='section'><h2>Task Performance (Mean ± σ)</h2>"
            "<div class='plot-grid'>"
        )
        tasks = self.reader.get_all_task_names(run_type)

        count = 0
        for task in tasks:
            data = self.reader.get_task_timing_series(run_type, task, days=None)
            if not data:
                continue

            # Pass std_key=None to force Temporal (Historical) bands
            f_full, f_7d = self.plotter.generate_dual_plots(
                f"{task}", data, "mean_runtime", None,
                f"time_{run_type}_{task}", "Seconds"
            )

            html += f"<div class='plot-card'><h3>{task}</h3>"
            if f_full:
                html += (
                    f"<img src='plots/{f_full}' class='plot-img-all'>"
                    f"<img src='plots/{f_7d}' class='plot-img-7d'>"
                )
            else:
                html += "<div class='no-plot'>Plot unavailable</div>"
            html += "</div>"
            count += 1

        if count == 0:
            html += "<p>No timing data available.</p>"
        html += "</div></div>"
        return html

    def _render_category_section(self, run_type):
        """Generates Observation Category plots (Mean ± StdDev)."""
        html = (
            "<div class='section'><h2>Observation Categories (Total Obs)</h2>"
            "<div class='plot-grid'>"
        )
        cats = self.reader.get_all_categories()

        for cat in cats:
            data = self.reader.get_category_counts(run_type, cat, days=None)
            if not data:
                continue

            # Pass std_key=None to force Temporal (Historical) bands
            fname_base = f"cat_{run_type}_{cat}"
            f_full, f_7d = self.plotter.generate_dual_plots(
                f"{cat} Total Obs", data, "total_obs", None, fname_base, "Count"
            )

            safe_cat = cat.replace("/", "_").replace(" ", "_")
            detail_filename = f"{run_type}_{safe_cat}.html"
            # detail_filename = f"detail_{run_type}_{cat}.html"
            # self._generate_detail_page(run_type, cat, detail_filename)

            html += f"""
            <div class='plot-card'>
                <a href='categories/{detail_filename}'
                   style='text-decoration:none; color:inherit'>
                    <h3>{cat} &rarr;</h3>
            """
            if f_full:
                html += (
                    f"<img src='plots/{f_full}' class='plot-img-all'>"
                    f"<img src='plots/{f_7d}' class='plot-img-7d'>"
                )
            else:
                html += "<div class='no-plot'>Plot unavailable</div>"
            html += "</a></div>"
        html += "</div></div>"
        return html
