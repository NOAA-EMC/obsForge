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
        self.output_dir = os.path.abspath(output_dir)

        # 1. Services
        self.reader = ReportDataService(db_path)

        # 2. Sub-generators (output_dir assigned later per run type)
        self.plotter = PlotGenerator(None)
        self.category_gen = CategoryGenerator(None, self.reader, self.plotter)
        self.obs_space_gen = ObsSpaceGenerator(None, self.reader, self.plotter, self.data_root)

        # 3. Prepare run paths dict
        self.run_paths = {}
        self.run_types = []

        # 4. Create website directory structure (archiving-ready)
        # This will create top-level dirs; per-run dirs created in _create_website_dir()
        # No deletion, safe for future incremental updates
        # Optional: call this here for top-level structure, per-run created in generate()
        os.makedirs(self.output_dir, exist_ok=True)


    def _create_website_dir(self):
        """
        Create/update website directories for all run types.
        Sets self.run_types and self.run_paths.
        """
        logger.info(f"Preparing website directories at {self.output_dir}")

        os.makedirs(self.output_dir, exist_ok=True)

        # Get all run types from DB
        self.run_types = self.reader.get_all_run_types()
        if not self.run_types:
            logger.warning("No run types found in DB")
            return

        # Top-level runs directory
        runs_dir = os.path.join(self.output_dir, "runs")
        os.makedirs(runs_dir, exist_ok=True)

        self.run_paths = {}

        for rt in self.run_types:
            run_root = os.path.join(runs_dir, rt)
            plots_dir = os.path.join(run_root, "plots")
            cat_dir = os.path.join(run_root, "categories")
            obs_dir = os.path.join(run_root, "observations")

            # Create missing dirs only, preserve existing content
            for d in [run_root, plots_dir, cat_dir, obs_dir]:
                os.makedirs(d, exist_ok=True)

            self.run_paths[rt] = {
                "run_root": run_root,
                "plots": plots_dir,
                "categories": cat_dir,
                "observations": obs_dir
            }

    def generate(self):
        logger.info("Starting Website Generation...")

        # 1. Create/update per-run directories and set self.run_types
        self._create_website_dir()
        if not self.run_types:
            return

        # 2. Create top-level index.html redirect to first run type
        index_path = os.path.join(self.output_dir, "index.html")
        with open(index_path, "w") as f:
            f.write(
                f'<meta http-equiv="refresh" content="0; url=runs/{self.run_types[0]}/index.html">'
            )

        # 3. Generate content for each run type
        for rt in self.run_types:
            logger.info(f"Generating website for run type: {rt}")
            dirs = self.run_paths[rt]

            # Assign directories to sub-generators for this run type
            self.plotter.output_dir = dirs["plots"]
            self.category_gen.output_dir = dirs["categories"]
            self.obs_space_gen.output_dir = dirs["observations"]

            # Generate dashboard, categories, obs space pages
            # self._generate_dashboard(rt, self.run_types, output_root=dirs["run_root"])
            self._generate_dashboard(rt, self.run_types)

            self.category_gen.generate(rt)
            self.obs_space_gen.generate(rt)

        logger.info(f"Website generation complete. Open {index_path}")

    def _relative_path(self, from_path, to_path):
        """
        Return the relative path from from_path to to_path
        """
        return os.path.relpath(to_path, start=os.path.dirname(from_path))


    def _generate_dashboard(self, current_run, all_runs):
        """Builds the main dashboard HTML for a specific run type."""

        # HTML Header
        html = ( 
            f"<!DOCTYPE html><html><head><title>ObsForge: {current_run.upper()}</title>"
            f"<style>{CSS_STYLES}</style></head><body>"
        )   

        # Title Bar
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

        # Navigation Tabs (legacy layout)
        html += "<div class='nav-tabs'>"
        for rt in all_runs:
            cls = "active" if rt == current_run else ""
            link = f"{rt}.html"
            html += f"<a href='{link}' class='nav-btn {cls}'>{rt.upper()}</a>"
        html += "</div>"

        # Global toggle
        html += "<input type='checkbox' id='global-history-toggle' class='history-toggle'>"

        # Main content container
        html += "<div class='container'>"
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

        # Sections
        html += self._render_flagged_section(current_run)
        html += self._render_inventory_section(current_run)
        html += self._render_timing_section(current_run)
        html += self._render_category_section(current_run)

        # Close container
        html += "</div></body></html>"

        # Write dashboard
        filename = f"{current_run}.html"
        with open(os.path.join(self.output_dir, filename), "w") as f:
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

