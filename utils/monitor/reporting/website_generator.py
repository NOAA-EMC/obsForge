import logging
import os
import shutil
from datetime import datetime

from .css_styles import CSS_STYLES
from .data_service import ReportDataService
from .website_structure import WebsiteStructure
from .plot_generator import PlotGenerator
from .category_pages import CategoryGenerator
from .obs_space_pages import ObsSpaceGenerator
from .data_products import DataProducts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("WebGen")


class WebsiteGenerator:
    def __init__(self, *, db_path, data_root, output_dir):
        self.data_root = os.path.abspath(data_root)
        self.output_dir = os.path.abspath(output_dir)

        self.reader = ReportDataService(db_path)

        self.run_types = self.reader.get_all_run_types()
        if not self.run_types:
            logger.warning("No run types found in DB")
            return

        # WebsiteStructure needs to go....
        self.structure = WebsiteStructure(self.output_dir, self.run_types)

    def generate(self):
        logger.info("Starting Website Generation...")
        if not self.run_types:
            return

        self.structure.create()

        # Top-level redirect index.html
        index_path = os.path.join(self.output_dir, "index.html")
        with open(index_path, "w") as f:
            f.write(f'<meta http-equiv="refresh" content="0; url=runs/{self.run_types[0]}/index.html">')

        # Generate content for each run type
        for rt in self.run_types:
            logger.info(f"Generating website for run type: {rt}")

            run_root = self.structure.run_root(rt)
            data_products_dir = os.path.join(run_root, "data_products")

            plotter = PlotGenerator(self.structure.plots_dir(rt))

            data_products = DataProducts(
                self.data_root,
                self.reader,
                data_products_dir
            )

            self._generate_dashboard(rt, plotter)

            category_gen = CategoryGenerator(
                self.structure.categories_dir(rt),
                self.reader,
                plotter
            )
            category_gen.generate(rt)

            obs_pages = ObsSpaceGenerator(
                self.structure.obsspaces_dir(rt),
                self.reader,
                plotter,
                self.data_root,
                data_products
            )
            obs_pages.generate(rt)

        logger.info(f"Website generation complete. Open {index_path}")

    def _relative_path(self, from_path, to_path):
        """
        Return the relative path from from_path to to_path
        """
        return os.path.relpath(to_path, start=os.path.dirname(from_path))


    def _generate_dashboard(self, current_run, plotter):
        """Builds the main dashboard HTML for a specific run type."""

        # HTML Header
        run_root = self.structure.run_root(current_run)
        html = (
            f"<!DOCTYPE html><html><head>"
            f"<title>ObsForge: {current_run.upper()}</title>"
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
        for rt in self.run_types:
            cls = "active" if rt == current_run else ""
            link = f"../{rt}/index.html"
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
        html += self._render_timing_section(current_run, plotter)
        html += self._render_category_section(current_run, plotter)

        # Close container
        html += "</div></body></html>"

        # Write dashboard
        with open(os.path.join(run_root, "index.html"), "w") as f:
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

    def _render_timing_section(self, run_type, plotter):
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
            f_full, f_7d = plotter.generate_dual_plots(
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

    def _render_category_section(self, run_type, plotter):
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
            f_full, f_7d = plotter.generate_dual_plots(
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
