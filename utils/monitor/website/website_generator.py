import logging
import os
import shutil
from datetime import datetime

from .css_styles import CSS_STYLES
from .category_pages import CategoryGenerator
from .obs_space_pages import ObsSpaceGenerator

from .data_manager import DataManager
from .data_service import ReportDataService

# to be removed:
from .website_structure import WebsiteStructure


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("WebGen")


class WebsiteGenerator:
    def __init__(self, *, db_path, data_root, data_products_root, output_dir):
        self.db_path = os.path.abspath(db_path)
        self.data_root = os.path.abspath(data_root)
        self.data_products_root = os.path.abspath(data_products_root)
        self.output_dir = os.path.abspath(output_dir)

        self.reader = ReportDataService(db_path)

        self.run_types = self.reader.get_all_run_types()
        if not self.run_types:
            logger.warning("No run types found in DB")
            return

        # WebsiteStructure needs to go....
        self.structure = WebsiteStructure(self.output_dir, self.run_types)


        self.local_data_dir = os.path.join(self.output_dir, "data")
        os.makedirs(self.local_data_dir, exist_ok=True)

        self.website_data = DataManager(
            products_root=self.data_products_root,
            web_data_root=self.local_data_dir
        )

        self.cycles = {}
        for rt in self.run_types:
            cycles = self.reader.get_cycles_for_run(rt)
            self.cycles[rt] = cycles[-4:]  # last 4 cycles

        for rt in self.run_types:
            for cycle in self.cycles[rt]:
                cycle_id = cycle["cycle_name"]  # or cycle.name
                self.website_data.fetch_all_products(rt, cycle_id)
                # self.website_data.fetch(rt, cycle_id)


    def generate(self):
        logger.info("Starting Website Generation...")
        if not self.run_types:
            return

        self.structure.create()

        # Top-level redirect index.html
        index_path = os.path.join(self.output_dir, "index.html")
        with open(index_path, "w") as f:
            f.write(f'<meta http-equiv="refresh" content="0; url=html/{self.run_types[0]}/index.html">')

        # Generate content for each run type
        for rt in self.run_types:
            logger.info(f"Generating website for run type: {rt}")

            self._generate_dashboard(rt)

            category_gen = CategoryGenerator(
                self.structure.categories_dir(rt),
                self.reader,
                self.website_data,
            )
            category_gen.generate(rt)

            obs_pages = ObsSpaceGenerator(
                self.structure.obsspaces_dir(rt),
                self.reader,
                self.data_root,
                self.website_data,
            )
            obs_pages.generate(rt)

        logger.info(f"Website generation complete. Open {index_path}")

    def _relative_path(self, from_path, to_path):
        """
        Return the relative path from from_path to to_path
        """
        return os.path.relpath(to_path, start=os.path.dirname(from_path))


    def _generate_dashboard(self, current_run):
        """Builds the main dashboard HTML for a specific run type."""

        # HTML Header
        run_root = self.structure.run_root(current_run)
        html = (
            f"<!DOCTYPE html><html><head>"
            f'<base href="../../">'
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

        # Navigation Tabs 
        html += "<div class='nav-tabs'>"
        for rt in self.run_types:
            cls = "active" if rt == current_run else ""
            link = f"html/{rt}/index.html"
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

    def _render_timing_section(self, run_type):
        """Generates Runtime performance plots (Mean ± σ)."""

        cycles = self.reader.get_cycles_for_run(run_type)
        last_cycles = cycles[-4:]  # last 4 cycles
        current_cycle = cycles[-1] if cycles else None
        current_cycle_name = current_cycle["cycle_name"]

        html = (
            "<div class='section'><h2>Task Performance (Mean ± σ)</h2>"
            "<div class='plot-grid'>"
        )
        tasks = self.reader.get_all_task_names(run_type)

        for task in tasks:
            cycle_id = current_cycle_name
            t_path = self.website_data.get_product_relative_path(
                "task_runtime",
                task,
                run_type,
                cycle_id,
            )
            t7_path = self.website_data.get_product_relative_path(
                "task_runtime7",
                task,
                run_type,
                cycle_id,
            )

            html += f"<div class='plot-card'><h3>{task}</h3>"
            html += (
                f"<img src='{t_path}' class='plot-img-all'>"
                f"<img src='{t7_path}' class='plot-img-7d'>"
            )
            html += "</div>"

        html += "</div></div>"
        return html

    def _render_category_section(self, run_type):
        """Generates Observation Category plots (Mean ± StdDev)."""

        cycles = self.reader.get_cycles_for_run(run_type)
        last_cycles = cycles[-4:]  # last 4 cycles
        current_cycle = cycles[-1] if cycles else None
        current_cycle_name = current_cycle["cycle_name"]

        html = (
            "<div class='section'><h2>Observation Categories (Total Obs)</h2>"
            "<div class='plot-grid'>"
        )

        categories = self.reader.get_all_categories()
     
        for category in categories:
            data = self.reader.get_category_counts(run_type, category, days=None)
            if not data:
                continue

            safe_cat = category.replace("/", "_").replace(" ", "_")
            detail_filename = f"{run_type}_{safe_cat}.html"
            cat_link = os.path.join("html", run_type, "categories", detail_filename)

            # html += f"""
            # <div class='plot-card'>
                # <a href='categories/{detail_filename}'
                   # style='text-decoration:none; color:inherit'>
                    # <h3>{category} &rarr;</h3>
            # """

            html += f"""
            <div class='plot-card'>
                <a href='{cat_link}'
                   style='text-decoration:none; color:inherit'>
                    <h3>{category} &rarr;</h3>
            """

            cycle_id = current_cycle_name
            c_path = self.website_data.get_product_relative_path(
                "category_volume",
                category,
                run_type,
                cycle_id,
            )
            c7_path = self.website_data.get_product_relative_path(
                "category_volume7",
                category,
                run_type,
                cycle_id,
            )

            html += (
                f"<img src='{c_path}' class='plot-img-all'>"
                f"<img src='{c7_path}' class='plot-img-7d'>"
            )
            html += "</a></div>"
        html += "</div></div>"
        return html
