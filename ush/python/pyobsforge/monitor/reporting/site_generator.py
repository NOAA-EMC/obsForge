import os
import shutil
import logging
from collections import defaultdict

# Optional Matplotlib import for environments without display
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

from pyobsforge.monitor.database.db_reader import DBReader
from pyobsforge.monitor.reporting.inventory_report import InventoryReport

logger = logging.getLogger("SiteGenerator")

class SiteGenerator:
    def __init__(self, db_path, output_dir):
        self.reader = DBReader(db_path)
        self.out_dir = output_dir
        self.img_root = os.path.join(output_dir, "img")
        self.css_dir = os.path.join(output_dir, "css")
        
        # Dynamically determine which run types exist (e.g. gdas, gfs)
        raw = self.reader.get_cycle_ranges()
        self.run_types = sorted(list(raw.keys())) if raw else []

    def build(self):
        print(f"Building Site in: {self.out_dir}")
        self._setup_structure()
        
        # 1. Generate Plots
        if HAS_MATPLOTLIB:
            print("... Generating Charts (Timings & Obs)")
            plots_meta = self._generate_deep_plots()
        else:
            print("... Skipping Charts (Matplotlib missing)")
            plots_meta = defaultdict(lambda: defaultdict(list))

        # 2. Write HTML Pages
        print("... Writing Pages")
        self._write_index()
        
        for rtype in self.run_types:
            self._write_run_type_page(rtype, plots_meta[rtype])

        print(f"Done. Open file://{os.path.abspath(os.path.join(self.out_dir, 'index.html'))}")

    def _setup_structure(self):
        os.makedirs(self.css_dir, exist_ok=True)
        # Create image directories for each run type
        for rtype in self.run_types:
            os.makedirs(os.path.join(self.img_root, rtype, "timings"), exist_ok=True)
            os.makedirs(os.path.join(self.img_root, rtype, "obs"), exist_ok=True)
        self._write_css()

    def _write_css(self):
        css = """
        body { font-family: 'Segoe UI', sans-serif; background: #f4f7f6; color: #333; margin: 0; }
        header { background: #2c3e50; color: white; padding: 15px 30px; }
        header h1 { margin: 0; font-size: 1.5em; }
        nav { margin-top: 10px; }
        nav a { color: #bdc3c7; text-decoration: none; margin-right: 20px; font-weight: bold; font-size: 1.1em; }
        nav a:hover { color: white; }
        nav a.active { color: white; border-bottom: 2px solid #3498db; }
        
        .container { padding: 20px; max_width: 1600px; margin: 0 auto; }
        .section { background: white; padding: 20px; margin-bottom: 30px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.05); }
        h2 { border-bottom: 2px solid #eee; padding-bottom: 10px; color: #2c3e50; }
        h3 { color: #555; margin-top: 30px; background: #eee; padding: 10px; border-radius: 4px; }
        
        /* Tables */
        table { width: 100%; border-collapse: collapse; font-size: 0.85em; }
        th, td { padding: 8px; border: 1px solid #ddd; text-align: left; }
        th { background: #f8f9fa; color: #333; }
        
        /* Status Codes */
        .status-ok { background: #d4edda; color: #155724; text-align: center; font-weight: bold; }
        .status-mis { background: #f8d7da; color: #721c24; text-align: center; font-weight: bold; } /* FAIL/DEAD */
        .status-dat { background: #d1ecf1; color: #0c5460; text-align: center; font-weight: bold; } /* RUN */
        .status-none { color: #ccc; text-align: center; }
        
        /* File Inventory Table */
        .file-table td { font-family: 'Consolas', monospace; font-size: 0.9em; }
        .bad-file { color: #721c24; background-color: #f8d7da; font-weight: bold; }
        .empty-file { color: #856404; background-color: #fff3cd; font-weight: bold; }
        .file-path { max-width: 600px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; display: block; }
        
        /* Plots Grid */
        .plot-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(600px, 1fr)); gap: 20px; }
        .plot-card { border: 1px solid #eee; padding: 15px; text-align: center; background: #fff; border-radius: 4px; }
        .plot-card img { width: 100%; height: auto; }
        .plot-title { font-weight: bold; margin-bottom: 10px; color: #555; text-transform: uppercase; font-size: 0.85em; letter-spacing: 1px; }
        
        .alert { padding: 15px; background-color: #e2e3e5; color: #383d41; border-radius: 4px; text-align: center; }
        """
        with open(os.path.join(self.css_dir, "style.css"), "w") as f: f.write(css)

    def _generate_deep_plots(self):
        """
        Generates static PNG plots for:
        1. Task Runtimes (All Tasks)
        2. Observation Counts (All Categories)
        """
        meta = defaultdict(lambda: defaultdict(list))
        tasks = self.reader.get_task_list()
        categories = self.reader.get_all_categories()

        for rtype in self.run_types:
            # 1. Task Timings
            for task in tasks:
                data = self.reader.get_task_timings(days=None, task_name=task, run_type=rtype)
                if not data or len(data) < 2: continue # Skip empty/single-point data
                
                fname = f"img/{rtype}/timings/{task}_time.png"
                self._plot_line(
                    data, 'duration', f"{task} - Runtime", "Seconds",
                    os.path.join(self.out_dir, fname), '#2980b9'
                )
                meta[rtype]['timings'].append({'title': task, 'file': fname})

            # 2. Category Obs Counts
            for cat in categories:
                data = self.reader.get_obs_counts_by_category(cat, days=None, run_type=rtype)
                if not data or len(data) < 2: continue
                
                fname = f"img/{rtype}/obs/{cat}_obs.png"
                self._plot_line(
                    data, 'count', f"{cat} - Total Observations", "Count",
                    os.path.join(self.out_dir, fname), '#27ae60'
                )
                meta[rtype]['obs_cat'].append({'title': cat, 'file': fname})
                
        return meta

    def _plot_line(self, data, y_key, title, ylabel, out_path, color):
        # Prepare Data: Flatten date/cycle into a string label
        # Sorting is handled by DBReader query
        dates = [f"{d['date']}.{d['cycle']:02d}" for d in data]
        values = [d[y_key] for d in data]
        
        plt.figure(figsize=(10, 5))
        plt.plot(dates, values, marker='o', markersize=4, linewidth=2, color=color)
        plt.title(title, fontsize=12, fontweight='bold')
        plt.ylabel(ylabel)
        plt.grid(True, linestyle='--', alpha=0.5)
        
        # Intelligent X-Axis Ticks
        n = len(dates)
        if n > 1:
            # Show max 15 labels to prevent crowding
            step = max(1, n // 15)
            idxs = range(0, n, step)
            plt.xticks(idxs, [dates[i] for i in idxs], rotation=45, ha='right', fontsize=9)
        
        plt.tight_layout()
        plt.savefig(out_path, dpi=100)
        plt.close()

    def _write_index(self):
        nav = self._get_nav_html("home")
        inv_html = ""
        
        # High-Level Status Matrix
        for rtype in self.run_types:
            report = InventoryReport(self.reader, run_type_filter=rtype, limit=20)
            t = report.render_html()
            if "No data" not in t: 
                inv_html += f"<h3>{rtype.upper()} Recent Cycles</h3>{t}"
            else:
                inv_html += f"<h3>{rtype.upper()}</h3><p>No recent data.</p>"

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>ObsForge Dashboard</title>
            <link rel="stylesheet" href="css/style.css">
        </head>
        <body>
            <header>
                <h1>ObsForge Monitor</h1>
                {nav}
            </header>
            <div class="container">
                <div class="section">
                    <h2>System Status</h2>
                    {inv_html}
                </div>
            </div>
        </body>
        </html>
        """
        with open(os.path.join(self.out_dir, "index.html"), "w") as f: f.write(html)

    def _write_run_type_page(self, rtype, meta):
        nav = self._get_nav_html(rtype)
        
        # --- LATEST CYCLE DRILL-DOWN ---
        matrix = self.reader.get_inventory_matrix(run_type_filter=rtype, limit=1)
        latest_files_html = "<div class='alert'>No recent cycle data found.</div>"
        
        if matrix:
            top = matrix[0]
            date, cycle = top['date'], top['cycle']
            
            # Use the new schema-aware reader method
            files = self.reader.get_files_for_cycle(date, cycle, rtype)
            
            if files:
                rows = []
                for f in files:
                    status = f['integrity_status']
                    
                    # Size formatting
                    size_bytes = f['file_size_bytes']
                    if size_bytes > 1024*1024:
                        size_str = f"{size_bytes / (1024*1024):.2f} MB"
                    elif size_bytes > 1024:
                        size_str = f"{size_bytes / 1024:.1f} KB"
                    else:
                        size_str = f"{size_bytes} B"

                    # Row styling
                    row_cls = ""
                    if status == "CORRUPT": row_cls = "bad-file"
                    elif status == "EMPTY": row_cls = "empty-file"
                    elif status == "MISSING": row_cls = "bad-file"
                    elif status == "METADATA_MISMATCH": row_cls = "bad-file" # If using learner logic
                    
                    obs_display = f"{f['obs_count']:,}" if f['obs_count'] is not None else "-"
                    
                    rows.append(f"""
                        <tr class="{row_cls}">
                            <td>{f['task']}</td>
                            <td><span class="file-path" title="{f['file_path']}">{f['file_path']}</span></td>
                            <td>{status}</td>
                            <td>{obs_display}</td>
                            <td>{size_str}</td>
                        </tr>
                    """)
                
                latest_files_html = f"""
                    <h3>Latest Cycle Inventory: {date} {cycle:02d}</h3>
                    <div style="overflow-x:auto;">
                        <table class="file-table">
                            <thead><tr>
                                <th width="15%">Task</th>
                                <th width="50%">File Path</th>
                                <th width="10%">Status</th>
                                <th width="10%">Obs Count</th>
                                <th width="15%">Size</th>
                            </tr></thead>
                            <tbody>{''.join(rows)}</tbody>
                        </table>
                    </div>
                """
            else:
                latest_files_html = f"<div class='alert'>Cycle {date} {cycle:02d} found, but no files in inventory.</div>"

        # --- PLOTS HTML ---
        if meta['timings']:
            timings = "<div class='plot-grid'>" + "".join([f"<div class='plot-card'><div class='plot-title'>{x['title']}</div><img src='{x['file']}'></div>" for x in meta['timings']]) + "</div>"
        else:
            timings = "<p>No timing data available.</p>"

        if meta['obs_cat']:
            obs = "<div class='plot-grid'>" + "".join([f"<div class='plot-card'><div class='plot-title'>{x['title']}</div><img src='{x['file']}'></div>" for x in meta['obs_cat']]) + "</div>"
        else:
            obs = "<p>No observation data available.</p>"
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{rtype.upper()} Analysis</title>
            <link rel="stylesheet" href="css/style.css">
        </head>
        <body>
            <header>
                <h1>{rtype.upper()} Analysis</h1>
                {nav}
            </header>
            <div class="container">
                <div class="section">
                    <h2>Latest Cycle Inventory</h2>
                    {latest_files_html}
                </div>
                <div class="section">
                    <h2>Task Performance</h2>
                    {timings}
                </div>
                <div class="section">
                    <h2>Observation Volume Trends</h2>
                    {obs}
                </div>
            </div>
        </body>
        </html>
        """
        with open(os.path.join(self.out_dir, f"{rtype}.html"), "w") as f: f.write(html)

    def _get_nav_html(self, active):
        links = [('<a href="index.html">Dashboard</a>', 'home')] + \
                [(f'<a href="{r}.html">{r.upper()}</a>', r) for r in self.run_types]
        
        html_links = []
        for link_str, name in links:
            if name == active:
                # Insert class="active" into the anchor tag
                html_links.append(link_str.replace('href=', 'class="active" href='))
            else:
                html_links.append(link_str)
        
        return "<nav>" + "".join(html_links) + "</nav>"
