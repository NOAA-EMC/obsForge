import os
import shutil
import statistics  # NEW: For stats calculation
from datetime import datetime
from collections import defaultdict

# Non-interactive backend for server environments
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

from pyobsforge.monitor.database.db_reader import DBReader
from pyobsforge.monitor.reporting.inventory_report import InventoryReport

class SiteGenerator:
    def __init__(self, db_path, output_dir):
        self.reader = DBReader(db_path)
        self.out_dir = output_dir
        self.img_root = os.path.join(output_dir, "img")
        self.css_dir = os.path.join(output_dir, "css")
        
        # Get all run types from DB
        self.run_types = sorted(list(self.reader.get_cycle_ranges().keys()))
        if not self.run_types: self.run_types = ['gdas', 'gfs']

    def build(self):
        print(f"Building Full Site in: {self.out_dir}")
        self._setup_structure()
        
        if HAS_MATPLOTLIB:
            print("... Generating Charts (This may take a moment)")
            plots_meta = self._generate_deep_plots()
        else:
            print("... Skipping Charts (Matplotlib missing)")
            plots_meta = defaultdict(lambda: defaultdict(list))

        print("... Writing Pages")
        self._write_index()
        for rtype in self.run_types:
            self._write_run_type_page(rtype, plots_meta[rtype])

        print(f"Done. To view: open {os.path.join(self.out_dir, 'index.html')}")

    def _setup_structure(self):
        os.makedirs(self.css_dir, exist_ok=True)
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
        nav a { color: #ecf0f1; text-decoration: none; margin-right: 20px; font-weight: bold; font-size: 1.1em;}
        nav a:hover { text-decoration: underline; color: #3498db; }
        .container { padding: 20px; max_width: 1600px; margin: 0 auto; }
        .section { background: white; padding: 20px; margin-bottom: 30px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.05); }
        h2 { border-bottom: 2px solid #eee; padding-bottom: 10px; color: #2c3e50; }
        h3 { color: #555; margin-top: 30px; background: #eee; padding: 10px; border-radius: 4px; }
        table.inventory-table { width: 100%; border-collapse: collapse; font-size: 0.85em; }
        th, td { padding: 8px; border: 1px solid #ddd; text-align: center; }
        th { background: #f1f1f1; }
        .status-ok { background: #d4edda; color: #155724; }
        .status-log { background: #fff3cd; color: #856404; }
        .status-dat { background: #d1ecf1; color: #0c5460; }
        .status-mis { background: #f8d7da; color: #721c24; }
        .plot-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(500px, 1fr)); gap: 20px; }
        .plot-card { border: 1px solid #eee; padding: 10px; text-align: center; }
        .plot-card img { width: 100%; height: auto; }
        .plot-title { font-weight: bold; margin-bottom: 5px; color: #444; font-size: 0.9em; }
        """
        with open(os.path.join(self.css_dir, "style.css"), "w") as f: f.write(css)

    # ------------------------------------------------------------------
    # PLOTTING ENGINE (Updated with Stats)
    # ------------------------------------------------------------------
    def _generate_deep_plots(self):
        meta = defaultdict(lambda: defaultdict(list))
        tasks = self.reader.get_task_list()
        categories = self.reader.get_all_categories()
        spaces = self.reader.get_all_spaces()

        for rtype in self.run_types:
            print(f"   Processing {rtype}...")
            
            # 1. Task Timings
            for task in tasks:
                data = self.reader.get_task_timings(days=None, task_name=task, run_type=rtype)
                if not data: continue
                fname = f"img/{rtype}/timings/{task}_all.png"
                self._plot_line(
                    data, x_key='date_cycle', y_key='duration',
                    title=f"Runtime: {task} (All Time)", ylabel="Sec",
                    out_path=os.path.join(self.out_dir, fname), color='#1f77b4' # Blue
                )
                meta[rtype]['timings'].append({'title': task, 'file': fname})

            # 2. Obs: Aggregate
            for cat in categories:
                data = self.reader.get_obs_counts_by_category(cat, days=None, run_type=rtype)
                if not data: continue
                fname = f"img/{rtype}/obs/cat_{cat}_all.png"
                self._plot_line(
                    data, x_key='date_cycle', y_key='count',
                    title=f"Category: {cat} (All Time)", ylabel="Count",
                    out_path=os.path.join(self.out_dir, fname), color='#2ca02c' # Green
                )
                meta[rtype]['obs_cat'].append({'title': f"Category: {cat}", 'file': fname})

            # 3. Obs: Spaces
            for space, cat in spaces:
                # All Time (Purple)
                data_all = self.reader.get_obs_counts_by_space(space, days=None, run_type=rtype)
                if not data_all: continue 
                
                fname_all = f"img/{rtype}/obs/space_{space}_all.png"
                self._plot_line(
                    data_all, x_key='date_cycle', y_key='count',
                    title=f"{space} (All Time)", ylabel="Count",
                    out_path=os.path.join(self.out_dir, fname_all), color='#9467bd' # Purple
                )
                
                # Last 7 Days (Orange)
                data_week = self.reader.get_obs_counts_by_space(space, days=7, run_type=rtype)
                fname_week = f"img/{rtype}/obs/space_{space}_week.png"
                if data_week:
                    self._plot_line(
                        data_week, x_key='date_cycle', y_key='count',
                        title=f"{space} (7 Days)", ylabel="Count",
                        out_path=os.path.join(self.out_dir, fname_week), color='#ff7f0e' # Orange
                    )
                
                meta[rtype]['obs_spaces'].append({
                    'category': cat, 'space': space,
                    'file_all': fname_all,
                    'file_week': fname_week if data_week else None
                })
        return meta

    def _plot_line(self, data, x_key, y_key, title, ylabel, out_path, color):
        """
        Generates a plot with Mean line and 1-StdDev band.
        """
        dates = [f"{d['date']}.{d['cycle']:02d}" for d in data]
        values = [d[y_key] for d in data]
        
        # Calculate Statistics
        if len(values) > 1:
            mean_val = statistics.mean(values)
            try:
                stdev = statistics.stdev(values)
            except Exception:
                stdev = 0.0
        elif values:
            mean_val = values[0]
            stdev = 0.0
        else:
            return

        plt.figure(figsize=(8, 4))
        
        # 1. Std Dev Band (Shaded Region)
        if stdev > 0:
            plt.axhspan(mean_val - stdev, mean_val + stdev, color=color, alpha=0.15, label='±1 $\sigma$')

        # 2. Mean Line
        plt.axhline(mean_val, color=color, linestyle='--', alpha=0.6, linewidth=1, label=f'Avg: {mean_val:.1f}')

        # 3. Main Data Line
        plt.plot(dates, values, marker='.', linestyle='-', linewidth=1.5, color=color, label='Data')
        
        # Formatting
        plt.title(title, fontsize=10, fontweight='bold')
        plt.ylabel(ylabel, fontsize=9)
        plt.grid(True, alpha=0.3)
        plt.legend(loc='best', fontsize='small')
        
        # Smart X-Ticks
        n = len(dates)
        if n > 10:
            step = max(1, n // 8)
            plt.xticks(range(0, n, step), [dates[i] for i in range(0, n, step)], rotation=30, ha='right', fontsize=8)
        else:
            plt.xticks(rotation=30, ha='right', fontsize=8)
            
        plt.tight_layout()
        plt.savefig(out_path, dpi=80)
        plt.close()

    # ------------------------------------------------------------------
    # HTML WRITERS (Same as before)
    # ------------------------------------------------------------------
    def _write_index(self):
        nav = self._get_nav_html("home")
        inv_html = ""
        for rtype in self.run_types:
            report = InventoryReport(self.reader, run_type_filter=rtype, limit=100)
            table = report.render_html()
            if "No data found" not in table:
                inv_html += f"<h3>{rtype.upper()} Inventory</h3>{table}"

        html = f"""<!DOCTYPE html><html><head><title>ObsForge Dashboard</title>
        <link rel="stylesheet" href="css/style.css"></head>
        <body><header><h1>ObsForge Monitor</h1>{nav}</header>
        <div class="container"><div class="section">
        <h2>Full Inventory Matrix</h2><p>Displaying last 100 cycles.</p>{inv_html}
        </div></div></body></html>"""
        with open(os.path.join(self.out_dir, "index.html"), "w") as f: f.write(html)

    def _write_run_type_page(self, rtype, meta):
        nav = self._get_nav_html(rtype)
        
        timings_html = "<div class='plot-grid'>" + "".join([f"<div class='plot-card'><div class='plot-title'>{x['title']}</div><img src='{x['file']}'></div>" for x in meta['timings']]) + "</div>" if meta['timings'] else "<p>No data.</p>"
        
        agg_html = "<div class='plot-grid'>" + "".join([f"<div class='plot-card'><div class='plot-title'>{x['title']}</div><img src='{x['file']}'></div>" for x in meta['obs_cat']]) + "</div>" if meta['obs_cat'] else "<p>No data.</p>"
        
        sorted_spaces = sorted(meta['obs_spaces'], key=lambda x: x['category'])
        spaces_html = ""
        current_cat = None
        for item in sorted_spaces:
            if item['category'] != current_cat:
                if current_cat: spaces_html += "</div>"
                current_cat = item['category']
                spaces_html += f"<h3>Category: {current_cat}</h3><div class='plot-grid'>"
            
            spaces_html += f"<div class='plot-card'><div class='plot-title'>{item['space']} (All Time)</div><img src='{item['file_all']}'></div>"
            if item['file_week']:
                spaces_html += f"<div class='plot-card'><div class='plot-title'>{item['space']} (7 Days)</div><img src='{item['file_week']}'></div>"
        if current_cat: spaces_html += "</div>"

        html = f"""<!DOCTYPE html><html><head><title>ObsForge: {rtype.upper()}</title>
        <link rel="stylesheet" href="css/style.css"></head>
        <body><header><h1>{rtype.upper()} Details</h1>{nav}</header>
        <div class="container">
            <div class="section"><h2>Task Timings</h2>{timings_html}</div>
            <div class="section"><h2>Observation Aggregates</h2>{agg_html}</div>
            <div class="section"><h2>Observation Spaces (Detailed)</h2>{spaces_html}</div>
        </div></body></html>"""
        with open(os.path.join(self.out_dir, f"{rtype}.html"), "w") as f: f.write(html)

    def _get_nav_html(self, active_page):
        links = [('<a href="index.html">Dashboard</a>', 'home')]
        for r in self.run_types: links.append((f'<a href="{r}.html">{r.upper()}</a>', r))
        html = "<nav>"
        for link_str, name in links:
            if name == active_page: html += link_str.replace('href=', 'class="active" href=')
            else: html += link_str
        html += "</nav>"
        return html
