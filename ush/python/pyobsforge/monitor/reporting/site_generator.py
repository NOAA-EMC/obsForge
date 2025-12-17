import os
import shutil
from datetime import datetime
from collections import defaultdict

from pyobsforge.monitor.database.db_reader import DBReader
from pyobsforge.monitor.reporting.inventory_report import InventoryReport

# Handle optional plotting dependency
try:
    import matplotlib
    matplotlib.use('Agg') # Force non-interactive backend
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

class SiteGenerator:
    def __init__(self, db_path, output_dir):
        self.reader = DBReader(db_path)
        self.out_dir = output_dir
        self.img_dir = os.path.join(output_dir, "img")
        self.css_dir = os.path.join(output_dir, "css")
        
        self.run_types = sorted(list(self.reader.get_cycle_ranges().keys()))
        if not self.run_types: self.run_types = ['gdas', 'gfs']

    def build(self):
        print(f"Building site in: {self.out_dir}")
        self._setup_structure()
        
        # 1. Generate Plots (Optional)
        if HAS_MATPLOTLIB:
            print("... Generating plots")
            plots_meta = self._generate_all_plots()
        else:
            print("... Matplotlib missing: Skipping plots.")
            plots_meta = {'timings': defaultdict(list), 'obs': defaultdict(list)}
        
        # 2. Write HTML Pages
        print("... Writing Index")
        self._write_index()
        
        print("... Writing Timings")
        self._write_timings(plots_meta['timings'])
        
        print("... Writing Observations")
        self._write_observations(plots_meta['obs'])
        
        print("Done.")

    def _setup_structure(self):
        os.makedirs(self.img_dir, exist_ok=True)
        os.makedirs(self.css_dir, exist_ok=True)
        
        css_content = """
        body { font-family: 'Segoe UI', sans-serif; margin: 0; background: #f4f7f6; color: #333; }
        header { background: #2c3e50; color: white; padding: 15px 40px; display: flex; justify-content: space-between; align-items: center; }
        header h1 { margin: 0; font-size: 1.4em; }
        nav a { color: #ecf0f1; text-decoration: none; margin-left: 20px; font-weight: 600; opacity: 0.8; }
        nav a:hover, nav a.active { opacity: 1.0; border-bottom: 2px solid #3498db; }
        .container { padding: 40px; max_width: 1400px; margin: 0 auto; }
        .card { background: white; padding: 25px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.05); margin-bottom: 30px; }
        h2 { color: #2c3e50; border-bottom: 1px solid #eee; padding-bottom: 10px; margin-top: 0; }
        h3 { color: #34495e; margin-top: 30px; }
        
        table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
        th, td { padding: 10px 15px; border-bottom: 1px solid #eee; text-align: center; }
        th { background: #f8f9fa; color: #555; font-weight: 600; }
        .status-ok { background: #d4edda; color: #155724; font-weight: bold; border-radius: 4px; padding: 4px; }
        .status-log { background: #fff3cd; color: #856404; border-radius: 4px; padding: 4px; }
        .status-dat { background: #d1ecf1; color: #0c5460; border-radius: 4px; padding: 4px; }
        .status-mis { background: #f8d7da; color: #721c24; border-radius: 4px; padding: 4px; }
        
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(600px, 1fr)); gap: 25px; }
        .chart-box { border: 1px solid #eee; padding: 10px; border-radius: 4px; background: #fff; }
        .chart-box img { width: 100%; height: auto; }
        .alert { background: #fff3cd; color: #856404; padding: 15px; border-radius: 4px; border: 1px solid #ffeeba; }
        """
        with open(os.path.join(self.css_dir, "style.css"), "w") as f:
            f.write(css_content)

    def _generate_all_plots(self):
        meta = {'timings': defaultdict(list), 'obs': defaultdict(list)}
        tasks = self.reader.get_task_list()
        obs_totals = self.reader.get_obs_totals(days=7) 
        
        for rtype in self.run_types:
            # 1. Timings
            for task in tasks:
                data = self.reader.get_task_timings(days=14, task_name=task, run_type=rtype)
                if not data: continue
                fname = f"time_{rtype}_{task}.png"
                self._plot_metric(
                    data, x_key='date_cycle', y_key='duration',
                    title=f"{task} ({rtype})", ylabel="Seconds",
                    filename=fname, color='#e74c3c' if rtype=='gfs' else '#3498db'
                )
                meta['timings'][rtype].append({'title': task, 'file': f"img/{fname}"})

            # 2. Obs
            for name, _ in obs_totals[:8]: 
                data = self.reader.get_obs_counts_by_space(name, days=14, run_type=rtype)
                if not data:
                    data = self.reader.get_obs_counts_by_category(name, days=14, run_type=rtype)
                if not data: continue

                fname = f"obs_{rtype}_{name}.png"
                self._plot_metric(
                    data, x_key='date_cycle', y_key='count',
                    title=f"{name} ({rtype})", ylabel="Count",
                    filename=fname, color='#27ae60'
                )
                meta['obs'][rtype].append({'title': name, 'file': f"img/{fname}"})
        return meta

    def _plot_metric(self, data, x_key, y_key, title, ylabel, filename, color):
        dates = [f"{d['date']}.{d['cycle']:02d}" for d in data]
        values = [d[y_key] for d in data]
        if not dates: return

        plt.figure(figsize=(10, 5))
        plt.plot(dates, values, marker='o', markersize=4, linestyle='-', linewidth=2, color=color)
        plt.title(title, fontsize=12, fontweight='bold')
        plt.ylabel(ylabel)
        plt.grid(True, linestyle='--', alpha=0.5)
        
        if len(dates) > 10:
            plt.xticks(range(0, len(dates), max(1, len(dates)//10)), 
                       [dates[i] for i in range(0, len(dates), max(1, len(dates)//10))],
                       rotation=45, ha='right')
        else:
            plt.xticks(rotation=45, ha='right')
            
        plt.tight_layout()
        plt.savefig(os.path.join(self.img_dir, filename), dpi=100)
        plt.close()

    # --- HTML WRITERS ---

    def _write_index(self):
        tables_html = ""
        for rtype in self.run_types:
            report = InventoryReport(self.reader, run_type_filter=rtype, limit=20)
            table = report.render_html()
            if "No data found" not in table:
                tables_html += f"<h3>Cycle Inventory: {rtype.upper()}</h3>"
                tables_html += table

        html = self._get_header("Dashboard", "index")
        html += f"""
        <div class="container">
            <div class="card">
                <h2>System Overview</h2>
                <p>Run Types Detected: <strong>{", ".join(self.run_types)}</strong></p>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}</p>
            </div>
            <div class="card">
                <h2>Recent Cycles</h2>
                {tables_html}
            </div>
        </div>
        </body></html>
        """
        with open(os.path.join(self.out_dir, "index.html"), "w") as f: f.write(html)

    def _write_timings(self, plots_dict):
        content = ""
        if not HAS_MATPLOTLIB:
            content = "<div class='alert'><strong>Plotting Unavailable:</strong> Matplotlib is not installed on this system.</div>"
        else:
            for rtype in self.run_types:
                if not plots_dict[rtype]: continue
                content += f"<h3>{rtype.upper()} Runtimes</h3><div class='grid'>"
                for p in plots_dict[rtype]:
                    content += f"<div class='chart-box'><div class='chart-title'>{p['title']}</div><img src='{p['file']}'></div>"
                content += "</div>"

        html = self._get_header("Timings", "timings")
        html += f"<div class='container'><div class='card'><h2>Task Performance</h2>{content}</div></div></body></html>"
        with open(os.path.join(self.out_dir, "timings.html"), "w") as f: f.write(html)

    def _write_observations(self, plots_dict):
        content = ""
        if not HAS_MATPLOTLIB:
            content = "<div class='alert'><strong>Plotting Unavailable:</strong> Matplotlib is not installed on this system.</div>"
        else:
            for rtype in self.run_types:
                if not plots_dict[rtype]: continue
                content += f"<h3>{rtype.upper()} Counts</h3><div class='grid'>"
                for p in plots_dict[rtype]:
                    content += f"<div class='chart-box'><div class='chart-title'>{p['title']}</div><img src='{p['file']}'></div>"
                content += "</div>"

        html = self._get_header("Observations", "obs")
        html += f"<div class='container'><div class='card'><h2>Data Counts</h2>{content}</div></div></body></html>"
        with open(os.path.join(self.out_dir, "observations.html"), "w") as f: f.write(html)

    def _get_header(self, title, active_page):
        nav_items = [
            ("Dashboard", "index", "index.html"),
            ("Timings", "timings", "timings.html"),
            ("Observations", "obs", "observations.html")
        ]
        
        nav_html = ""
        for label, name, link in nav_items:
            cls = "active" if name == active_page else ""
            nav_html += f'<a href="{link}" class="{cls}">{label}</a>'

        return f"""<!DOCTYPE html>
        <html>
        <head>
            <title>ObsForge: {title}</title>
            <link rel="stylesheet" href="css/style.css">
        </head>
        <body>
            <header>
                <h1>ObsForge Monitor</h1>
                <nav>{nav_html}</nav>
            </header>
        """
