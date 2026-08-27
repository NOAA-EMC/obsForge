import os
import shutil
import re
from pathlib import Path
from datetime import datetime

class WebsiteGenerator:
    def __init__(self, website_dir: str, server: 'DataProductsServer', n_cycles: int = 4):
        self.root = Path(website_dir)
        self.server = server
        self.n_cycles = n_cycles
        
        # Internal paths
        self.html_dir = self.root / "html"
        self.data_dir = self.root / "data"
        
        # Regex for local data folder parsing
        self.dir_pattern = re.compile(r"(.+)_(\d{8})_(\d{2})")

        # Automatically discover datasets from the server
        self.dataset_names = self.server.list_datasets()

        # Ensure directory structure exists
        self.html_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def fetch_data(self):
        """Synchronize the latest N cycles from server to local data dir."""
        for name in self.dataset_names:
            available = self.server.list_available_cycles(name)
            to_fetch = available[-self.n_cycles:] if available else []
            
            for c_date, c_hour in to_fetch:
                folder_name = f"{name}_{c_date:%Y%m%d}_{c_hour}"
                if not (self.data_dir / folder_name).exists():
                    print(f"Syncing: {folder_name}")
                    self.server.fetch_cycle(name, c_date, c_hour, str(self.data_dir))

    def generate(self):
        """Build the website structure based on locally synced data."""
        # 1. Map local data and prune anything outside the current window
        inventory = self._get_local_inventory()
        self._prune_local_storage(inventory)
        
        # 2. Refresh inventory after pruning and build pages
        inventory = self._get_local_inventory()
        for ds_name, cycles in inventory.items():
            obs_spaces = self._discover_obs_spaces(cycles)
            self._build_dataset_pages(ds_name, cycles, obs_spaces)
            self._build_interactive_pages(ds_name, cycles, obs_spaces)
            self._build_single_viewer_pages(ds_name, cycles)
            
        self._build_index(inventory)

    def _get_local_inventory(self):
        """Groups folders in data/ by dataset name and sorts them."""
        inventory = {name: [] for name in self.dataset_names}
        for folder in self.data_dir.iterdir():
            if not folder.is_dir(): continue
            match = self.dir_pattern.match(folder.name)
            if match:
                ds_name, d_str, h_str = match.groups()
                if ds_name in inventory:
                    inventory[ds_name].append({
                        "date_str": d_str, "hour": h_str,
                        "path": folder, "sort_key": f"{d_str}{h_str}"
                    })
        
        for ds in inventory:
            inventory[ds].sort(key=lambda x: x["sort_key"])
        return inventory

    def _prune_local_storage(self, inventory):
        """Removes folders from web-root if they exceed N cycles."""
        for cycles in inventory.values():
            if len(cycles) > self.n_cycles:
                for old_cycle in cycles[:-self.n_cycles]:
                    shutil.rmtree(old_cycle['path'])

    def _discover_obs_spaces(self, cycles):
        """Identifies obs_space subfolders (e.g., 'radiance', 'conv')."""
        obs = set()
        for c in cycles:
            obs.update(p.name for p in c['path'].iterdir() if p.is_dir())
        return sorted(list(obs))

    def _build_dataset_pages(self, ds_name, cycles, obs_spaces):
        """Generates static HTML for every cycle/obs combination."""
        for cycle in cycles:
            c_label = f"{cycle['date_str']}_{cycle['hour']}"
            for obs in obs_spaces:
                local_path = cycle['path'] / obs
                imgs = [f.name for f in local_path.iterdir() if f.is_file()] if local_path.exists() else []
                
                filename = f"{ds_name}_{c_label}_{obs}.html"
                # Relative path from 'html/' to 'data/...'
                rel_img_path = f"../data/{cycle['path'].name}/{obs}"
                
                page_html = self._layout(ds_name, cycle, obs, cycles, obs_spaces, 
                                        self._render_grid(rel_img_path, imgs))
                
                (self.html_dir / filename).write_text(page_html)

    def _render_grid(self, rel_path, imgs):
        if not imgs: return "<div class='empty'>No plots found.</div>"
        return f'<div class="grid">' + "".join([
            f'<div class="card"><h5>{img}</h5><a href="{rel_path}/{img}"><img src="{rel_path}/{img}"></a></div>'
            for img in sorted(imgs)
        ]) + '</div>'


    def _layout(self, current_ds, current_cycle, current_obs, all_cycles, all_obs, content):
        # 1. Standard Dataset Nav
        ds_nav = "".join([f'<a href="{n}_latest.html" class="{"active" if n == current_ds else ""}">{n}</a>' 
                         for n in self.dataset_names])
        
        # 2. Cycle Nav (YYYYMMDD HH)
        cy_nav = "".join([
            f'<a href="{current_ds}_{c["date_str"]}_{c["hour"]}_{current_obs}.html" class="btn {"act" if c == current_cycle else ""}">'
            f'{c["date_str"]} {c["hour"]}z</a>' for c in all_cycles
        ])

        # 3. Data Product Nav
        ob_nav = "".join([
            f'<a href="{current_ds}_{current_cycle["date_str"]}_{current_cycle["hour"]}_{o}.html" class="ob {"act" if o == current_obs else ""}">{o}</a>'
            for o in all_obs
        ])

        # 4. Define the Interactive Toggle Button
        # This link points to the "view_" version of the current page
        c_date = current_cycle["date_str"]
        c_hour = current_cycle["hour"]
        interactive_btn = f"""
        <div style="margin-top:15px; padding:10px; border-top:1px solid #ddd;">
            <a href="view_{current_ds}_{c_date}_{c_hour}_{current_obs}.html" 
               style="display:block; padding:8px; background:#444; color:yellow; text-decoration:none; text-align:center; border-radius:4px; font-size:11px; font-weight:bold;">
               OPEN INTERACTIVE VIEWER
            </a>
        </div>
        """

        # Define the top-bar link for redundancy
        interactive_link = f'<a href="view_{current_ds}_{c_date}_{c_hour}_{current_obs}.html" style="color:yellow; margin-left:20px;">[INTERACTIVE]</a>'

        return f"""
        <html><head><style>{self._css()}</style></head><body>
            <nav class="top"><strong>Dataset:</strong> {ds_nav} {interactive_link}</nav>
            <div class="side">
                <h4>Cycles</h4>
                <div class="cy-box">{cy_nav}</div>
                <hr>
                <h4>Data Products</h4>
                <div class="product-list">
                    {ob_nav}
                    {interactive_btn}
                </div>
            </div>
            <div class="main">
                {content}
            </div>
        </body></html>
        """

    def _css(self):
        return """
        body { font-family: sans-serif; margin: 0; background: #f4f4f4; }
        
        .top { 
            position: fixed; top: 0; width: 100%; height: 45px; 
            background: #111; color: #fff; display: flex; 
            align-items: center; padding: 0 20px; z-index: 1000; 
        }
        .top a { color: #888; text-decoration: none; margin-left: 15px; font-size: 13px; }
        
        /* Fixed Sidebar with internal scroll */
        .side { 
            width: 240px; position: fixed; top: 45px; left: 0; bottom: 0;
            background: #fff; border-right: 1px solid #ddd; 
            padding: 20px 15px; overflow-y: auto; z-index: 900;
        }

        /* The Main content needs a margin to sit next to the fixed sidebar */
        .main { margin-left: 240px; padding: 65px 25px 25px 25px; }
        
        .cy-box { display: flex; flex-wrap: wrap; gap: 4px; margin-bottom: 10px; }
        .btn { font-size: 10px; padding: 3px 6px; border: 1px solid #ccc; text-decoration: none; color: #333; }
        .btn.act { background: #007bff; color: #fff; }
        
        .ob { display: block; padding: 8px; color: #444; text-decoration: none; font-size: 12px; border-bottom: 1px solid #f9f9f9; }
        .ob.act { background: #eef; color: #007bff; font-weight: bold; }
        
        /* Standard CSS Grid for your cards */
        .grid { 
            display: grid; 
            grid-template-columns: repeat(auto-fill, minmax(380px, 1fr)); 
            gap: 15px; 
            width: 100%;
        }
        .card { background: #fff; border: 1px solid #eee; padding: 8px; box-shadow: 1px 1px 3px rgba(0,0,0,0.05); }
        .card img { width: 100%; height: auto; display: block; }
        h5 { margin: 0 0 5px 0; font-size: 11px; color: #999; }
        """


    def oldold_css(self):
        return """
        body { font-family: 'Segoe UI', sans-serif; margin: 0; display: flex; background: #f8f9fa; color: #333; }
        .top { position: fixed; top: 0; width: 100%; height: 50px; background: #222; color: #fff; display: flex; align-items: center; padding: 0 20px; z-index: 10; }
        .top a { color: #aaa; text-decoration: none; margin-left: 20px; font-size: 14px; transition: color 0.2s; }
        .top a:hover, .top a.active { color: #fff; font-weight: 600; }
        
        .side { width: 240px; background: #fff; border-right: 1px solid #dee2e6; padding: 70px 20px; height: 100vh; position: fixed; overflow-y: auto; }
        .side h4 { font-size: 12px; text-transform: uppercase; color: #6c757d; letter-spacing: 1px; margin-bottom: 15px; }
        
        /* Vertical Cycle List */
        .cy-list { display: flex; flex-direction: column; gap: 8px; margin-bottom: 25px; }
        .cy-item { padding: 10px; border: 1px solid #eee; text-decoration: none; color: #495057; font-family: monospace; font-size: 14px; border-radius: 4px; text-align: center; }
        .cy-item:hover { background: #f1f3f5; }
        .cy-item.act { background: #007bff; color: #fff; border-color: #007bff; font-weight: bold; }
        
        .main { flex: 1; margin-left: 240px; padding: 80px 40px; }
        
        .ob { display: block; padding: 10px; color: #444; text-decoration: none; font-size: 14px; border-radius: 4px; margin-bottom: 4px; }
        .ob:hover { background: #f8f9fa; }
        .ob.act { background: #e7f1ff; color: #007bff; font-weight: bold; }
        
        .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(400px, 1fr)); gap: 20px; }
        .card { background: #fff; border: 1px solid #dee2e6; padding: 12px; border-radius: 6px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
        .card img { width: 100%; height: auto; border-radius: 2px; }
        h5 { margin: 0 0 8px 0; font-size: 12px; color: #6c757d; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        hr { border: 0; border-top: 1px solid #eee; margin: 20px 0; }
        """



    def old_layout(self, current_ds, current_cycle, current_obs, all_cycles, all_obs, content):
        # Build Navigation Strings
        ds_nav = "".join([f'<a href="{n}_latest.html" class="{"active" if n == current_ds else ""}">{n}</a>' 
                         for n in self.dataset_names])
        
        cy_nav = "".join([
            f'<a href="{current_ds}_{c["date_str"]}_{c["hour"]}_{current_obs}.html" class="btn {"act" if c == current_cycle else ""}">'
            f'{c["date_str"][-4:]} {c["hour"]}z</a>' for c in all_cycles
        ])

        ob_nav = "".join([
            f'<a href="{current_ds}_{current_cycle["date_str"]}_{current_cycle["hour"]}_{o}.html" class="ob {"act" if o == current_obs else ""}">{o}</a>'
            for o in all_obs
        ])

        return f"""
        <html><head><style>{self._css()}</style></head><body>
            <nav class="top"><strong>Dataset:</strong> {ds_nav}</nav>
            <div class="side">
                <h4>Cycles</h4><div class="cy-box">{cy_nav}</div>
                <hr><h4>Obs Spaces</h4>{ob_nav}
            </div>
            <div class="main">
                <header><h3>{current_ds} / {current_obs} <small>{current_cycle['date_str']} {current_cycle['hour']}z</small></h3></header>
                {content}
            </div>
        </body></html>
        """

    def old_css(self):
        return """
        body { font-family: sans-serif; margin: 0; display: flex; background: #f4f4f4; }
        .top { position: fixed; top: 0; width: 100%; height: 45px; background: #111; color: #fff; display: flex; align-items: center; padding: 0 20px; z-index: 5; }
        .top a { color: #888; text-decoration: none; margin-left: 15px; font-size: 13px; }
        .top a.active { color: #fff; font-weight: bold; }
        .side { width: 220px; background: #fff; border-right: 1px solid #ddd; padding: 60px 15px; height: 100vh; overflow-y: auto; }
        .main { flex: 1; padding: 60px 25px; }
        .cy-box { display: flex; flex-wrap: wrap; gap: 4px; }
        .btn { font-size: 10px; padding: 3px 6px; border: 1px solid #ccc; text-decoration: none; color: #333; }
        .btn.act { background: #007bff; color: #fff; border-color: #0056b3; }
        .ob { display: block; padding: 7px; color: #444; text-decoration: none; font-size: 13px; margin-bottom: 2px; }
        .ob.act { background: #eef; color: #007bff; font-weight: bold; border-radius: 4px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(380px, 1fr)); gap: 15px; }
        .card { background: #fff; border: 1px solid #eee; padding: 8px; }
        .card img { width: 100%; height: auto; }
        h5 { margin: 0 0 5px 0; font-size: 11px; color: #999; }
        """

    def _build_index(self, inventory):
        """Builds redirecting entry points to the Single Viewer."""
        for ds_name, cycles in inventory.items():
            if not cycles: continue
            
            # 1. Get the absolute latest cycle
            latest = cycles[-1]
            c_path = Path(latest['path'])
            c_label = f"{latest['date_str']}_{latest['hour']}"
            
            # 2. Re-run the inversion logic once to find the first valid landing spot
            obs_to_products = {}
            for prod_dir in [d for d in c_path.iterdir() if d.is_dir()]:
                for f in prod_dir.iterdir():
                    if f.is_file():
                        obs_name = f.stem
                        if obs_name not in obs_to_products:
                            obs_to_products[obs_name] = []
                        obs_to_products[obs_name].append(prod_dir.name)
            
            if not obs_to_products:
                continue

            # 3. Identify the first Obs Space and its first Product
            first_obs = sorted(obs_to_products.keys())[0]
            first_prod = sorted(obs_to_products[first_obs])[0]
            
            # Target filename matches the 'single_' format
            target_filename = f"single_{ds_name}_{c_label}_{first_obs}_{first_prod}.html"
            
            # Global Site Index (Redirects to the first dataset's latest page)
            if ds_name == self.dataset_names[0]:
                index_path = self.root / "index.html"
                index_path.write_text(f'<html><meta http-equiv="refresh" content="0;url=html/{target_filename}"></html>')
            
            # Dataset-specific "Latest" link (e.g., gdas_latest.html)
            latest_path = self.html_dir / f"{ds_name}_latest.html"
            latest_path.write_text(f'<html><meta http-equiv="refresh" content="0;url={target_filename}"></html>')

    def old_build_index(self, inventory):
        """Builds redirecting entry points for the root and each dataset."""
        for ds_name, cycles in inventory.items():
            if not cycles: continue
            latest = cycles[-1]
            obs = self._discover_obs_spaces([latest])
            first_ob = obs[0] if obs else "none"
            target = f"html/{ds_name}_{latest['date_str']}_{latest['hour']}_{first_ob}.html"
            
            # Global index
            if ds_name == self.dataset_names[0]:
                (self.root / "index.html").write_text(f'<html><meta http-equiv="refresh" content="0;url={target}"></html>')
            
            # Dataset latest link
            (self.html_dir / f"{ds_name}_latest.html").write_text(f'<html><meta http-equiv="refresh" content="0;url={ds_name}_{latest["date_str"]}_{latest["hour"]}_{first_ob}.html"></html>')


    def _build_interactive_pages(self, ds_name, cycles, obs_spaces):
        """Duplicated logic to create HTML pages that embed the Plotly viewers."""
        for cycle in cycles:
            c_label = f"{cycle['date_str']}_{cycle['hour']}"
            for obs in obs_spaces:
                # Same path as before
                local_path = cycle['path'] / obs
                # Find the .html files produced by PlotGenerator
                html_files = [f.name for f in local_path.iterdir() if f.name.endswith('.html')] if local_path.exists() else []
                
                # New filename for the page itself (prefixed with 'view_')
                filename = f"view_{ds_name}_{c_label}_{obs}.html"
                rel_data_path = f"../data/{cycle['path'].name}/{obs}"
                
                # Brute force HTML grid using iframes
                grid_html = '<div class="grid">'
                for h in sorted(html_files):
                    grid_html += f"""
                    <div class="card" style="height: 500px; width: 500px; display: inline-block; margin: 10px; background: #fff;">
                        <h5>{h}</h5>
                        <iframe src="{rel_data_path}/{h}" style="width:100%; height:90%; border:none;"></iframe>
                    </div>"""
                grid_html += '</div>'
                
                # Minimal layout wrapper (duplicated CSS)
                page_html = f"<html><head><style>{self._css()}</style></head><body>{grid_html}</body></html>"
                
                (self.html_dir / filename).write_text(page_html)

    def _build_single_viewer_pages(self, ds_name, cycles):
        # 1. First, build a map of "First Valid Page" for every cycle
        # This prevents broken links when switching cycles
        cycle_landing_pages = {}
        
        all_cycle_data = {}
        for cycle in cycles:
            c_path = Path(cycle['path'])
            c_label = f"{cycle['date_str']}_{cycle['hour']}"
            
            # Invert storage for this cycle
            obs_map = {}
            for prod_dir in [d for d in c_path.iterdir() if d.is_dir()]:
                for f in prod_dir.iterdir():
                    if f.is_file():
                        obs_name = f.stem
                        if obs_name not in obs_map: obs_map[obs_name] = []
                        obs_map[obs_name].append(prod_dir.name)
            
            all_cycle_data[c_label] = obs_map
            
            # Store the first valid page for this cycle as a fallback
            if obs_map:
                first_o = sorted(obs_map.keys())[0]
                first_p = sorted(obs_map[first_o])[0]
                cycle_landing_pages[c_label] = f"single_{ds_name}_{c_label}_{first_o}_{first_p}.html"

        # 2. Generate the pages
        for c_label, obs_map in all_cycle_data.items():
            current_cycle_obj = next(c for c in cycles if f"{c['date_str']}_{c['hour']}" == c_label)
            all_obs_sorted = sorted(obs_map.keys())
            
            for obs in all_obs_sorted:
                available_prods = sorted(obs_map[obs])
                for prod in available_prods:
                    prod_path = Path(current_cycle_obj['path']) / prod
                    actual_file = next(f.name for f in prod_path.iterdir() if f.stem == obs)
                    
                    page_name = f"single_{ds_name}_{c_label}_{obs}_{prod}.html"
                    rel_data_path = f"../data/{current_cycle_obj['path'].name}/{prod}"
                    
                    page_html = self._single_viewer_layout(
                        ds_name, current_cycle_obj, obs, prod,
                        cycles, all_obs_sorted, available_prods,
                        actual_file, rel_data_path, cycle_landing_pages
                    )
                    (self.html_dir / page_name).write_text(page_html)


    def _single_viewer_layout(self, ds, cur_cy, cur_obs, cur_prod, all_cycles, all_obs, all_prods, filename, rel_path, landing_pages):
        ds_nav = "".join([f'<a href="{n}_latest.html">{n}</a>' for n in self.dataset_names])
        
        # FIXED CYCLE LINKS: Use pre-calculated valid landing pages
        cy_links = []
        for c in all_cycles[::-1]:
            label = f"{c['date_str']}_{c['hour']}"
            fmt_date = f"{c['date_str'][:4]} {c['date_str'][4:6]} {c['date_str'][6:8]}: {c['hour']}"
            act = "act" if c == cur_cy else ""
            
            # If the landing page exists for this cycle, use it.
            target = landing_pages.get(label, "#")
            cy_links.append(f'<a href="{target}" class="cy-item {act}">{fmt_date}</a>')
        cy_nav = "".join(cy_links)

        ob_nav = "".join([f'<a href="single_{ds}_{cur_cy["date_str"]}_{cur_cy["hour"]}_{o}_{cur_prod if cur_prod in all_prods else all_prods[0]}.html" class="ob {"act" if o == cur_obs else ""}">{o}</a>' for o in all_obs])
        pr_nav = "".join([f'<a href="single_{ds}_{cur_cy["date_str"]}_{cur_cy["hour"]}_{cur_obs}_{p}.html" class="ob {"act" if p == cur_prod else ""}">{p}</a>' for p in all_prods])

        file_url = f"{rel_path}/{filename}"
        display = f'<iframe src="{file_url}" style="width:100%; height:100%; border:none;"></iframe>' if filename.endswith(".html") else f'<img src="{file_url}" style="max-width:100%;">'

        return f"""
        <html><head><style>
            {self._css()}
            body {{ height: 100vh; display: flex; flex-direction: column; overflow: hidden; }}
            .top {{ flex: 0 0 45px; position: relative; }}
            .container {{ display: flex; flex: 1; overflow: hidden; }}
            
            .side {{ 
                width: 260px; display: flex; flex-direction: column; 
                background: #fff; border-right: 1px solid #ddd; height: 100%;
            }}
            .cy-list {{ flex: 0 0 160px; overflow-y: auto; border: 1px solid #eee; margin: 5px; }}
            .ob-list {{ flex: 1 1 200px; overflow-y: auto; border: 1px solid #eee; margin: 5px; }}
            .pr-list {{ flex: 1 1 150px; overflow-y: auto; border: 1px solid #eee; margin: 5px; }}
            
            .cy-item {{ display: block; padding: 8px; font-family: monospace; font-size: 13px; text-decoration: none; color: #333; background: #f9f9f9; text-align: center; border-bottom: 1px solid #eee; }}
            .cy-item.act {{ background: #007bff; color: #fff; font-weight: bold; }}
            .main {{ flex: 1; overflow: auto; padding: 15px; background: #eee; display: flex; flex-direction: column; }}
            .viewer {{ flex: 1; background: #fff; border: 1px solid #ccc; display: flex; align-items: center; justify-content: center; overflow: hidden; }}
        </style></head><body>
            <nav class="top"><strong>Dataset:</strong> {ds_nav}</nav>
            <div class="container">
                <div class="side">
                    <h4>1. Cycles</h4><div class="cy-list">{cy_nav}</div>
                    <h4>2. Obs Spaces</h4><div class="ob-list">{ob_nav}</div>
                    <h4>3. Products</h4><div class="pr-list">{pr_nav}</div>
                </div>
                <div class="main">
                    <div style="padding-bottom:10px; font-size:12px; font-weight:bold;">{cur_obs} / {cur_prod}</div>
                    <div class="viewer">{display}</div>
                </div>
            </div>
        </body></html>
        """


    def old_build_single_viewer_pages(self, ds_name, cycles):
        """Builds pages with Logical Hierarchy: Cycle -> Obs Space -> Product."""
        for cycle in cycles:
            c_path = Path(cycle['path'])
            c_label = f"{cycle['date_str']}_{cycle['hour']}"
            
            # 1. Invert the storage: Map Obs Spaces to their available Products
            # Structure: { 'radiance': ['plt_map', 'int_globe'], 'ocean': ['plt_map'] }
            obs_to_products = {}
            product_dirs = sorted([d.name for d in c_path.iterdir() if d.is_dir()])
            
            for prod in product_dirs:
                prod_path = c_path / prod
                # Files are 'Obs Spaces'
                for f in prod_path.iterdir():
                    if f.is_file():
                        obs_name = f.stem  # e.g., 'radiance'
                        if obs_name not in obs_to_products:
                            obs_to_products[obs_name] = []
                        obs_to_products[obs_name].append(prod)

            all_obs_sorted = sorted(obs_to_products.keys())

            # 2. Iterate through the Logical Hierarchy to generate pages
            for obs in all_obs_sorted:
                available_prods = sorted(obs_to_products[obs])
                
                for prod in available_prods:
                    # Find the actual filename (we need the extension for the viewer)
                    # Search in cycle/prod/obs.*
                    prod_path = c_path / prod
                    actual_file = next(f.name for f in prod_path.iterdir() if f.stem == obs)

                    page_name = f"single_{ds_name}_{c_label}_{obs}_{prod}.html"
                    rel_data_path = f"../data/{c_path.name}/{prod}"
                    
                    page_html = self._single_viewer_layout(
                        ds_name, cycle, obs, prod,
                        cycles, all_obs_sorted, available_prods,
                        actual_file, rel_data_path
                    )
                    
                    (self.html_dir / page_name).write_text(page_html)

    def old_single_viewer_layout(self, ds, cur_cy, cur_obs, cur_prod, all_cycles, all_obs, all_prods, filename, rel_path):
        # 1. Dataset Top Nav
        ds_nav = "".join([f'<a href="{n}_latest.html">{n}</a>' for n in self.dataset_names])
        
        # 2. Vertical Cycle Selector (YYYY MM DD: HH) - 4 entries max
        cy_links = []
        for c in all_cycles[::-1]:
            d = c["date_str"]
            fmt_date = f"{d[:4]} {d[4:6]} {d[6:8]}: {c['hour']}"
            act = "act" if c == cur_cy else ""
            # Link must maintain current Obs and Prod if possible
            cy_links.append(f'<a href="single_{ds}_{d}_{c["hour"]}_{cur_obs}_{cur_prod}.html" class="cy-item {act}">{fmt_date}</a>')
        cy_nav = "".join(cy_links)

        # 3. Logical Selectors: Obs Space FIRST, then Products
        # Note: ob_nav remains stable, prod_nav updates based on available products for that Obs Space
        ob_nav = "".join([f'<a href="single_{ds}_{cur_cy["date_str"]}_{cur_cy["hour"]}_{o}_{cur_prod}.html" class="ob {"act" if o == cur_obs else ""}">{o}</a>' for o in all_obs])
        pr_nav = "".join([f'<a href="single_{ds}_{cur_cy["date_str"]}_{cur_cy["hour"]}_{cur_obs}_{p}.html" class="ob {"act" if p == cur_prod else ""}">{p}</a>' for p in all_prods])

        # 4. Content Viewer
        file_url = f"{rel_path}/{filename}"
        if filename.endswith(".html"):
            display = f'<iframe src="{file_url}" style="width:100%; height:80vh; border:none; background:#fff;"></iframe>'
        else:
            display = f'<div class="img-viewer"><img src="{file_url}"></div>'

        return f"""
        <html><head><style>
            {self._css()}
            .cy-list {{ display: flex; flex-direction: column; gap: 5px; max-height: 170px; overflow-y: auto; padding: 5px; background: #eee; border-radius: 4px; }}
            .cy-item {{ display: block; padding: 8px; font-family: monospace; font-size: 13px; text-decoration: none; color: #333; background: #fff; text-align: center; border: 1px solid #ddd; }}
            .cy-item.act {{ background: #007bff; color: #fff; font-weight: bold; }}
            .scroll-box {{ overflow-y: auto; flex-grow: 1; margin-top: 5px; border-top: 1px solid #eee; }}
            .img-viewer {{ background: #fff; padding: 10px; text-align: center; border: 1px solid #ddd; }}
            .img-viewer img {{ max-width: 100%; height: auto; }}
        </style></head><body>
            <nav class="top"><strong>Dataset:</strong> {ds_nav}</nav>
            <div class="side" style="display:flex; flex-direction:column; height: 100vh;">
                <h4>1. Cycles</h4>
                <div class="cy-list">{cy_nav}</div>
                <hr>
                <h4>2. Obs Spaces</h4>
                <div class="scroll-box" style="max-height: 200px;">{ob_nav}</div>
                <hr>
                <h4>3. Products (for {cur_obs})</h4>
                <div class="scroll-box">{pr_nav}</div>
            </div>
            <div class="main">
                <div style="font-size: 11px; margin-bottom: 8px; color: #888; text-transform: uppercase;">
                    {cur_obs} &raquo; {cur_prod}
                </div>
                {display}
            </div>
        </body></html>
        """
