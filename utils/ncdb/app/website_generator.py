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

    def _css(self):
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
