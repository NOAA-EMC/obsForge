from collections import defaultdict
from pyobsforge.monitor.database.db_reader import DBReader

class InventoryReport:
    """
    Presentation Layer.
    Fetches raw data from DBReader, applies status logic (OK/Fail),
    and renders the output as Text (CLI) or HTML (Web).
    """
    def __init__(self, reader: DBReader, run_type_filter=None, limit=50):
        self.reader = reader
        self.run_type_filter = run_type_filter
        self.limit = limit
        
        self.task_names = []
        self.matrix = defaultdict(dict) # Stores status codes
        self.keys = []                  # Stores row headers (date, cycle, type)
        
        self._build_report()

    def _build_report(self):
        """Fetch data and calculate status codes."""
        self.task_names = self.reader.get_task_list()
        if not self.task_names:
            return

        # Fetch dicts from DBReader
        rows = self.reader.get_inventory_matrix(self.run_type_filter, self.limit)
        
        seen_keys = set()
        for r in rows:
            key = (r['date'], r['cycle'], r['run_type'])
            
            if key not in seen_keys:
                self.keys.append(key)
                seen_keys.add(key)
            
            # Copy the status map to our local matrix
            for task_name, status_obj in r['tasks'].items():
                self.matrix[key][task_name] = status_obj['status']

    def render_cli(self) -> str:
        """Returns ANSI-colored text string for terminal output."""
        if not self.task_names: return "No data found."

        output = []
        # Dynamic column width
        max_len = max(len(t) for t in self.task_names)
        col_width = max(8, max_len + 2)

        # Header
        header_cycle = f"{'DATE':<10} | {'CYC':<3} | {'TYPE':<6}"
        header_tasks = " | ".join([f"{t[:col_width-1]:<{col_width}}" for t in self.task_names])
        sep_line = "-" * (len(header_cycle) + 3 + len(header_tasks))
        
        output.append("\nInventory Report")
        if self.run_type_filter: output.append(f"Filter: Run-Type = {self.run_type_filter}")
        output.append(sep_line)
        output.append(f"{header_cycle} | {header_tasks}")
        output.append(sep_line)

        for k in self.keys:
            date, cycle, run_type = k
            row_str = f"{date:<10} | {cycle:02d}  | {run_type:<6}"
            
            cells = []
            for t in self.task_names:
                val = self.matrix[k].get(t, "-")
                color = ""
                if val == "OK": color = "\033[92m"   # Green
                elif val == "LOG": color = "\033[93m"  # Yellow
                elif val == "DAT": color = "\033[94m"  # Blue
                elif val == "MIS": color = "\033[91m"  # Red
                reset = "\033[0m"
                cells.append(f"{color}{val:<{col_width}}{reset}")
            
            output.append(f"{row_str} | {' | '.join(cells)}")
        
        output.append(sep_line)
        output.append(f"Showing last {len(self.keys)} cycles.")
        output.append("Legend: \033[92mOK\033[0m=Log+Data | \033[93mLOG\033[0m=Log Only | \033[94mDAT\033[0m=Data Only | \033[91mMIS\033[0m=Missing Log\n")
        
        return "\n".join(output)

    def render_html(self) -> str:
        """Returns HTML table string for website."""
        if not self.task_names: return "<p>No data found.</p>"

        html = ['<table class="inventory-table"><thead><tr><th>Date</th><th>Cyc</th><th>Type</th>']
        for t in self.task_names: html.append(f'<th>{t}</th>')
        html.append('</tr></thead><tbody>')

        for k in self.keys:
            date, cycle, run_type = k
            html.append(f'<tr><td>{date}</td><td>{cycle:02d}</td><td>{run_type}</td>')
            
            for t in self.task_names:
                val = self.matrix[k].get(t, "-")
                css = f"status-{val.lower()}" if val != "-" else "status-none"
                html.append(f'<td class="{css}">{val}</td>')
            html.append('</tr>')
        
        html.append('</tbody></table>')
        html.append('<div class="legend"><strong>Legend:</strong> <span class="status-ok">OK</span> Log+Data | <span class="status-log">LOG</span> Log Only | <span class="status-dat">DAT</span> Data Only | <span class="status-mis">MIS</span> Missing Log</div>')
        
        return "".join(html)
