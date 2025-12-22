from collections import defaultdict
from pyobsforge.monitor.database.db_reader import DBReader

class InventoryReport:
    def __init__(self, reader: DBReader, run_type_filter=None, limit=50):
        self.reader = reader
        self.run_type_filter = run_type_filter
        self.limit = limit
        self.task_names = []
        self.matrix = defaultdict(dict)
        self.keys = []
        self._build_report()

    def _build_report(self):
        self.task_names = self.reader.get_task_list()
        if not self.task_names: return

        rows = self.reader.get_inventory_matrix(self.run_type_filter, self.limit)
        seen_keys = set()
        for r in rows:
            key = (r['date'], r['cycle'], r['run_type'])
            if key not in seen_keys:
                self.keys.append(key)
                seen_keys.add(key)
            
            for task_name, status_obj in r['tasks'].items():
                self.matrix[key][task_name] = status_obj['status']

    def render_cli(self) -> str:
        if not self.task_names: return "No data found."
        output = []
        max_len = max(len(t) for t in self.task_names)
        col_width = max(8, max_len + 2)

        header_cycle = f"{'DATE':<10} | {'CYC':<3} | {'TYPE':<6}"
        header_tasks = " | ".join([f"{t[:col_width-1]:<{col_width}}" for t in self.task_names])
        sep_line = "-" * (len(header_cycle) + 3 + len(header_tasks))
        
        output.append("\nInventory Report")
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
                elif val == "FAIL": color = "\033[91m" # Red
                elif val == "DEAD": color = "\033[35m" # Magenta
                elif val == "RUN": color = "\033[94m"  # Blue
                reset = "\033[0m"
                cells.append(f"{color}{val:<{col_width}}{reset}")
            output.append(f"{row_str} | {' | '.join(cells)}")
        return "\n".join(output)

    def render_html(self) -> str:
        if not self.task_names: return "<p>No data found.</p>"
        html = ['<table class="inventory-table"><thead><tr><th>Date</th><th>Cyc</th><th>Type</th>']
        for t in self.task_names: html.append(f'<th>{t}</th>')
        html.append('</tr></thead><tbody>')

        for k in self.keys:
            date, cycle, run_type = k
            html.append(f'<tr><td>{date}</td><td>{cycle:02d}</td><td>{run_type}</td>')
            for t in self.task_names:
                val = self.matrix[k].get(t, "-")
                # Normalize CSS class
                css_class = "status-none"
                if val == "OK": css_class = "status-ok"
                elif val == "FAIL": css_class = "status-mis"
                elif val == "DEAD": css_class = "status-mis"
                elif val == "RUN": css_class = "status-dat"
                
                html.append(f'<td class="{css_class}">{val}</td>')
            html.append('</tr>')
        html.append('</tbody></table>')
        return "".join(html)
