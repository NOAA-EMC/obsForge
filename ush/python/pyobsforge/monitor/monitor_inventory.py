import argparse
import sqlite3
import sys
import os
from collections import defaultdict

class InventoryReport:
    def __init__(self, db_path, limit=50):
        self.db_path = db_path
        self.limit = limit
        self.task_names = []
        self.matrix = defaultdict(dict)
        self.keys = []
        
        self._load_data()

    def _get_db_connection(self):
        if not os.path.exists(self.db_path):
            print(f"Error: Database not found at {self.db_path}")
            sys.exit(1)
        return sqlite3.connect(self.db_path)

    def _load_data(self):
        conn = self._get_db_connection()
        cur = conn.cursor()

        # 1. Get Tasks
        try:
            cur.execute("SELECT name FROM tasks ORDER BY name")
            self.task_names = [row[0] for row in cur.fetchall()]
        except sqlite3.OperationalError:
            return # DB likely empty

        if not self.task_names:
            return

        # 2. Get Run Data
        # We join with task_run_details to sum obs_count
        query = """
        SELECT 
            tr.date, tr.cycle, tr.run_type, t.name as task_name, tr.logfile,
            (SELECT SUM(obs_count) FROM task_run_details trd WHERE trd.task_run_id = tr.id) as total_obs
        FROM task_runs tr
        JOIN tasks t ON tr.task_id = t.id
        ORDER BY tr.date DESC, tr.cycle DESC, tr.run_type ASC
        """
        cur.execute(query)
        rows = cur.fetchall()
        
        seen_keys = set()
        for r in rows:
            date, cycle, run_type, task, logfile, total_obs = r
            k = (date, cycle, run_type)
            
            if k not in seen_keys:
                self.keys.append(k)
                seen_keys.add(k)
            
            # Determine Status
            has_log = logfile and "missing" not in (logfile or "")
            has_obs = (total_obs is not None and total_obs > 0)
            
            if has_log and has_obs: status = "OK"
            elif has_log and not has_obs: status = "Log"
            elif not has_log and has_obs: status = "Dat"
            elif not has_log and not has_obs: status = "Mis"
            else: status = "?"

            self.matrix[k][task] = status

    def render_cli(self):
        if not self.task_names:
            print("No data found.")
            return

        # Dynamic width calculation
        max_len = max(len(t) for t in self.task_names)
        col_width = max(8, max_len + 2)

        # Header
        header_cycle = f"{'DATE':<10} | {'CYC':<3} | {'TYPE':<6}"
        header_tasks = " | ".join([f"{t[:col_width-1]:<{col_width}}" for t in self.task_names])
        sep_line = "-" * (len(header_cycle) + 3 + len(header_tasks))
        
        print("\nInventory Report")
        print(sep_line)
        print(f"{header_cycle} | {header_tasks}")
        print(sep_line)

        count = 0
        for k in self.keys:
            if count >= self.limit: break
            date, cycle, run_type = k
            row_str = f"{date:<10} | {cycle:02d}  | {run_type:<6}"
            
            cells = []
            for t in self.task_names:
                val = self.matrix[k].get(t, "-")
                # ANSI Colors
                color = ""
                if val == "OK": color = "\033[92m"   # Green
                elif val == "Log": color = "\033[93m"  # Yellow
                elif val == "Dat": color = "\033[94m"  # Blue
                elif val == "Mis": color = "\033[91m"  # Red
                reset = "\033[0m"
                cells.append(f"{color}{val:<{col_width}}{reset}")
            
            print(f"{row_str} | {' | '.join(cells)}")
            count += 1
        
        print(sep_line)
        print(f"Showing last {count} cycles.")
        print(f"Legend: \033[92mOK\033[0m=Log+Data | \033[93mLog\033[0m=Log Only (0 Obs) | \033[94mDat\033[0m=Data Only | \033[91mMis\033[0m=Missing Log\n")

    def render_html(self):
        if not self.task_names:
            return "<p>No data found.</p>"

        html = [
            '<table class="inventory-table">',
            '<thead><tr>',
            '<th>Date</th><th>Cyc</th><th>Type</th>'
        ]
        for t in self.task_names:
            html.append(f'<th>{t}</th>')
        html.append('</tr></thead><tbody>')

        count = 0
        for k in self.keys:
            if count >= self.limit: break
            date, cycle, run_type = k
            
            html.append(f'<tr><td>{date}</td><td>{cycle:02d}</td><td>{run_type}</td>')
            
            for t in self.task_names:
                val = self.matrix[k].get(t, "-")
                css_class = f"status-{val.lower()}" if val != "-" else "status-none"
                html.append(f'<td class="{css_class}">{val}</td>')
            
            html.append('</tr>')
            count += 1
        
        html.append('</tbody></table>')
        
        # Add Legend to HTML output as well
        html.append('''
        <div style="margin-top: 10px; font-size: 0.9em;">
            <strong>Legend:</strong> 
            <span class="status-ok" style="padding: 2px 5px;">OK</span> Log+Data | 
            <span class="status-log" style="padding: 2px 5px;">Log</span> Log Only | 
            <span class="status-dat" style="padding: 2px 5px;">Dat</span> Data Only | 
            <span class="status-mis" style="padding: 2px 5px;">Mis</span> Missing Log
        </div>
        ''')
        
        return "".join(html)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate ObsForge Inventory Report")
    parser.add_argument("--db", required=True, help="Path to SQLite database file")
    parser.add_argument("--limit", type=int, default=50, help="Rows to show")
    parser.add_argument("--html", action="store_true", help="Output HTML snippet instead of CLI text")
    
    args = parser.parse_args()
    
    report = InventoryReport(args.db, args.limit)
    
    if args.html:
        print(report.render_html())
    else:
        report.render_cli()
