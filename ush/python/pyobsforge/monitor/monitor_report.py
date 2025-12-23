#!/usr/bin/env python3
import sys
import argparse
import os
from datetime import timedelta, datetime

# --- ARCHITECTURE IMPORTS ---
try:
    from pyobsforge.monitor.reporting.data_service import ReportDataService
except ImportError:
    # Bootstrap path if needed
    sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))
    from pyobsforge.monitor.reporting.data_service import ReportDataService

try:
    from pyobsforge.monitor.reporting.plot_generator import PlotGenerator
    HAS_PLOT = True
except ImportError:
    HAS_PLOT = False

DESCRIPTION = """
ObsForge Monitor Reporter
Commands:
  inventory    Show task status matrix
  schema       Show Obs Space definition
  stats        Show file statistics
  tables       Inspect DB tables
  query        Execute SQL
"""

class MonitorReporter:
    def __init__(self, db_path: str):
        try:
            self.reader = ReportDataService(db_path)
        except Exception as e:
            print(f"Error opening DB: {e}")
            sys.exit(1)
        
        self.parser = argparse.ArgumentParser(description=DESCRIPTION, formatter_class=argparse.RawDescriptionHelpFormatter)
        self.parser.add_argument("--db", required=True, help="Path to SQLite DB file")
        subparsers = self.parser.add_subparsers(dest="command", required=True)

        # Commands
        p_inv = subparsers.add_parser("inventory", help="Show matrix")
        p_inv.add_argument("--limit", type=int, default=50)
        p_inv.add_argument("--type", dest="run_type")

        p_tab = subparsers.add_parser("tables")
        p_tab.add_argument("table_name", nargs="?")
        p_tab.add_argument("--limit", type=int, default=None)
        p_tab.add_argument("--filter", default=None)

        p_query = subparsers.add_parser("query")
        p_query.add_argument("sql")
        p_query.add_argument("--limit", type=int, default=None)

        p_schema = subparsers.add_parser("schema")
        p_schema.add_argument("name")

        p_stats = subparsers.add_parser("stats")
        p_stats.add_argument("pattern")

    def run(self):
        args = self.parser.parse_args()
        if args.command == "inventory": self.handle_inventory(args)
        elif args.command == "tables": self.handle_tables(args)
        elif args.command == "query": self.handle_query(args)
        elif args.command == "schema": self.handle_schema(args)
        elif args.command == "stats": self.handle_stats(args)
        else: self.parser.print_help()

    def _print_table(self, rows, headers, tablefmt="psql"):
        if not rows:
            print("(No data)")
            return
        
        # Simple printer to avoid dependencies
        str_rows = [[str(c) if c is not None else "" for c in r] for r in rows]
        widths = [len(h) for h in headers]
        for r in str_rows:
            for i, c in enumerate(r):
                if i < len(widths): widths[i] = max(widths[i], len(c))
        
        fmt = "  ".join([f"{{:<{w}}}" for w in widths])
        print(fmt.format(*headers))
        print("-" * (sum(widths) + 2*len(widths)))
        for r in str_rows:
            print(fmt.format(*r))

    # --- HANDLERS ---

    def handle_inventory(self, args):
        # Uses the compressed inventory logic
        matrix = self.reader.get_inventory_matrix(run_type_filter=args.run_type, limit=args.limit)
        headers = ["Cycle", "Run", "Task Status"]
        rows = []
        
        for item in matrix:
            if item['type'] == 'group':
                # Handle Collapsed Row
                label = f"{item['start_date']}.{item['start_cycle']:02d} -> {item['end_date']}.{item['end_cycle']:02d}"
                rows.append([label, item['run_type'], f"[ALL OK] ({item['count']} cycles)"])
            else:
                # Handle Single Row
                label = f"{item['date']}.{item['cycle']:02d}"
                task_str = " | ".join([f"{t}: {s}" for t, s in item['tasks'].items()])
                rows.append([label, item['run_type'], task_str])
        
        self._print_table(rows, headers)

    def handle_tables(self, args):
        if not args.table_name:
            print("\n".join(self.reader.fetch_table_names()))
        else:
            cols = self.reader.get_table_schema(args.table_name)
            rows = self.reader.get_raw_table_rows(args.table_name, limit=args.limit, filter_sql=args.filter)
            self._print_table(rows, cols)

    def handle_query(self, args):
        try:
            sql = args.sql + (f" LIMIT {args.limit}" if args.limit else "")
            # Direct connection access for arbitrary query
            with self.reader.conn.get_cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                headers = [d[0] for d in cur.description] if cur.description else []
                self._print_table(rows, headers)
        except Exception as e:
            print(f"Error: {e}")

    def handle_schema(self, args):
        details = self.reader.get_obs_space_schema_details(args.name)
        rows = [[d['group_name'], d['var_name'], d['data_type'], d['dimensionality']] for d in details]
        self._print_table(rows, ["Group", "Variable", "Type", "Dims"])

    def handle_stats(self, args):
        stats = self.reader.get_file_statistics(args.pattern)
        rows = [[s['file_path'].split('/')[-1], s['variable'], s['min_val'], s['max_val'], s['mean_val'], s['std_dev']] for s in stats]
        self._print_table(rows, ["File", "Variable", "Min", "Max", "Mean", "StdDev"])

if __name__ == "__main__":
    if "--db" not in sys.argv:
        print("Error: --db is required")
        sys.exit(1)
    db = sys.argv[sys.argv.index("--db")+1]
    MonitorReporter(db).run()
