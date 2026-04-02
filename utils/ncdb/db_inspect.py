#!/usr/bin/env python3
import argparse
import sys
from data_service import ReportDataService
# from ncdb.data_service import ReportDataService

DESCRIPTION = """ObsForge Monitor Reporter
Commands: inventory, schema, stats, tables, query"""

class MonitorReporter:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description=DESCRIPTION, 
            formatter_class=argparse.RawDescriptionHelpFormatter)
        self.parser.add_argument("--db", required=True, help="Path to SQLite DB file")
        
        subparsers = self.parser.add_subparsers(dest="command", required=True)

        # Inventory
        p_inv = subparsers.add_parser("inventory")
        p_inv.add_argument("--limit", type=int, default=50)
        p_inv.add_argument("--type", dest="run_type")
        p_inv.set_defaults(func=self.handle_inventory)

        # Tables
        p_tab = subparsers.add_parser("tables")
        p_tab.add_argument("table_name", nargs="?")
        p_tab.add_argument("--limit", type=int)
        p_tab.add_argument("--filter")
        p_tab.set_defaults(func=self.handle_tables)

        # Query
        p_query = subparsers.add_parser("query")
        p_query.add_argument("sql")
        p_query.add_argument("--limit", type=int)
        p_query.set_defaults(func=self.handle_query)

        # Schema
        p_schema = subparsers.add_parser("schema")
        p_schema.add_argument("name")
        p_schema.set_defaults(func=self.handle_schema)

        # Stats
        p_stats = subparsers.add_parser("stats")
        p_stats.add_argument("pattern")
        p_stats.set_defaults(func=self.handle_stats)

    def _print_table(self, rows, headers):
        if not rows: return print("(No data)")
        str_rows = [[str(c) if c is not None else "" for c in r] for r in rows]
        widths = [max([len(str(h))] + [len(r[i]) for r in str_rows]) for i, h in enumerate(headers)]
        fmt = "  ".join([f"{{:<{w}}}" for w in widths])
        
        print(fmt.format(*headers))
        print("-" * (sum(widths) + 2 * len(widths)))
        for r in str_rows: print(fmt.format(*r))

    def handle_inventory(self, args):
        matrix = self.reader.get_inventory_matrix(run_type_filter=args.run_type, limit=args.limit)
        rows = []
        for item in matrix:
            if item['type'] == 'group':
                label = f"{item['start_date']}.{item['start_cycle']:02d} -> {item['end_date']}.{item['end_cycle']:02d}"
                rows.append([label, item['run_type'], f"[ALL OK] ({item['count']} cycles)"])
            else:
                label = f"{item['date']}.{item['cycle']:02d}"
                tasks = " | ".join([f"{t}: {s}" for t, s in item['tasks'].items()])
                rows.append([label, item['run_type'], tasks])
        self._print_table(rows, ["Cycle", "Run", "Task Status"])

    def handle_tables(self, args):
        if not args.table_name:
            print("\n".join(self.reader.fetch_table_names()))
        else:
            cols = self.reader.get_table_schema(args.table_name)
            rows = self.reader.get_raw_table_rows(args.table_name, limit=args.limit, filter_sql=args.filter)
            self._print_table(rows, cols)

    def handle_query(self, args):
        try:
            sql = f"{args.sql} LIMIT {args.limit}" if args.limit else args.sql
            cur = self.reader.conn.cursor().execute(sql)
            self._print_table(cur.fetchall(), [d[0] for d in cur.description] if cur.description else [])
        except Exception as e: print(f"Error: {e}")

    def handle_schema(self, args):
        details = self.reader.get_obs_space_schema(args.name)
        self._print_table([[d['group_name'], d['name']] for d in details], ["Group", "Variable"])

    def handle_stats(self, args):
        stats = self.reader.get_file_statistics(args.pattern)
        rows = [[s['file_path'].split('/')[-1], s['variable'], s['min_val'], s['max_val'], s['mean_val'], s['std_dev']] for s in stats]
        self._print_table(rows, ["File", "Variable", "Min", "Max", "Mean", "StdDev"])

    def run(self):
        args = self.parser.parse_args()
        try:
            self.reader = ReportDataService(args.db)
            args.func(args)
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)

if __name__ == "__main__":
    MonitorReporter().run()
