#!/usr/bin/env python3
# monitor_report.py
# Main entry point for inspecting the ObsForge Monitor Database.

import sys
import argparse
from datetime import timedelta, datetime

# --- ARCHITECTURE IMPORTS ---
from pyobsforge.monitor.database.db_reader import DBReader
from pyobsforge.monitor.reporting.inventory_report import InventoryReport

# Optional plotting import
try:
    from pyobsforge.monitor.reporting.plotutil import MonitorPlotter
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

DESCRIPTION = """
ObsForge Monitor Reporter

Inspects task runs, observation counts, and cycle continuity.
Commands:
  inventory    Show matrix of task status per cycle
  show         Show text reports (ranges, time, obs)
  plot         Generate plots (time, obs)
  tables       Inspect raw DB tables
"""

class MonitorReporter:
    def __init__(self, db_path: str):
        # Initialize Data Access Layer
        try:
            self.reader = DBReader(db_path)
        except Exception as e:
            print(f"Error opening DB: {e}")
            sys.exit(1)
        
        self.parser = argparse.ArgumentParser(
            description=DESCRIPTION,
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        self.parser.add_argument("--db", required=True, help="Path to SQLite DB file")

        subparsers = self.parser.add_subparsers(dest="command", required=True)

        # ----------------------------------------------------------------------
        # COMMAND: INVENTORY
        # ----------------------------------------------------------------------
        p_inv = subparsers.add_parser("inventory", help="Show matrix of task status.")
        p_inv.add_argument("--limit", type=int, default=50, help="Number of cycles to show")
        p_inv.add_argument("--type", dest="run_type", help="Filter by run type (e.g. gdas, gfs)")

        # ----------------------------------------------------------------------
        # COMMAND: TABLES
        # ----------------------------------------------------------------------
        p_tables = subparsers.add_parser("tables", help="List or inspect raw DB tables.")
        p_tables.add_argument("table_name", nargs="?", help="Table name")
        p_tables.add_argument("--limit", type=int, default=20, help="Limit rows printed")
        p_tables.add_argument("--filter", default=None, help="SQL WHERE clause")

        # ----------------------------------------------------------------------
        # COMMAND: SHOW
        # ----------------------------------------------------------------------
        p_show = subparsers.add_parser("show", help="Show text-based reports.")
        show_sub = p_show.add_subparsers(dest="show_command", required=True)

        # show ranges
        show_sub.add_parser("ranges", help="Report available continuous cycle ranges.")

        # show time
        ps_time = show_sub.add_parser("time", help="Show task runtimes.")
        ps_time.add_argument("--days", type=int, default=None)
        ps_time.add_argument("--task", default=None)
        ps_time.add_argument("--run-type", help="Filter by run type")

        # show obs
        ps_obs = show_sub.add_parser("obs", help="Show observation counts.")
        ps_obs.add_argument("--days", type=int, default=None)
        ps_obs.add_argument("--run-type", help="Filter by run type")
        ps_obs.add_argument("--aggregate", action="store_true")
        g_obs = ps_obs.add_mutually_exclusive_group()
        g_obs.add_argument("--obs-space")
        g_obs.add_argument("--obs-category")

        # ----------------------------------------------------------------------
        # COMMAND: PLOT
        # ----------------------------------------------------------------------
        p_plot = subparsers.add_parser("plot", help="Generate plots.")
        plot_sub = p_plot.add_subparsers(dest="plot_command", required=True)

        # plot time
        pp_time = plot_sub.add_parser("time", help="Plot task runtimes.")
        pp_time.add_argument("--days", type=int, default=None)
        pp_time.add_argument("--task", default=None)
        pp_time.add_argument("--output", help="Save to file")

        # plot obs
        pp_obs = plot_sub.add_parser("obs", help="Plot obs counts.")
        pp_obs.add_argument("--days", type=int, default=None)
        pp_obs.add_argument("--output", help="Save to file")
        gp_obs = pp_obs.add_mutually_exclusive_group(required=True)
        gp_obs.add_argument("--obs-space")
        gp_obs.add_argument("--obs-category")

    def run(self):
        args = self.parser.parse_args()

        if args.command == "inventory":
            self.handle_inventory(args)
        elif args.command == "tables":
            self.handle_tables(args)
        elif args.command == "show":
            self.handle_show(args)
        elif args.command == "plot":
            self.handle_plot(args)
        else:
            self.parser.print_help()

    # --- Handlers ---

    def handle_inventory(self, args):
        report = InventoryReport(self.reader, run_type_filter=args.run_type, limit=args.limit)
        print(report.render_cli())

    def handle_tables(self, args):
        if not args.table_name:
            tables = self.reader.fetch_table_names()
            print("\nDatabase Tables:")
            print("-" * 20)
            print("\n".join(tables))
            print("-" * 20)
        else:
            tname = args.table_name
            cols = self.reader.get_table_schema(tname)
            if not cols:
                print(f"Table '{tname}' not found or empty.")
                return

            rows = self.reader.get_raw_table_rows(tname, limit=args.limit, filter_sql=args.filter)
            
            print(f"Table: {tname}")
            sep = "-" * (len(" | ".join(cols)) + 5)
            print(sep)
            print(" | ".join(cols))
            print(sep)
            
            if not rows:
                print("(no rows)")
            else:
                for r in rows:
                    print(" | ".join(str(x) for x in r))
            print(sep)

    def handle_show(self, args):
        if args.show_command == "ranges":
            self._print_ranges(self.reader.get_cycle_ranges())
            return

        days_str = f"Last {args.days} days" if args.days is not None else "All Time"
        if hasattr(args, 'run_type') and args.run_type:
            days_str += f" ({args.run_type})"

        if args.show_command == "time":
            rows = self.reader.get_task_timings(args.days, args.task, run_type=args.run_type)
            if not rows:
                print("No data found.")
                return
            print(f"\nTask Runtimes ({days_str}):")
            print("-" * 65)
            print("Date         Cycle | Task             | Runtime (s)")
            print("-" * 65)
            for r in rows:
                print(f"{r['date']} {r['cycle']:02d}    | {r['task']:15s} | {r['duration']:.2f}")
            print("-" * 65)

        elif args.show_command == "obs":
            if args.aggregate:
                rows = self.reader.get_obs_totals(args.days, run_type=args.run_type)
                print(f"\nTotal Obs by Space ({days_str}):")
                print("-" * 40)
                for name, tot in rows:
                    print(f"{name:30s} {tot}")
                print("-" * 40)
            elif args.obs_space:
                rows = self.reader.get_obs_counts_by_space(args.obs_space, args.days, run_type=args.run_type)
                print(f"\nObs Count: {args.obs_space} ({days_str}):")
                for r in rows:
                    print(f"{r['date']}.{r['cycle']:02d} : {r['count']}")
            elif args.obs_category:
                rows = self.reader.get_obs_counts_by_category(args.obs_category, args.days, run_type=args.run_type)
                print(f"\nObs Category: {args.obs_category} ({days_str}):")
                for r in rows:
                    print(f"{r['date']}.{r['cycle']:02d} : {r['count']}")

    def _print_ranges(self, data):
        # data is {run_type: [(date, cyc), ...]} or similar depending on DBReader impl
        # We need to adapt the reader output to datetime objects for gap logic
        print("\nAvailable Cycle Ranges per Run Type")
        print("=" * 60)
        
        for r_type in sorted(data.keys()):
            # Assuming DBReader might return empty list if not implemented fully yet
            raw_tuples = self.reader.conn.execute(
                "SELECT date, cycle FROM task_runs WHERE run_type=? ORDER BY date, cycle", 
                (r_type,)
            ).fetchall()
            
            dts = []
            for r in raw_tuples:
                try:
                    dts.append(datetime.strptime(f"{r[0]}{r[1]:02d}", "%Y%m%d%H"))
                except: pass
            
            print(f"\nRun Type: {r_type}")
            if not dts: 
                print("  (No data)")
                continue
            
            start, prev = dts[0], dts[0]
            gap = timedelta(hours=6) # Standard cycle gap
            
            for curr in dts[1:]:
                if curr - prev > gap:
                    self._print_range_line(start, prev)
                    start = curr
                prev = curr
            self._print_range_line(start, prev)

    def _print_range_line(self, start, end):
        s_str = start.strftime('%Y%m%d%H')
        e_str = end.strftime('%Y%m%d%H')
        if s_str == e_str:
            print(f"  - {s_str}")
        else:
            print(f"  - {s_str} through {e_str}")

    def handle_plot(self, args):
        if not HAS_MATPLOTLIB:
            print("[Error] Plotting unavailable. Matplotlib not installed.")
            sys.exit(1)

        # Pass the Reader (which contains the connection logic)
        plotter = MonitorPlotter(self.reader)

        if args.plot_command == "time":
            plotter.plot_timings(args.task, args.days, args.output)
        elif args.plot_command == "obs":
            plotter.plot_obs(args.obs_space, args.obs_category, args.days, args.output)

if __name__ == "__main__":
    # Pre-parse just for DB path to init the object
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--db", required=True)
    try:
        temp_args, _ = pre_parser.parse_known_args()
    except argparse.ArgumentError:
        # Let main parser handle help/error
        pass
        
    # Main execution logic is inside the class to keep it clean
    # But we need args to init. So let's just parse fully in __init__? 
    # Or cleaner: Just check sys.argv for --db before class init if needed, 
    # but argparse handles requirement well enough.
    
    # Simple fix: Let the class parse args. 
    # Note: We need db path to init DBReader BEFORE parsing commands 
    # because subcommands might depend on DB introspection (though not currently).
    
    # Robust Entry:
    if "--db" not in sys.argv:
        print("Error: --db argument is required")
        sys.exit(1)
        
    db_val = sys.argv[sys.argv.index("--db") + 1]
    app = MonitorReporter(db_val)
    app.run()
