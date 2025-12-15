import argparse
import os
import shutil
from datetime import datetime
from pyobsforge.monitor.monitor_inventory import InventoryReport

# Simple CSS to make it look decent
CSS = """
body { font-family: sans-serif; margin: 20px; background: #f4f4f9; }
h1 { color: #333; }
.card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }
table.inventory-table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
table.inventory-table th, table.inventory-table td { padding: 8px 12px; border: 1px solid #ddd; text-align: center; }
table.inventory-table th { background-color: #eee; }
.status-ok { background-color: #d4edda; color: #155724; font-weight: bold; } /* Green */
.status-log { background-color: #fff3cd; color: #856404; } /* Yellow */
.status-dat { background-color: #d1ecf1; color: #0c5460; } /* Blue */
.status-mis { background-color: #f8d7da; color: #721c24; } /* Red */
.status-none { color: #ccc; }
.footer { font-size: 0.8em; color: #666; margin-top: 20px; }
"""

TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>ObsForge Monitor</title>
    <style>{css}</style>
</head>
<body>
    <h1>ObsForge Monitor Dashboard</h1>
    
    <div class="card">
        <h2>Cycle Inventory</h2>
        <p>Recent cycles and task completion status.</p>
        {inventory_table}
    </div>

    <div class="footer">Generated at {timestamp}</div>
</body>
</html>
"""

def generate_site(db_path, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 1. Generate Inventory HTML Fragment
    inv_report = InventoryReport(db_path, limit=100)
    inventory_html = inv_report.render_html()

    # 2. Build Index Page
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
    
    # FIX: Ensure keys match the placeholders in TEMPLATE ({inventory_table})
    html_content = TEMPLATE.format(
        css=CSS,
        inventory_table=inventory_html, 
        timestamp=timestamp
    )

    # 3. Write File
    index_path = os.path.join(output_dir, "index.html")
    with open(index_path, "w") as f:
        f.write(html_content)
    
    print(f"Website generated at: {index_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--out", required=True, help="Output directory for HTML files")
    args = parser.parse_args()
    
    generate_site(args.db, args.out)
