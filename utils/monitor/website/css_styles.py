CSS_STYLES = """
/* BASE STYLES */
body {
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    margin: 0;
    background: #f4f7f6;
    color: #333;
}
header {
    background: #2c3e50;
    color: white;
    padding: 15px 20px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}
h1 { margin: 0; font-size: 1.5em; }
a { text-decoration: none; color: inherit; }

/* NAVIGATION TABS */
.nav-tabs {
    display: flex;
    gap: 10px;
    background: #34495e;
    padding: 10px 20px;
}
.nav-btn {
    color: #ecf0f1;
    padding: 8px 16px;
    border-radius: 4px;
    background: #2c3e50;
    font-weight: bold;
    transition: background 0.2s;
}
.nav-btn.active { background: #3498db; color: white; }
.nav-btn:hover { background: #2980b9; }

/* PAGE LAYOUT */
.container {
    max-width: 1400px;
    margin: 20px auto;
    padding: 0 20px;
}
.section {
    background: white;
    padding: 20px;
    border-radius: 8px;
    box-shadow: 0 2px 5px rgba(0,0,0,0.05);
    margin-bottom: 20px;
}
h2 {
    border-bottom: 2px solid #eee;
    padding-bottom: 10px;
    margin-top: 0;
    color: #2c3e50;
}
h3 { margin: 0 0 10px 0; color: #555; font-size: 1.1em; }

/* INVENTORY MATRIX TABLE */
table.matrix {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.85em;
}
th, td {
    padding: 6px 10px;
    border: 1px solid #eee;
    text-align: left;
}
th { background: #f8f9fa; color: #7f8c8d; }

/* STATUS COLORS */
.status-OK { color: #27ae60; font-weight: bold; }
.status-FAIL { color: #e74c3c; font-weight: bold; }
.status-WARNING { color: #f39c12; font-weight: bold; }
.status-MIS { color: #95a5a6; }
.group-row {
    background: #eafaf1;
    color: #27ae60;
    font-weight: bold;
    cursor: default;
}

/* LEGEND */
.legend {
    font-size: 0.85em;
    margin-bottom: 10px;
    padding: 5px;
    background: #fdfdfd;
    border: 1px solid #eee;
    display: inline-block;
    border-radius: 4px;
}
.legend span { margin-right: 15px; font-weight: bold; }
.dot {
    height: 10px;
    width: 10px;
    display: inline-block;
    border-radius: 50%;
    margin-right: 5px;
}

/* FLAGGED FILES TABLE (Scrollable) */
.flag-scroll-box {
    max-height: 400px;
    overflow-y: auto;
    border: 1px solid #eee;
}
.flag-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9em;
}
.flag-table th {
    background: #fdfefe;
    color: #7f8c8d;
    border-bottom: 2px solid #eee;
    padding: 8px;
    text-align: left;
    position: sticky;
    top: 0;
}
.flag-table td { border-bottom: 1px solid #f0f0f0; padding: 8px; }
.flag-table tr:hover { background: #f9f9f9; }

/* PLOT GRID */
.plot-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(450px, 1fr));
    gap: 20px;
}
.plot-card {
    background: #fff;
    border: 1px solid #eee;
    padding: 10px;
    border-radius: 4px;
    text-align: center;
    transition: box-shadow 0.2s;
}
.plot-card:hover { box-shadow: 0 4px 8px rgba(0,0,0,0.1); }
.plot-card img { max-width: 100%; height: auto; }
.no-plot {
    color: #999;
    font-style: italic;
    padding: 40px;
    background: #fafafa;
}

/* DOMAIN INFO BOX */
.domain-info {
    font-size: 0.85em;
    color: #666;
    margin-bottom: 8px;
    background: #f8f9fa;
    padding: 4px 8px;
    border-radius: 4px;
    display: inline-block;
}

/* HISTORY TOGGLE SWITCH */
.toggle-control {
    text-align: right;
    margin-bottom: 10px;
    font-size: 0.9em;
    user-select: none;
}
.toggle-label {
    cursor: pointer;
    color: #3498db;
    font-weight: bold;
    display: inline-flex;
    align-items: center;
    gap: 5px;
}
.toggle-label:hover { color: #2980b9; }
input[type="checkbox"].history-toggle { display: none; }

/* Visibility Logic: Unchecked (Default) = Show All */
.plot-img-all { display: block; }
.plot-img-7d { display: none; }
.toggle-text-all { display: inline; }
.toggle-text-7d { display: none; }

#global-history-toggle:checked ~ .container .plot-img-all { display: none; }
#global-history-toggle:checked ~ .container .plot-img-7d { display: block; }
#global-history-toggle:checked ~ .container .toggle-text-all { display: none; }
#global-history-toggle:checked ~ .container .toggle-text-7d { display: inline; }
"""
