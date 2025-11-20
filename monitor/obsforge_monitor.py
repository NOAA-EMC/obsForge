import sqlite3
from pathlib import Path
from datetime import datetime


class ObsforgeMonitor:
    def __init__(self, db_path="obsforge_monitor.db"):
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    # ------------------------------------------------------------
    # Database setup
    # ------------------------------------------------------------
    def init_db(self):
        cur = self.conn.cursor()

        # tasks
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            description TEXT,
            task_type TEXT
        );
        """)

        # obs spaces
        cur.execute("""
        CREATE TABLE IF NOT EXISTS obs_spaces (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            description TEXT
        );
        """)

        # optional collections
        cur.execute("""
        CREATE TABLE IF NOT EXISTS obs_space_collections (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            description TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS obs_space_collection_members (
            collection_id INTEGER,
            obs_space_id INTEGER,
            FOREIGN KEY(collection_id) REFERENCES obs_space_collections(id),
            FOREIGN KEY(obs_space_id) REFERENCES obs_spaces(id),
            PRIMARY KEY(collection_id, obs_space_id)
        );
        """)

        # task runs
        cur.execute("""
        CREATE TABLE IF NOT EXISTS task_runs (
            id INTEGER PRIMARY KEY,
            task_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            cycle INTEGER NOT NULL,
            run_type TEXT,
            start_time TEXT,
            end_time TEXT,
            runtime_sec REAL,
            notes TEXT,
            FOREIGN KEY(task_id) REFERENCES tasks(id)
        );
        """)

        # details
        cur.execute("""
        CREATE TABLE IF NOT EXISTS task_run_details (
            id INTEGER PRIMARY KEY,
            task_run_id INTEGER NOT NULL,
            obs_space_id INTEGER NOT NULL,
            obs_count INTEGER,
            runtime_sec REAL,
            FOREIGN KEY(task_run_id) REFERENCES task_runs(id),
            FOREIGN KEY(obs_space_id) REFERENCES obs_spaces(id)
        );
        """)

        # recommended indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_task_runs_task_cycle_date ON task_runs(task_id, cycle, date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trd_run ON task_run_details(task_run_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trd_space ON task_run_details(obs_space_id);")

        self.conn.commit()

    # ------------------------------------------------------------
    def reset_db(self):
        """Drop all tables (careful!)"""
        cur = self.conn.cursor()
        tables = ["task_run_details", "task_runs",
                  "obs_space_collection_members", "obs_space_collections",
                  "obs_spaces", "tasks"]
        for t in tables:
            cur.execute(f"DROP TABLE IF EXISTS {t}")
        self.conn.commit()
        self.init_db()

    # ------------------------------------------------------------
    # Insert helpers
    # ------------------------------------------------------------
    def add_task(self, name, description="", task_type=None):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO tasks(name, description, task_type)
            VALUES(?,?,?)
        """, (name, description, task_type))
        self.conn.commit()
        return cur.lastrowid

    def add_obs_space(self, name, description=""):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO obs_spaces(name, description)
            VALUES(?,?)
        """, (name, description))
        self.conn.commit()
        return cur.lastrowid

    def log_task_run(self, task_id, date, cycle, run_type, start_time, end_time, notes=None):
        runtime_sec = None
        if start_time and end_time:
            runtime_sec = (datetime.fromisoformat(end_time) -
                           datetime.fromisoformat(start_time)).total_seconds()
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO task_runs(task_id, date, cycle, run_type, start_time, end_time, runtime_sec, notes)
            VALUES(?,?,?,?,?,?,?,?)
        """, (task_id, date, cycle, run_type, start_time, end_time, runtime_sec, notes))
        self.conn.commit()
        return cur.lastrowid

    def log_task_run_detail(self, task_run_id, obs_space_id, obs_count, runtime_sec=None):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO task_run_details(task_run_id, obs_space_id, obs_count, runtime_sec)
            VALUES(?,?,?,?)
        """, (task_run_id, obs_space_id, obs_count, runtime_sec))
        self.conn.commit()
        return cur.lastrowid

    # ------------------------------------------------------------
    # Query/helpers for plotting and stats
    # ------------------------------------------------------------

    def fetch_task_time_series(self, task_name):
        """Return all runtime_sec values for a given task, ordered by date+cycle"""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT tr.date, tr.cycle, tr.runtime_sec
            FROM task_runs tr
            JOIN tasks t ON t.id = tr.task_id
            WHERE t.name = ?
            ORDER BY tr.date, tr.cycle
        """, (task_name,))
        return cur.fetchall()

    def fetch_obs_count_time_series(self, obs_space_name):
        """Return obs_count time series for one obs space"""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT tr.date, tr.cycle, d.obs_count
            FROM task_run_details d
            JOIN obs_spaces s ON s.id = d.obs_space_id
            JOIN task_runs tr ON tr.id = d.task_run_id
            WHERE s.name = ?
            ORDER BY tr.date, tr.cycle
        """, (obs_space_name,))
        return cur.fetchall()

    def fetch_obs_count_for_collection(self, collection_name):
        """Return aggregated obs counts for an obs-space collection"""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT tr.date, tr.cycle, SUM(d.obs_count) AS total_obs
            FROM task_run_details d
            JOIN task_runs tr ON tr.id = d.task_run_id
            JOIN obs_space_collection_members m ON m.obs_space_id = d.obs_space_id
            JOIN obs_space_collections c ON c.id = m.collection_id
            WHERE c.name = ?
            GROUP BY tr.date, tr.cycle
            ORDER BY tr.date, tr.cycle
        """, (collection_name,))
        return cur.fetchall()

    def fetch_all_runs(self):
        """Return all task runs with joined task names."""
        cur = self.conn.cursor()
        return cur.execute("""
            SELECT tr.id, t.name AS task, tr.date, tr.cycle, tr.run_type, tr.runtime_sec
            FROM task_runs tr
            JOIN tasks t ON t.id = tr.task_id
            ORDER BY tr.date, tr.cycle
        """).fetchall()

    def fetch_run_details(self, task_run_id):
        """Return all obs-space details for a task run."""
        cur = self.conn.cursor()
        return cur.execute("""
            SELECT s.name AS obs_space, d.obs_count, d.runtime_sec
            FROM task_run_details d
            JOIN obs_spaces s ON s.id = d.obs_space_id
            WHERE d.task_run_id = ?
        """, (task_run_id,)).fetchall()
