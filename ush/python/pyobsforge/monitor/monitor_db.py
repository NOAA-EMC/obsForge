import os
import sqlite3
from typing import Tuple, Optional, Set


class MonitorDB:
    """
    SQLite-backed DB for storing monitored task runs and obs-space data.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

        # Ensure DB directory exists
        parent = os.path.dirname(db_path)
        if parent and not os.path.exists(parent):
            raise FileNotFoundError(f"Directory does not exist: {parent}")

        # Create file if missing
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA foreign_keys = ON;")

        self._initialize_schema()

    # -----------------------------------------------------
    def _initialize_schema(self):
        """Create tables and indexes if missing."""
        cur = self.conn.cursor()

        # Table 1: obs_space_categories
        cur.execute("""
        CREATE TABLE IF NOT EXISTS obs_space_categories (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            description TEXT
        );
        """)

        # Table 2: obs_spaces (Obs Space must belong to one Category)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS obs_spaces (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            category_id INTEGER NOT NULL,
            description TEXT,
            FOREIGN KEY(category_id) REFERENCES obs_space_categories(id)
        );
        """)

        # Table 3: tasks
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            description TEXT
        );
        """)

        # Table 4: task_obs_space_map (Enforces disjoint set rule)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS task_obs_space_map (
            task_id INTEGER NOT NULL,
            obs_space_id INTEGER NOT NULL UNIQUE,
            FOREIGN KEY(task_id) REFERENCES tasks(id),
            FOREIGN KEY(obs_space_id) REFERENCES obs_spaces(id),
            PRIMARY KEY (task_id, obs_space_id)
        );
        """)

        # Table 5: task_runs
        cur.execute("""
        CREATE TABLE IF NOT EXISTS task_runs (
            id INTEGER PRIMARY KEY,
            task_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            cycle INTEGER NOT NULL,
            run_type TEXT NOT NULL DEFAULT 'unknown',
            logfile TEXT,
            start_time TEXT,
            end_time TEXT,
            runtime_sec REAL,
            notes TEXT,
            FOREIGN KEY(task_id) REFERENCES tasks(id),
            UNIQUE(task_id, date, cycle, run_type)
        );
        """)

        # Table 6: task_run_details
        cur.execute("""
        CREATE TABLE IF NOT EXISTS task_run_details (
            id INTEGER PRIMARY KEY,
            task_run_id INTEGER NOT NULL,
            obs_space_id INTEGER NOT NULL,
            obs_count INTEGER,
            runtime_sec REAL,
            FOREIGN KEY(task_run_id) REFERENCES task_runs(id),
            FOREIGN KEY(obs_space_id) REFERENCES obs_spaces(id),
            UNIQUE(task_run_id, obs_space_id)
        );
        """)

        # Indexes
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_task_runs_task_cycle_date
        ON task_runs(task_id, cycle, date);
        """)
        cur.execute("""CREATE INDEX IF NOT EXISTS idx_trd_run ON task_run_details(task_run_id);""")
        cur.execute("""CREATE INDEX IF NOT EXISTS idx_trd_space ON task_run_details(obs_space_id);""")

        self.conn.commit()

    # -----------------------------------------------------
    # Helper methods (Get/Create IDs)

    def get_or_create_task(self, name: str) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM tasks WHERE name=?", (name,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute("INSERT INTO tasks(name) VALUES(?)", (name,))
        self.conn.commit()
        return cur.lastrowid

    def get_or_create_category(self, name: str) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM obs_space_categories WHERE name=?", (name,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute("INSERT INTO obs_space_categories(name) VALUES(?)", (name,))
        self.conn.commit()
        return cur.lastrowid

    def get_or_create_obs_space(self, name: str, category_id: int) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM obs_spaces WHERE name=?", (name,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute(
            "INSERT INTO obs_spaces(name, category_id) VALUES(?,?)",
            (name, category_id)
        )
        self.conn.commit()
        return cur.lastrowid

    def set_task_obs_space_mapping(self, task_id: int, obs_space_id: int):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO task_obs_space_map(task_id, obs_space_id) VALUES(?,?)",
            (task_id, obs_space_id)
        )
        self.conn.commit()
        return cur.lastrowid

    # -----------------------------------------------------
    # Logging methods

    def log_task_run(self, task_id: int, date: str, cycle: int, run_type: str,
                     logfile: str = None, start_time: str = None,
                     end_time: str = None, runtime_sec: float = None,
                     notes: str = None) -> tuple[int, str]:
        
        with self.conn:
            # Check if row exists to determine status (Insert vs Update)
            cursor = self.conn.execute("""
                SELECT id FROM task_runs 
                WHERE task_id=? AND date=? AND cycle=? AND run_type=?
            """, (task_id, date, cycle, run_type))
            existing = cursor.fetchone()
            
            action = "UPDATED" if existing else "INSERTED"

            # Execute the Upsert
            cur = self.conn.execute("""
                INSERT INTO task_runs (
                    task_id, date, cycle, run_type, 
                    logfile, start_time, end_time, runtime_sec, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id, date, cycle, run_type) 
                DO UPDATE SET
                    logfile = excluded.logfile,
                    start_time = excluded.start_time,
                    end_time = excluded.end_time,
                    runtime_sec = excluded.runtime_sec,
                    notes = excluded.notes
            """, (task_id, date, cycle, run_type, logfile, start_time, 
                  end_time, runtime_sec, notes))
            
            row_id = cur.lastrowid if cur.lastrowid else (existing[0] if existing else 0)
            
            # If we missed the ID fetch (sqlite quirk on update), fetch it now
            if not row_id:
                 cursor = self.conn.execute("""
                    SELECT id FROM task_runs 
                    WHERE task_id=? AND date=? AND cycle=? AND run_type=?
                """, (task_id, date, cycle, run_type))
                 row_id = cursor.fetchone()[0]

            return row_id, action

    def log_task_run_detail(self, task_run_id: int, obs_space_id: int, 
                            obs_count: int, runtime_sec: float = 0.0):
        """
        Logs observation stats. If the record exists, it updates the counts.
        """
        with self.conn:
            self.conn.execute("""
                INSERT INTO task_run_details (
                    task_run_id, obs_space_id, obs_count, runtime_sec
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(task_run_id, obs_space_id) 
                DO UPDATE SET 
                    obs_count = excluded.obs_count,
                    runtime_sec = excluded.runtime_sec
            """, (task_run_id, obs_space_id, obs_count, runtime_sec))

    # -----------------------------------------------------
    # Inspection Methods (For Monitor Logic)

    def get_latest_cycle(self) -> Optional[Tuple[str, str]]:
        """Returns the NEWEST (date, cycle) tuple in DB. E.g. ('20251120', '18')"""
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT date, cycle FROM task_runs ORDER BY date DESC, cycle DESC LIMIT 1")
            row = cur.fetchone()
            if row:
                return (str(row[0]), f"{row[1]:02d}")
            return None
        except Exception:
            return None

    def get_oldest_cycle(self) -> Optional[Tuple[str, str]]:
        """Returns the OLDEST (date, cycle) tuple in DB."""
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT date, cycle FROM task_runs ORDER BY date ASC, cycle ASC LIMIT 1")
            row = cur.fetchone()
            if row:
                return (str(row[0]), f"{row[1]:02d}")
            return None
        except Exception:
            return None

    def get_existing_cycles(self, run_type: str) -> Set[Tuple[str, str]]:
        """
        Returns a set of (date_str, cycle_str) for ALL runs of a specific type.
        Used to detect gaps by comparing against the filesystem.
        """
        cur = self.conn.cursor()
        try:
            cur.execute(
                "SELECT DISTINCT date, cycle FROM task_runs WHERE run_type=?",
                (run_type,)
            )
            found = set()
            for row in cur.fetchall():
                # Format cycle as 2-digit string to match filesystem "00", "06"
                found.add((str(row[0]), f"{row[1]:02d}"))
            return found
        except Exception:
            return set()

    def get_all_run_cycles(self) -> set:
        """
        Returns a set of (run_type, date, cycle) tuples for ALL recorded runs.
        Used by the Scanner to skip directories that are already fully processed.
        """
        with self.conn:
            cur = self.conn.execute("SELECT run_type, date, cycle FROM task_runs")
            # Returns: {('gdas', '20251210', 12), ('gfs', '20251210', 12), ...}
            rows = cur.fetchall()
            # Ensure cycle is int for consistent comparison
            return {(row[0], row[1], int(row[2])) for row in rows}
