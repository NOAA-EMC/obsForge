import sqlite3
import os
import logging
from typing import Optional, Any

logger = logging.getLogger("MonitorDB")

class MonitorDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_path()
        # Enable Auto-commit (isolation_level=None) for immediate visibility
        self.conn = sqlite3.connect(db_path, isolation_level=None)
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self._init_schema()

    def _ensure_path(self):
        parent = os.path.dirname(self.db_path)
        if parent and not os.path.exists(parent):
            try:
                os.makedirs(parent)
            except OSError:
                pass

    def _init_schema(self):
        # ------------------------------------------------------------------
        # 1. KNOWLEDGE BASE (The Truth)
        # ------------------------------------------------------------------
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS obs_space_categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS obs_spaces (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category_id INTEGER,
                FOREIGN KEY(category_id) REFERENCES obs_space_categories(id),
                UNIQUE(name, category_id)
            )
        """)

        # Key-Value store for learned metadata (e.g. variables, platform)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS obs_space_properties (
                obs_space_id INTEGER NOT NULL,
                property_key TEXT NOT NULL,
                property_value TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(obs_space_id) REFERENCES obs_spaces(id) ON DELETE CASCADE,
                PRIMARY KEY(obs_space_id, property_key)
            )
        """)

        # ------------------------------------------------------------------
        # 2. OPERATIONAL TABLES (Execution History)
        # ------------------------------------------------------------------
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS task_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                cycle INTEGER NOT NULL,
                run_type TEXT,
                
                -- Execution Details
                job_id TEXT,
                status TEXT,        -- SUCCEEDED, FAILED, RUNNING, DEAD
                exit_code INTEGER,
                attempt INTEGER,    -- Try number
                host TEXT,
                
                -- Timings & Logs
                logfile TEXT,
                start_time TEXT,
                end_time TEXT,
                runtime_sec REAL,
                
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(task_id) REFERENCES tasks(id),
                UNIQUE(task_id, date, cycle, run_type)
            )
        """)

        # ------------------------------------------------------------------
        # 3. PHYSICAL INVENTORY (The Evidence)
        # ------------------------------------------------------------------
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS file_inventory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_run_id INTEGER NOT NULL,
                obs_space_id INTEGER,  -- Nullable if file is uncategorized/log
                
                file_path TEXT NOT NULL,
                file_type TEXT,        -- nc, bufr, log
                
                -- Integrity & Metrics
                integrity_status TEXT, -- OK, CORRUPT, MISSING, EMPTY, BAD_META
                file_size_bytes INTEGER,
                obs_count INTEGER,
                error_message TEXT,
                
                -- Raw Metadata (JSON) found in this specific file
                metadata TEXT,  
                
                FOREIGN KEY(task_run_id) REFERENCES task_runs(id) ON DELETE CASCADE,
                FOREIGN KEY(obs_space_id) REFERENCES obs_spaces(id),
                UNIQUE(task_run_id, file_path)
            )
        """)

    def _execute_log(self, sql, params=()):
        """Internal helper to execute SQL."""
        return self.conn.execute(sql, params)

    # ------------------------------------------------------------------
    # ENTITY MANAGEMENT
    # ------------------------------------------------------------------

    def get_or_create_task(self, name: str) -> int:
        self._execute_log("INSERT OR IGNORE INTO tasks(name) VALUES(?)", (name,))
        cur = self._execute_log("SELECT id FROM tasks WHERE name=?", (name,))
        return cur.fetchone()[0]

    def get_or_create_category(self, name: str) -> int:
        self._execute_log("INSERT OR IGNORE INTO obs_space_categories(name) VALUES(?)", (name,))
        cur = self._execute_log("SELECT id FROM obs_space_categories WHERE name=?", (name,))
        return cur.fetchone()[0]

    def get_or_create_obs_space(self, name: str, category_id: int) -> int:
        self._execute_log(
            "INSERT OR IGNORE INTO obs_spaces(name, category_id) VALUES(?, ?)", 
            (name, category_id)
        )
        cur = self._execute_log(
            "SELECT id FROM obs_spaces WHERE name=? AND category_id=?", 
            (name, category_id)
        )
        return cur.fetchone()[0]

    def set_obs_space_property(self, obs_space_id: int, key: str, value: Any):
        """
        Upserts a learned property into the Knowledge Base.
        """
        val_str = str(value)
        self._execute_log("""
            INSERT INTO obs_space_properties(obs_space_id, property_key, property_value)
            VALUES(?, ?, ?)
            ON CONFLICT(obs_space_id, property_key) 
            DO UPDATE SET 
                property_value=excluded.property_value, 
                updated_at=CURRENT_TIMESTAMP
        """, (obs_space_id, key, val_str))

    # ------------------------------------------------------------------
    # LOGGING EXECUTION & INVENTORY
    # ------------------------------------------------------------------

    def log_task_run(self, task_id, date, cycle, run_type, 
                     job_id=None, status=None, exit_code=None, attempt=None, host=None,
                     logfile=None, start_time=None, end_time=None, runtime_sec=None):
        """
        Upserts Task Run with full execution details.
        Returns: (row_id, action_string)
        """
        action = "updated"
        
        with self.conn:
            # Check existence
            cursor = self._execute_log("""
                SELECT id FROM task_runs 
                WHERE task_id=? AND date=? AND cycle=? AND run_type=?
            """, (task_id, date, cycle, run_type))
            row = cursor.fetchone()

            if row:
                tr_id = row[0]
                # Update existing record
                self._execute_log("""
                    UPDATE task_runs SET 
                        job_id=?, status=?, exit_code=?, attempt=?, host=?,
                        logfile=?, start_time=?, end_time=?, runtime_sec=?, 
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                """, (job_id, status, exit_code, attempt, host, 
                      logfile, start_time, end_time, runtime_sec, tr_id))
            else:
                action = "inserted"
                # Insert new record
                cur = self._execute_log("""
                    INSERT INTO task_runs (
                        task_id, date, cycle, run_type, 
                        job_id, status, exit_code, attempt, host,
                        logfile, start_time, end_time, runtime_sec
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (task_id, date, cycle, run_type, job_id, status, exit_code, attempt, host,
                      logfile, start_time, end_time, runtime_sec))
                tr_id = cur.lastrowid

        return tr_id, action

    def log_file_inventory(self, task_run_id: int, obs_space_id: Optional[int], 
                           path: str, integrity: str, size: int, 
                           obs_count: int = 0, error_msg: str = None, 
                           metadata_json: str = None):
        """
        Records a physical file found during the scan.
        Uses UPSERT to handle re-scans.
        """
        file_type = path.split('.')[-1] if '.' in path else 'unknown'
        
        self._execute_log("""
            INSERT INTO file_inventory (
                task_run_id, obs_space_id, file_path, file_type,
                integrity_status, file_size_bytes, obs_count, error_message, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_run_id, file_path)
            DO UPDATE SET
                integrity_status=excluded.integrity_status,
                file_size_bytes=excluded.file_size_bytes,
                obs_count=excluded.obs_count,
                error_message=excluded.error_message,
                metadata=excluded.metadata
        """, (task_run_id, obs_space_id, path, file_type, integrity, size, obs_count, error_msg, metadata_json))

    def update_file_status(self, file_id: int, status: str, error_msg: Optional[str]):
        """
        Updates just the status of a specific file.
        Used by the Validator (monitor_validate.py) after logic checks.
        """
        self._execute_log("""
            UPDATE file_inventory 
            SET integrity_status=?, error_message=? 
            WHERE id=?
        """, (status, error_msg, file_id))

    # ------------------------------------------------------------------
    # READ METHODS (Required for Scanner logic)
    # ------------------------------------------------------------------

    def get_all_properties(self) -> dict:
        """
        Returns {obs_space_id: {key: value}} for all known properties.
        Used by Scanner/Validator to load 'The Truth'.
        """
        data = {}
        rows = self._execute_log("SELECT obs_space_id, property_key, property_value FROM obs_space_properties").fetchall()
        for r in rows:
            sid, k, v = r
            if sid not in data: data[sid] = {}
            data[sid][k] = v
        return data

    def get_all_run_cycles(self):
        """
        Returns set of (run_type, date, cycle) to allow skipping existing scans.
        """
        cur = self._execute_log("SELECT DISTINCT run_type, date, cycle FROM task_runs")
        return set((r[0] if r[0] else 'unknown', r[1], r[2]) for r in cur.fetchall())
