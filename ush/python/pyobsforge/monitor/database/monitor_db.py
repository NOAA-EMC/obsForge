import sqlite3
import os
import json
import logging

logger = logging.getLogger("MonitorDB")

class MonitorDB:
    """
    The Database Writer.
    
    Responsibilities:
    1. Schema Management (Creation).
    2. Workflow Logging (Tasks, Runs).
    3. Inventory Management (Files, Categories).
    4. Gatekeeper Logic: Enforces 'Update-on-Change' to preserve data lineage.
    """
    
    def __init__(self, db_path):
        self.db_path = db_path
        # Ensure directory exists
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        
        # Enforce foreign keys for data integrity
        self.conn.execute("PRAGMA foreign_keys = ON")
        
        # Initialize Schema
        self._create_schema()

    def _create_schema(self):
        """Defines the database structure."""
        cur = self.conn.cursor()
        
        # --- SECTION 1: WORKFLOW ENTITIES ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                description TEXT
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS task_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                run_type TEXT,       -- e.g., 'gdas', 'gfs'
                date TEXT,           -- YYYYMMDD
                cycle INTEGER,       -- 0, 6, 12, 18
                job_id TEXT,         -- Scheduler ID
                status TEXT,         -- 'SUCCEEDED', 'FAILED'
                exit_code INTEGER,
                attempt INTEGER,
                host TEXT,
                logfile TEXT,
                start_time TEXT,
                end_time TEXT,
                runtime_sec REAL,
                FOREIGN KEY(task_id) REFERENCES tasks(id)
            )
        """)

        # --- SECTION 2: DATA ORGANIZATION ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS obs_space_categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS obs_spaces (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_id INTEGER,
                name TEXT UNIQUE NOT NULL,
                FOREIGN KEY(category_id) REFERENCES obs_space_categories(id)
            )
        """)

        # --- SECTION 3: FILE INVENTORY ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS file_inventory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_run_id INTEGER,
                obs_space_id INTEGER,
                file_path TEXT UNIQUE NOT NULL,
                integrity_status TEXT,          -- 'OK', 'CORRUPT', 'EMPTY', 'WARNING'
                obs_count INTEGER,
                file_size_bytes INTEGER,
                file_modified_time INTEGER,     -- Used for Change Detection
                error_message TEXT,             -- Stores inspector/scanner errors
                properties TEXT,                -- JSON blob for flexible metadata
                FOREIGN KEY(task_run_id) REFERENCES task_runs(id),
                FOREIGN KEY(obs_space_id) REFERENCES obs_spaces(id)
            )
        """)

        # --- SECTION 4: CONTENT & STATISTICS ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS variables (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                data_type TEXT,
                dimensionality INTEGER
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS obs_space_content (
                obs_space_id INTEGER,
                variable_id INTEGER,
                group_name TEXT,
                PRIMARY KEY (obs_space_id, variable_id, group_name)
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS file_variable_statistics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id INTEGER,
                variable_id INTEGER,
                min_val REAL,
                max_val REAL,
                mean_val REAL,
                std_dev REAL,
                FOREIGN KEY(file_id) REFERENCES file_inventory(id) ON DELETE CASCADE
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS file_domains (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id INTEGER UNIQUE,
                start_time INTEGER,
                end_time INTEGER,
                min_lat REAL, max_lat REAL,
                min_lon REAL, max_lon REAL,
                FOREIGN KEY(file_id) REFERENCES file_inventory(id) ON DELETE CASCADE
            )
        """)

        self.conn.commit()

    # ==========================================================================
    # REGISTRATION METHODS (Get or Create)
    # ==========================================================================

    def get_or_create_task(self, name):
        self.conn.execute("INSERT OR IGNORE INTO tasks (name) VALUES (?)", (name,))
        return self.conn.execute("SELECT id FROM tasks WHERE name=?", (name,)).fetchone()[0]

    def get_or_create_category(self, name):
        self.conn.execute("INSERT OR IGNORE INTO obs_space_categories (name) VALUES (?)", (name,))
        return self.conn.execute("SELECT id FROM obs_space_categories WHERE name=?", (name,)).fetchone()[0]

    def get_or_create_obs_space(self, name, cat_id):
        self.conn.execute("INSERT OR IGNORE INTO obs_spaces (name, category_id) VALUES (?, ?)", (name, cat_id))
        return self.conn.execute("SELECT id FROM obs_spaces WHERE name=?", (name,)).fetchone()[0]

    def get_or_create_variable(self, name, dtype, ndim):
        self.conn.execute(
            "INSERT OR IGNORE INTO variables (name, data_type, dimensionality) VALUES (?, ?, ?)", 
            (name, dtype, ndim)
        )
        return self.conn.execute("SELECT id FROM variables WHERE name=?", (name,)).fetchone()[0]

    # ==========================================================================
    # LOGGING METHODS
    # ==========================================================================

    def log_task_run(self, task_id, date, cycle, run_type, job_id, status, exit_code, attempt, host, logfile, start_time, end_time, runtime_sec):
        """
        Logs a workflow execution. Updates if the (task, date, cycle, run) tuple exists.
        Task Runs are always updated to reflect the 'latest attempt'.
        """
        cur = self.conn.execute(
            "SELECT id FROM task_runs WHERE task_id=? AND date=? AND cycle=? AND run_type=?", 
            (task_id, date, cycle, run_type)
        )
        existing = cur.fetchone()
        
        if existing:
            rid = existing[0]
            self.conn.execute("""
                UPDATE task_runs SET 
                    job_id=?, status=?, exit_code=?, attempt=?, host=?, 
                    logfile=?, start_time=?, end_time=?, runtime_sec=?
                WHERE id=?
            """, (job_id, status, exit_code, attempt, host, logfile, start_time, end_time, runtime_sec, rid))
            return rid, 'updated'
        else:
            cur = self.conn.execute("""
                INSERT INTO task_runs 
                (task_id, date, cycle, run_type, job_id, status, exit_code, attempt, host, logfile, start_time, end_time, runtime_sec)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (task_id, date, cycle, run_type, job_id, status, exit_code, attempt, host, logfile, start_time, end_time, runtime_sec))
            return cur.lastrowid, 'inserted'

    def log_file_inventory(self, task_run_id, obs_space_id, path, integrity, size, mtime, obs_count, error_msg, properties):
        """
        Logs a file.
        GATEKEEPER LOGIC:
        - If file exists and mtime matches DB -> RETURN None (Ignore update).
        - If file exists and mtime is newer -> UPDATE record (New lineage, new stats).
        - If file is new -> INSERT record.
        """
        props_json = json.dumps(properties) if properties else None
        
        cur = self.conn.execute("SELECT id, file_modified_time FROM file_inventory WHERE file_path=?", (path,))
        existing = cur.fetchone()
        
        if existing:
            fid = existing[0]
            db_mtime = existing[1]
            
            # Check Change: Disk mtime vs DB mtime
            if mtime > db_mtime:
                # File Regenerated: This is a new artifact. Update Everything.
                self.conn.execute("""
                    UPDATE file_inventory SET 
                        task_run_id=?, obs_space_id=?, integrity_status=?, 
                        obs_count=?, file_size_bytes=?, file_modified_time=?, 
                        error_message=?, properties=?
                    WHERE id=?
                """, (task_run_id, obs_space_id, integrity, obs_count, size, mtime, error_msg, props_json, fid))
                return fid
            else:
                # File Unchanged: This is an old artifact.
                # Do NOT update task_run_id (preserve link to creator task).
                # Do NOT update stats (preserve scan results).
                # Return None to signal 'Skipped'.
                return None
        else:
            # New File: Insert fresh
            cur = self.conn.execute("""
                INSERT INTO file_inventory 
                (task_run_id, obs_space_id, file_path, integrity_status, obs_count, file_size_bytes, file_modified_time, error_message, properties)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (task_run_id, obs_space_id, path, integrity, obs_count, size, mtime, error_msg, props_json))
            return cur.lastrowid

    def register_file_schema(self, obs_space_id, schema_dict):
        """Registers variables found in the file."""
        for path, meta in schema_dict.items():
            parts = path.split('/')
            if len(parts) > 1:
                group = parts[0]
                var_name = parts[1]
            else:
                group = 'root'
                var_name = parts[0]
            
            var_id = self.get_or_create_variable(var_name, meta['type'], meta['ndim'])
            
            self.conn.execute("""
                INSERT OR IGNORE INTO obs_space_content (obs_space_id, variable_id, group_name)
                VALUES (?, ?, ?)
            """, (obs_space_id, var_id, group))

    def log_variable_statistics(self, file_id, stats_list):
        """Logs Min/Max/Mean/Std. Clears old stats first."""
        self.conn.execute("DELETE FROM file_variable_statistics WHERE file_id=?", (file_id,))
        
        for s in stats_list:
            v_name = s['name'].split('/')[-1]
            cur = self.conn.execute("SELECT id FROM variables WHERE name=?", (v_name,))
            row = cur.fetchone()
            
            if row:
                vid = row[0]
                self.conn.execute("""
                    INSERT INTO file_variable_statistics 
                    (file_id, variable_id, min_val, max_val, mean_val, std_dev)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (file_id, vid, s['min'], s['max'], s['mean'], s['std']))

    def log_file_domain(self, file_id, start, end, min_lat, max_lat, min_lon, max_lon):
        """Logs Lat/Lon/Time bounds. Clears old bounds first."""
        self.conn.execute("DELETE FROM file_domains WHERE file_id=?", (file_id,))
        
        self.conn.execute("""
            INSERT INTO file_domains 
            (file_id, start_time, end_time, min_lat, max_lat, min_lon, max_lon)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (file_id, start, end, min_lat, max_lat, min_lon, max_lon))

    def update_file_status(self, file_id, status, error_msg):
        """Used by Inspector to flag files without changing lineage."""
        self.conn.execute("""
            UPDATE file_inventory 
            SET integrity_status = ?, error_message = ? 
            WHERE id = ?
        """, (status, error_msg, file_id))

    def commit(self):
        self.conn.commit()
    
    def close(self):
        self.conn.close()
