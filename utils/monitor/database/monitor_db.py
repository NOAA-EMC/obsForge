import sqlite3
import os
import json
import logging
from .schema import MonitorSchema

logger = logging.getLogger("MonitorDB")

class MonitorDB:
    """
    The High-Level Interface for Database Operations.
    Handles connection management, entity registration, and inventory logging.
    Enforces 'Update-on-Change' logic for file tracking.
    """
    
    def __init__(self, db_path):
        self.db_path = db_path
        
        # Ensure the directory exists
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            
        # Connect to SQLite
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        
        # Enforce foreign key constraints
        self.conn.execute("PRAGMA foreign_keys = ON")
        
        # Initialize the Schema (create tables if missing)
        MonitorSchema(self.conn)

    # ==========================================================================
    # 1. ENTITY REGISTRATION (Get or Create)
    # ==========================================================================

    def get_or_create_task(self, name):
        """Registers a workflow task."""
        self.conn.execute(
            "INSERT OR IGNORE INTO tasks (name) VALUES (?)", 
            (name,)
        )
        return self.conn.execute(
            "SELECT id FROM tasks WHERE name=?", 
            (name,)
        ).fetchone()[0]

    def get_or_create_category(self, name):
        """Registers a category (e.g., 'Marine')."""
        self.conn.execute(
            "INSERT OR IGNORE INTO obs_space_categories (name) VALUES (?)", 
            (name,)
        )
        return self.conn.execute(
            "SELECT id FROM obs_space_categories WHERE name=?", 
            (name,)
        ).fetchone()[0]

    def get_or_create_obs_space(self, name, cat_id):
        """Registers an Obs Space."""
        self.conn.execute(
            "INSERT OR IGNORE INTO obs_spaces (name, category_id) VALUES (?, ?)", 
            (name, cat_id)
        )
        return self.conn.execute(
            "SELECT id FROM obs_spaces WHERE name=?", 
            (name,)
        ).fetchone()[0]

    def get_or_create_variable(self, name, dtype="float", ndim=1):
        """Registers a standard variable."""
        self.conn.execute(
            """
            INSERT OR IGNORE INTO variables (name, data_type, dimensionality) 
            VALUES (?, ?, ?)
            """, 
            (name, dtype, ndim)
        )
        return self.conn.execute(
            "SELECT id FROM variables WHERE name=?", 
            (name,)
        ).fetchone()[0]

    # ==========================================================================
    # 2. LOGGING & INVENTORY
    # ==========================================================================

    def log_task_run(self, task_id, date, cycle, run_type, job_id, status, 
                     exit_code, attempt, host, logfile, start_time, end_time, runtime_sec):
        """Logs a specific execution of a task."""
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
            """, (job_id, status, exit_code, attempt, host, logfile, 
                  start_time, end_time, runtime_sec, rid))
            return rid, 'updated'
        else:
            cur = self.conn.execute("""
                INSERT INTO task_runs 
                (task_id, date, cycle, run_type, job_id, status, exit_code, 
                 attempt, host, logfile, start_time, end_time, runtime_sec)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (task_id, date, cycle, run_type, job_id, status, exit_code, 
                  attempt, host, logfile, start_time, end_time, runtime_sec))
            return cur.lastrowid, 'inserted'

    def log_file_inventory(self, task_run_id, obs_space_id, path, integrity, 
                           size, mtime, obs_count, error_msg, properties):
        """
        Logs a file entry into the inventory.
        Enforces "Update-on-Change" logic.
        """
        props_json = json.dumps(properties) if properties else None
        
        cur = self.conn.execute(
            "SELECT id, file_modified_time FROM file_inventory WHERE file_path=?", 
            (path,)
        )
        existing = cur.fetchone()
        
        if existing:
            fid = existing[0]
            db_mtime = existing[1]
            
            if mtime > db_mtime:
                # File Changed: Update Everything
                self.conn.execute("""
                    UPDATE file_inventory SET 
                        task_run_id=?, obs_space_id=?, integrity_status=?, 
                        obs_count=?, file_size_bytes=?, file_modified_time=?, 
                        error_message=?, properties=?
                    WHERE id=?
                """, (task_run_id, obs_space_id, integrity, obs_count, size, 
                      mtime, error_msg, props_json, fid))
                return fid
            else:
                # File Unchanged: Skip
                return None
        else:
            # New File: Insert
            cur = self.conn.execute("""
                INSERT INTO file_inventory 
                (task_run_id, obs_space_id, file_path, integrity_status, 
                 obs_count, file_size_bytes, file_modified_time, error_message, properties)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (task_run_id, obs_space_id, path, integrity, obs_count, 
                  size, mtime, error_msg, props_json))
            return cur.lastrowid

    # ==========================================================================
    # 3. STATS & METADATA
    # ==========================================================================

    def register_file_schema(self, obs_space_id, schema_dict):
        """Registers the variables found inside the file."""
        for path, meta in schema_dict.items():
            parts = path.split('/')
            
            if len(parts) > 1:
                group, var_name = parts[0], parts[1]
            else:
                group, var_name = 'root', parts[0]
            
            var_id = self.get_or_create_variable(var_name, meta['type'], meta['ndim'])
            
            self.conn.execute("""
                INSERT OR IGNORE INTO obs_space_content (obs_space_id, variable_id, group_name) 
                VALUES (?, ?, ?)
            """, (obs_space_id, var_id, group))

    def log_variable_statistics(self, file_id, stats_list):
        """Logs Min/Max/Mean/Std for variables. Clears old stats first."""
        self.conn.execute("DELETE FROM file_variable_statistics WHERE file_id=?", (file_id,))
        
        for s in stats_list:
            v_name = s['name'].split('/')[-1]
            
            # Resolve variable ID
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
        """Logs Lat/Lon/Time bounds."""
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
