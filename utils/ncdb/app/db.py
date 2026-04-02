import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Global placeholders
engine = None
SessionLocal = sessionmaker(autoflush=False, autocommit=False)
DB_PATH = ""

# Auto-initialize if the launcher set the environment variable
env_db_path = os.getenv("NCDB_DB_PATH")

if env_db_path:
    DB_PATH = env_db_path
    engine = create_engine(f"sqlite:///{DB_PATH}")
    SessionLocal.configure(bind=engine)
    # Optional: print for visibility in the Uvicorn logs
    print(f"Worker: Connected to Database at {DB_PATH}")

# def init_db(path: str):
    # """Fallback for direct scripts/scanners."""
    # global engine, DB_PATH
    # DB_PATH = path
    # engine = create_engine(f"sqlite:///{path}")
    # SessionLocal.configure(bind=engine)
