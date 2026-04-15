from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from ds.db_base import Base

db_path = "/scratch3/NCEPDEV/da/Edward.Givelberg/monitoring/diags.db"
engine = create_engine(f"sqlite:///{db_path}")
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

Base.metadata.create_all(engine)

session = SessionLocal()
