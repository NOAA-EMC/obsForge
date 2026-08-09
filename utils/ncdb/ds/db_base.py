# db_base.py
"""
Shared SQLAlchemy declarative base.

All ORM classes should import Base from here.
"""

from sqlalchemy.orm import declarative_base

Base = declarative_base()
