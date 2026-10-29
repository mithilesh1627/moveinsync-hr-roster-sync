"""Database Initialization & Migration Verification Script"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.config import settings
from app.core.database import sync_engine
from app.models import Base


def init_database():
    print(f"[{time.strftime('%X')}] Initializing database tables...")
    print(f"Connecting to: {settings.database_sync_url}")
    
    with sync_engine.connect() as conn:
        print(f"[{time.strftime('%X')}] Connection established successfully.")
    
    print(f"[{time.strftime('%X')}] Calling Base.metadata.create_all...")
    Base.metadata.create_all(bind=sync_engine)
    print(f"[{time.strftime('%X')}] Tables created successfully!")
    
    # Verify tables exist
    from sqlalchemy import inspect
    inspector = inspect(sync_engine)
    tables = inspector.get_table_names()
    print(f"[{time.strftime('%X')}] Tables present in PostgreSQL: {tables}")
    return tables


if __name__ == "__main__":
    init_database()
