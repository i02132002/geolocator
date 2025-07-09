from sqlalchemy import create_engine, MetaData, Table, Column, String, inspect
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
metadata = MetaData()

def get_or_create_daily_trash_cache_table():
    inspector = inspect(engine)
    if inspector.has_table("daily_trash_cache"):
        daily_trash_cache_table = Table("daily_trash_cache", metadata, autoload_with=engine)
    else:
        daily_trash_cache_table = Table(
            'daily_trash_cache', metadata,
            Column('date', String, primary_key=True),
            Column('data', String)
        )
        metadata.create_all(engine)
    return daily_trash_cache_table

daily_trash_cache_table = get_or_create_daily_trash_cache_table()
