import hashlib
from sqlalchemy import Table, Column, String
from sqlalchemy.exc import SQLAlchemyError
from geopy.geocoders import Nominatim

from db import metadata, SessionLocal  # Import from db.py

geolocator = Nominatim(user_agent="geolocator_app", timeout=10)

cache_table = Table(
    'cache', metadata,
    Column('address_hash', String, primary_key=True),
    Column('coordinates', String)
)

async def get_coordinates_from_package(address: str):
    location = geolocator.geocode(address)
    if location:
        return {'longitude': location.longitude, 'latitude': location.latitude}
    else:
        return None

async def get_cached_coordinates(address: str):
    address_hash = hashlib.sha256(address.encode()).hexdigest()
    session = SessionLocal()
    try:
        result = session.execute(cache_table.select().where(
            cache_table.c.address_hash == address_hash)).fetchone()
        return result[1] if result else None
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        return None
    finally:
        session.close()

async def cache_coordinates(address: str, coordinates: str):
    address_hash = hashlib.sha256(address.encode()).hexdigest()
    session = SessionLocal()
    try:
        session.execute(
            cache_table.insert().values(address_hash=address_hash, coordinates=coordinates)
        )
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        print(f"Database error: {e}")
    finally:
        session.close()
