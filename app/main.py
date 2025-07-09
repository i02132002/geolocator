from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from geopy.geocoders import Nominatim
from typing import List
import asyncio
import os
from sqlalchemy import create_engine, Column, String, Table, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import hashlib
import uvicorn
from dotenv import load_dotenv
from config import Settings
from fastapi.middleware.cors import CORSMiddleware
import requests
import xml.etree.ElementTree as ET
import pandas as pd

load_dotenv()

app = FastAPI()
settings = Settings()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

class AddressList(BaseModel):
    addresses: List[str]

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
metadata = MetaData()

cache_table = Table(
    'cache', metadata,
    Column('address_hash', String, primary_key=True),
    Column('coordinates', String)
)

metadata.create_all(engine)

geolocator = Nominatim(user_agent="geolocator_app", timeout=10)
async def get_cached_coordinates(address: str):
    address_hash = hashlib.sha256(address.encode()).hexdigest()
    session = SessionLocal()
    try:
        result = session.execute(cache_table.select().where(cache_table.c.address_hash == address_hash)).fetchone()
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

async def get_coordinates_from_package(address: str):
    location = geolocator.geocode(address)
    if location:
        return {'longitude': location.longitude, 'latitude': location.latitude}
    else:
        return None

@app.post("/get_coords/")
async def get_coords(address_list: AddressList):
    sem = asyncio.Semaphore(5)

    async def sem_task(address):
        async with sem:
            cached_result = await get_cached_coordinates(address)
            if cached_result == "Coordinates not found":
                return None
            if cached_result:
                return eval(cached_result)
            coords = await get_coordinates_from_package(address)
            await cache_coordinates(address, str(coords) if coords else "Coordinates not found")
            return coords
    tasks = [sem_task(address) for address in address_list.addresses]
    results = await asyncio.gather(*tasks)
    return [{'address': addr, 'coordinates': coord} if coord else {'address': addr, 'error': 'Coordinates not found'} for addr, coord in zip(address_list.addresses, results)]

@app.get("/get_trash_locations/")
async def get_trash_locations():
    url = "https://data.cityofnewyork.us/OData.svc/erm2-nwe9"
    params = {
        '$top': 10000,
        '$orderby': 'created_date desc',
        "$filter": "descriptor eq 'Trash'"
    }
    headers = {
        'Accept': 'application/xml'  # Ask for XML
    }

    response = requests.get(url, params=params, headers=headers)
    xml_data = response.text

    ns = {
        'atom': 'http://www.w3.org/2005/Atom',
        'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices',
        'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata'
    }

    root = ET.fromstring(xml_data)

    records = []
    for entry in root.findall('atom:entry', ns):
        props = entry.find('atom:content/m:properties', ns)
        if props is not None:
            record = {}
            for field in props:
                # Strip the namespace from the tag
                tag = field.tag.split('}', 1)[1]
                record[tag] = field.text
            records.append(record)


    # Filter for status open
    df = pd.DataFrame(records)
    df = df[df.status != "Closed"]


    results = df.apply(lambda x: {'address': x.incident_address, 'coordinates': {'longitude': x.longitude, 'latitude': x.latitude}}, axis=1).tolist()
    return results

if __name__ == "__main__":
    uvicorn.run(app, host=settings.HOST_URL, port=settings.HOST_PORT)
