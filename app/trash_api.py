from datetime import date
from typing import List
from xml.etree import ElementTree as ET

import pandas as pd
import requests
from sqlalchemy.exc import SQLAlchemyError

from db import SessionLocal, daily_trash_cache_table

async def get_trash_locations_from_api():
    url = "https://data.cityofnewyork.us/OData.svc/erm2-nwe9"
    params = {
        '$top': 10000,
        '$orderby': 'created_date desc',
        "$filter": "descriptor eq 'Trash'"
    }
    headers = {
        'Accept': 'application/xml'
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
                tag = field.tag.split('}', 1)[1]
                record[tag] = field.text
            records.append(record)
    df = pd.DataFrame(records)
    df = df[df.status != "Closed"]
    results = df.apply(lambda x: {'address': x.incident_address,
                                  'coordinates': {'longitude': x.longitude,
                                                  'latitude': x.latitude}},
                       axis=1).tolist()
    await cache_trash_locations(results)
    return results

async def cache_trash_locations(data: List[dict]):
    today = date.today().isoformat()
    session = SessionLocal()
    try:
        session.execute(
            daily_trash_cache_table.insert().values(date=today, data=str(data))
        )
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        print(f"Database error: {e}")
    finally:
        session.close()


async def get_cached_trash_locations() -> List[dict]:
    today = date.today().isoformat()
    session = SessionLocal()
    try:
        result = session.execute(daily_trash_cache_table.select().where(
            daily_trash_cache_table.c.date == today)).fetchone()
        return eval(result[1]) if result else None
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        return None
    finally:
        session.close()
