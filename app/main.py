from fastapi import FastAPI
from pydantic import BaseModel

from typing import List
import asyncio
import uvicorn

from app.geolocator_api import get_coordinates_from_package, \
    get_cached_coordinates, cache_coordinates
from app.trash_api import get_trash_locations_from_api, \
    get_cached_trash_locations
from config import Settings
from fastapi.middleware.cors import CORSMiddleware



app = FastAPI()
settings = Settings()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class AddressList(BaseModel):
    addresses: List[str]

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
    cached_data = await get_cached_trash_locations()
    if cached_data:
        return cached_data
    results = await get_trash_locations_from_api()
    return results


if __name__ == "__main__":
    uvicorn.run(app, host=settings.HOST_URL, port=settings.HOST_PORT)
