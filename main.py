import os
from typing import List
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, func, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry, WKTElement
import httpx
from redis.asyncio import Redis
import json
from config import get_settings
from sqlalchemy.engine.url import make_url


# Environment variables (DATABASE_URL should NOT contain hardcoded passwords in the code)
DATABASE_URL = get_settings().DATABASE_URL
REDIS_URL = get_settings().REDIS_URL
CA_CERT_PATH = os.path.join(os.getcwd(), "ca.pem")  # Path to the CA certificate

raw_url = get_settings().DATABASE_URL

# Normalize Aiven or other "postgres://" URLs
url = make_url(raw_url)
if url.drivername == "postgres":
    url = url.set(drivername="postgresql+psycopg2")

engine = create_engine(
    url,
    connect_args={"sslmode": "require", "sslrootcert": CA_CERT_PATH},
    echo=False
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Ensure PostGIS extension is enabled
with engine.connect() as conn:
    conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
    conn.commit()

class Property(Base):
    __tablename__ = 'properties'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    address = Column(String)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    location = Column(Geometry('POINT', srid=4326), nullable=False)

# Create tables if they don't exist
Base.metadata.create_all(bind=engine)

# Pydantic schemas
class PropertyCreate(BaseModel):
    name: str
    address: str
    latitude: float
    longitude: float

class PropertyOut(BaseModel):
    id: int
    name: str
    address: str
    latitude: float
    longitude: float

    class Config:
        orm_mode = True

# FastAPI app and Redis client
app = FastAPI()
redis = Redis.from_url(REDIS_URL, decode_responses=True)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

async def geocode_location(query: str):
    key = f'geocode:{query.lower()}'
    cached = await redis.get(key)
    if cached:
        lat, lon = json.loads(cached)
        return lat, lon

    url = 'https://geocode.maps.co/search'
    params = {'q': query}
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, params=params, timeout=1.0)
    results = resp.json()
    if not results or not isinstance(results, list):
        raise HTTPException(status_code=400, detail='Location not found')

    first = results[0]
    lat, lon = float(first['lat']), float(first['lon'])

    await redis.set(key, json.dumps([lat, lon]), ex=86400)
    return lat, lon

def get_nearby_properties(db, lat: float, lon: float, radius_km: float = 50.0):
    point = func.ST_MakePoint(lon, lat)
    dist_expr = func.ST_DistanceSphere(Property.location, point)

    query = (
        db.query(
            Property.id,
            Property.name,
            Property.address,
            Property.latitude,
            Property.longitude,
            (dist_expr / 1000).label('distance_km')
        )
        .filter(dist_expr <= radius_km * 1000)
        .order_by(dist_expr)
    )

    return [
        {
            'id': id,
            'name': name,
            'address': address,
            'latitude': latitude,
            'longitude': longitude,
            'distance_km': round(distance, 2)
        }
        for id, name, address, latitude, longitude, distance in query.all()
    ]

@app.post('/properties', response_model=PropertyOut, status_code=201)
def create_property(prop: PropertyCreate, db=Depends(get_db)):
    point = WKTElement(f'POINT({prop.longitude} {prop.latitude})', srid=4326)
    db_prop = Property(
        name=prop.name,
        address=prop.address,
        latitude=prop.latitude,
        longitude=prop.longitude,
        location=point
    )
    db.add(db_prop)
    db.commit()
    db.refresh(db_prop)
    return db_prop

@app.get('/properties', response_model=List[PropertyOut])
def list_properties(db=Depends(get_db)):
    return db.query(Property).all()

@app.get('/properties/{property_id}', response_model=PropertyOut)
def get_property(property_id: int, db=Depends(get_db)):
    prop = db.query(Property).filter(Property.id == property_id).first()
    if not prop:
        raise HTTPException(status_code=404, detail='Property not found')
    return prop

@app.put('/properties/{property_id}', response_model=PropertyOut)
def update_property(property_id: int, prop_in: PropertyCreate, db=Depends(get_db)):
    db_prop = db.query(Property).filter(Property.id == property_id).first()
    if not db_prop:
        raise HTTPException(status_code=404, detail='Property not found')
    db_prop.name = prop_in.name
    db_prop.address = prop_in.address
    db_prop.latitude = prop_in.latitude
    db_prop.longitude = prop_in.longitude
    db_prop.location = WKTElement(f'POINT({prop_in.longitude} {prop_in.latitude})', srid=4326)
    db.commit()
    db.refresh(db_prop)
    return db_prop

@app.delete('/properties/{property_id}', status_code=204)
def delete_property(property_id: int, db=Depends(get_db)):
    db_prop = db.query(Property).filter(Property.id == property_id).first()
    if not db_prop:
        raise HTTPException(status_code=404, detail='Property not found')
    db.delete(db_prop)
    db.commit()

@app.get('/properties/get/nearby')
async def nearby(location: str, db=Depends(get_db)):
    lat, lon = await geocode_location(location)
    props = get_nearby_properties(db, lat, lon)
    return {'properties': props}