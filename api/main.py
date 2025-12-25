from fastapi import FastAPI, Query, Depends, HTTPException, Header
from typing import List
from api.db import supabase
from api.models import Technician

# ----- API Key Verification -----
def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != "YOUR_API_KEY_HERE":  # replace with your actual key
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return True

# ----- FastAPI App -----
app = FastAPI(title="Global Technician DaaS API", version="1.0")

@app.get("/")
def root():
    return {"message": "FastAPI is working!"}

@app.get("/technicians", response_model=List[Technician])
def get_technicians(
    country_code: str = Query(None, max_length=2),
    city: str = Query(None),
    service_category: str = Query(None),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0),
    limit: int = Query(50, le=500),
    offset: int = Query(0),
    api_key: bool = Depends(verify_api_key)
):
    query = supabase.table("technicians").select("*")

    if country_code:
        query = query.eq("country_code", country_code.upper())
    if city:
        query = query.ilike("city", f"%{city}%")
    if service_category:
        query = query.ilike("service_category", f"%{service_category}%")
    if min_confidence > 0:
        query = query.gte("confidence_score", min_confidence)

    query = query.range(offset, offset + limit - 1)
    response = query.execute()
    return response.data
