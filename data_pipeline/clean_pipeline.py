import pandas as pd
import uuid
import hashlib
from rapidfuzz import fuzz
from geopy.geocoders import Nominatim
from datetime import datetime
import time

geolocator = Nominatim(user_agent="global-technician-daas")

SALT = "change_this_secret_salt"

SERVICE_MAP = {
    "ac repair": "HVAC",
    "air conditioner": "HVAC",
    "wireman": "Electrician",
    "electric work": "Electrician",
    "plumbing": "Plumber",
    "carpentry": "Carpenter"
}

def normalize_service(service):
    if not isinstance(service, str):
        return "General Maintenance"
    s = service.lower()
    for k, v in SERVICE_MAP.items():
        if k in s:
            return v
    return service.title()

def mask_phone(phone):
    if not isinstance(phone, str) or len(phone) < 6:
        return None
    return phone[:3] + "XXXX" + phone[-3:]

def hash_phone(phone):
    if not isinstance(phone, str):
        return None
    return hashlib.sha256((phone + SALT).encode()).hexdigest()

def geo_enrich(address):
    try:
        loc = geolocator.geocode(address, timeout=10)
        if loc:
            return loc.latitude, loc.longitude
    except:
        pass
    return None, None

def confidence_score(row):
    score = 0.0
    if row["latitude"] and row["longitude"]:
        score += 0.4
    if row["service_category"]:
        score += 0.2
    if row["skills"]:
        score += 0.2
    if row["contact_hash"]:
        score += 0.2
    return round(score, 2)

def clean_data(input_file, output_file):
    df = pd.read_csv(input_file)

    cleaned = []
    seen = []

    for _, r in df.iterrows():
        phone_hash = hash_phone(str(r.get("phone", "")))

        duplicate = False
        for s in seen:
            if s == phone_hash:
                duplicate = True
                break

        if duplicate:
            continue

        seen.append(phone_hash)

        address = f"{r.get('address','')}, {r.get('city','')}, {r.get('country','')}"
        lat, lng = geo_enrich(address)
        time.sleep(1)  # be polite to OSM

        record = {
            "technician_id": str(uuid.uuid4()),
            "full_name": r.get("name"),
            "service_category": normalize_service(r.get("service")),
            "skills": str(r.get("skills","")).split(","),
            "country_code": str(r.get("country","")).upper()[:2],
            "city": r.get("city"),
            "address": r.get("address"),
            "latitude": lat,
            "longitude": lng,
            "contact_masked": mask_phone(str(r.get("phone",""))),
            "contact_hash": phone_hash,
            "languages": ["en"],
            "availability": "ON_DEMAND",
            "experience_years": None,
            "source_type": "PUBLIC",
            "confidence_score": 0,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }

        record["confidence_score"] = confidence_score(record)
        cleaned.append(record)

    pd.DataFrame(cleaned).to_json(output_file, orient="records", indent=2)

if __name__ == "__main__":
    clean_data("raw_technicians.csv", "clean_technicians.json")
