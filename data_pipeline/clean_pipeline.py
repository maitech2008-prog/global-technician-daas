import pandas as pd
import uuid
import hashlib
from geopy.geocoders import Nominatim
from datetime import datetime
import time

# ---------- CONFIG ----------
SALT = "CHANGE_THIS_SECRET"
geolocator = Nominatim(user_agent="global-technician-daas")

SERVICE_MAP = {
    "ac repair": "HVAC",
    "air conditioner": "HVAC",
    "electric": "Electrician",
    "wireman": "Electrician",
    "plumb": "Plumber",
    "carpenter": "Carpenter"
}

# ---------- HELPERS ----------
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

def calculate_confidence(record):
    score = 0.0
    if record["latitude"] and record["longitude"]:
        score += 0.4
    if record["service_category"]:
        score += 0.2
    if record["skills"]:
        score += 0.2
    if record["contact_hash"]:
        score += 0.2
    return round(score, 2)

# ---------- MAIN PIPELINE ----------
def run_pipeline(input_csv, output_json):
    df = pd.read_csv(input_csv)
    results = []
    seen_hashes = set()

    for _, r in df.iterrows():
        phone = str(r.get("phone", ""))
        phone_hash = hash_phone(phone)

        if phone_hash in seen_hashes:
            continue
        seen_hashes.add(phone_hash)

        address_full = f"{r.get('address','')}, {r.get('city','')}, {r.get('country','')}"
        lat, lng = geo_enrich(address_full)
        time.sleep(1)

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
            "contact_masked": mask_phone(phone),
            "contact_hash": phone_hash,
            "languages": ["en"],
            "availability": "ON_DEMAND",
            "experience_years": None,
            "source_type": "PUBLIC",
            "confidence_score": 0,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }

        record["confidence_score"] = calculate_confidence(record)
        results.append(record)

    pd.DataFrame(results).to_json(output_json, orient="records", indent=2)

if __name__ == "__main__":
    run_pipeline("raw_technicians.csv", "clean_technicians.json")
