from pydantic import BaseModel
from typing import List, Optional

class Technician(BaseModel):
    technician_id: str
    full_name: Optional[str]
    service_category: Optional[str]
    skills: Optional[List[str]]
    country_code: Optional[str]
    city: Optional[str]
    address: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    contact_masked: Optional[str]
    languages: Optional[List[str]]
    availability: Optional[str]
    experience_years: Optional[int]
    source_type: Optional[str]
    confidence_score: Optional[float]
    created_at: Optional[str]
    updated_at: Optional[str]
