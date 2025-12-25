from fastapi import Header, HTTPException

API_KEYS = ["FREE_API_KEY_12345"]  # Replace with real keys

def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key not in API_KEYS:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return True
