import logging
import requests
from typing import Optional

from my_proof.models.google import GoogleUserInfo
from my_proof.config import settings

def get_google_user() -> Optional[GoogleUserInfo]:
    """
    Get Google user information using the OAuth token.
    
    Returns:
        Optional[GoogleUserInfo]: User information if successful, None if failed
    """
    try:
        if not settings.GOOGLE_TOKEN:
            raise ValueError("GOOGLE_TOKEN is not set in environment")
            
        response = requests.get(
            "https://www.googleapis.com/oauth2/v1/userinfo",
            params={"alt": "json"},
            headers={"Authorization": f"Bearer {settings.GOOGLE_TOKEN}"}
        )
        
        response.raise_for_status()
        user_data = response.json()
        
        return GoogleUserInfo(**user_data)
        
    except Exception as e:
        logging.error(f"Failed to get Google user info: {str(e)}")
        return None
