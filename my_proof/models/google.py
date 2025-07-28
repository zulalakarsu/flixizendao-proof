from pydantic import BaseModel

class GoogleUserInfo(BaseModel):
    """Google OAuth2 user info response"""
    id: str
    email: str
    verified_email: bool
    name: str
    given_name: str
    family_name: str
    picture: str 