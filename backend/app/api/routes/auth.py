from fastapi import APIRouter, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from fastapi import Depends
from pydantic import BaseModel

from app.core.security import create_access_token, verify_password, get_password_hash
from app.core.config import settings

router = APIRouter(prefix="/api/auth", tags=["auth"])


class Token(BaseModel):
    access_token: str
    token_type: str


@router.post("/login", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """Authenticate with username/password, return JWT."""
    # Single-user auth from environment variables
    if form_data.username != settings.dashboard_username:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")

    # Compare against hashed or plain password from env
    stored_pw = settings.dashboard_password
    # Accept plain text comparison for simplicity (or hashed if starts with $2b$)
    if stored_pw.startswith("$2b$"):
        valid = verify_password(form_data.password, stored_pw)
    else:
        valid = form_data.password == stored_pw

    if not valid:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")

    token = create_access_token({"sub": form_data.username})
    return {"access_token": token, "token_type": "bearer"}
