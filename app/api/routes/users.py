import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select, func

from app.core.security import get_current_user, get_password_hash, verify_password
from app.db.postgres import get_session
from app.models.user import User, user_to_response

router = APIRouter(prefix="/api/users", tags=["users"])


class CreateUserRequest(BaseModel):
    username: str
    password: str


class UpdateUserRequest(BaseModel):
    username: Optional[str] = None
    is_active: Optional[bool] = None


class ChangePasswordRequest(BaseModel):
    new_password: str


@router.get("")
async def list_users(_: str = Depends(get_current_user)):
    async with get_session() as session:
        result = await session.execute(select(User).order_by(User.created_at.asc()))
        users = result.scalars().all()
    return [user_to_response(u) for u in users]


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_user(body: CreateUserRequest, _: str = Depends(get_current_user)):
    if len(body.password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters.")

    async with get_session() as session:
        existing = await session.execute(select(User).where(User.username == body.username))
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=409, detail="Username already exists.")

    async with get_session() as session:
        async with session.begin():
            user = User(
                id=uuid.uuid4(),
                username=body.username,
                hashed_password=get_password_hash(body.password),
                is_active=True,
            )
            session.add(user)
            await session.flush()
            return user_to_response(user)


@router.put("/{user_id}")
async def update_user(user_id: str, body: UpdateUserRequest, _: str = Depends(get_current_user)):
    try:
        uid = uuid.UUID(user_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid user ID.")

    async with get_session() as session:
        async with session.begin():
            user = await session.get(User, uid)
            if not user:
                raise HTTPException(status_code=404, detail="User not found.")

            # Prevent deactivating the last active user
            if body.is_active is False and user.is_active:
                count = await session.scalar(
                    select(func.count()).select_from(User).where(User.is_active == True)
                )
                if count <= 1:
                    raise HTTPException(status_code=400, detail="Cannot deactivate the last active user.")

            if body.username is not None:
                existing = await session.execute(
                    select(User).where(User.username == body.username, User.id != uid)
                )
                if existing.scalar_one_or_none():
                    raise HTTPException(status_code=409, detail="Username already exists.")
                user.username = body.username

            if body.is_active is not None:
                user.is_active = body.is_active

            return user_to_response(user)


@router.put("/{user_id}/password")
async def change_password(user_id: str, body: ChangePasswordRequest, _: str = Depends(get_current_user)):
    try:
        uid = uuid.UUID(user_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid user ID.")

    if len(body.new_password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters.")

    async with get_session() as session:
        async with session.begin():
            user = await session.get(User, uid)
            if not user:
                raise HTTPException(status_code=404, detail="User not found.")
            user.hashed_password = get_password_hash(body.new_password)

    return {"message": "Password updated successfully."}


@router.delete("/{user_id}")
async def delete_user(user_id: str, _: str = Depends(get_current_user)):
    try:
        uid = uuid.UUID(user_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid user ID.")

    async with get_session() as session:
        async with session.begin():
            user = await session.get(User, uid)
            if not user:
                raise HTTPException(status_code=404, detail="User not found.")

            # Prevent deleting the last active user
            if user.is_active:
                count = await session.scalar(
                    select(func.count()).select_from(User).where(User.is_active == True)
                )
                if count <= 1:
                    raise HTTPException(status_code=400, detail="Cannot delete the last active user.")

            await session.delete(user)

    return {"message": "User deleted."}
