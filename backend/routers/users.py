"""
Users / Auth router — /api/auth/*, /api/users/*
"""
import sqlite3

from fastapi import APIRouter, HTTPException, Depends

from database import get_db
from models import UserLogin, UserCreate, ChangePassword
from shared import get_current_user, hash_password, verify_password
import secrets

router = APIRouter()


@router.get("/api/health")
def health_check():
    try:
        conn = get_db()
        conn.execute("SELECT 1").fetchone()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}


@router.post("/api/auth/login")
def login(creds: UserLogin):
    conn = get_db()
    user = conn.execute(
        "SELECT * FROM users WHERE email=? AND is_active=1", (creds.email,)
    ).fetchone()
    conn.close()
    if not user or not verify_password(creds.password, user['password_hash']):
        raise HTTPException(401, "Invalid credentials")
    return {
        "token": user['api_token'],
        "user": {
            "id": user['id'],
            "email": user['email'],
            "name": user['name'],
            "role": user['role']
        }
    }


@router.get("/api/auth/me")
def get_me(user: dict = Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "Not authenticated")
    return {
        "id": user['id'],
        "email": user['email'],
        "name": user['name'],
        "role": user['role']
    }


@router.post("/api/auth/register")
def register(user: UserCreate):
    conn = get_db()
    try:
        token = secrets.token_urlsafe(32)
        pwd_hash = hash_password(user.password)  # Use bcrypt
        conn.execute(
            "INSERT INTO users (email, password_hash, name, role, api_token) VALUES (?, ?, ?, ?, ?)",
            (user.email, pwd_hash, user.name, user.role, token)
        )
        conn.commit()
        return {"message": "Created", "token": token}
    except sqlite3.IntegrityError:
        raise HTTPException(400, "Email exists")
    finally:
        conn.close()


@router.post("/api/auth/change-password")
def change_password(data: ChangePassword, user: dict = Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "Not authenticated")

    conn = get_db()
    db_user = conn.execute("SELECT * FROM users WHERE id=?", (user['id'],)).fetchone()

    if not db_user or not verify_password(data.current_password, db_user['password_hash']):
        conn.close()
        raise HTTPException(400, "Current password is incorrect")

    if len(data.new_password) < 6:
        conn.close()
        raise HTTPException(400, "New password must be at least 6 characters")

    new_hash = hash_password(data.new_password)
    conn.execute("UPDATE users SET password_hash=? WHERE id=?", (new_hash, user['id']))
    conn.commit()
    conn.close()
    return {"message": "Password changed successfully"}


@router.get("/api/users")
def get_users(user: dict = Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "Not authenticated")
    conn = get_db()
    users = conn.execute(
        "SELECT id, email, name, role, is_active, created_at FROM users ORDER BY created_at DESC"
    ).fetchall()
    conn.close()
    return {"data": [dict(u) for u in users]}


@router.delete("/api/users/{user_id}")
def delete_user(user_id: int, user: dict = Depends(get_current_user)):
    if not user:
        raise HTTPException(401, "Not authenticated")
    if user.get("role") != "admin":
        raise HTTPException(403, "Admin only")
    conn = get_db()
    conn.execute("UPDATE users SET is_active=0 WHERE id=?", (user_id,))
    conn.commit()
    conn.close()
    return {"message": "Deactivated"}


@router.get("/health")
def health():
    return {"status": "ok", "version": "5.2"}


@router.get("/api/info")
def info():
    return {"name": "Deduply", "version": "5.2"}
