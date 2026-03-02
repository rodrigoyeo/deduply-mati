"""
Settings router — /api/settings/*
"""
from datetime import datetime

from fastapi import APIRouter, Request

from database import get_db, USE_POSTGRES

router = APIRouter()


@router.get("/api/settings/{key}")
def get_setting(key: str):
    """Get a setting value. API keys are masked for security."""
    conn = get_db()
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    if not row or not row[0]:
        return {"configured": False, "value": None}
    if "api_key" in key.lower():
        return {"configured": True, "value": "***configured***"}
    return {"configured": True, "value": row[0]}


@router.put("/api/settings/{key}")
async def update_setting(key: str, request: Request):
    """Update a setting value."""
    body = await request.json()
    value = body.get("value")
    conn = get_db()
    now = datetime.now().isoformat()
    if USE_POSTGRES:
        conn.execute("""
            INSERT INTO settings (key, value, updated_at) VALUES (%s, %s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
        """, (key, value, now))
    else:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, ?)",
            (key, value, now)
        )
    conn.commit()
    conn.close()
    return {"status": "ok", "key": key}
