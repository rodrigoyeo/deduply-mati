"""
Email verification router — /api/verify/*
"""
import time
import threading
import traceback
from datetime import datetime
from typing import List, Optional

import httpx
import asyncio
from fastapi import APIRouter, HTTPException, Query

from database import get_db
from shared import background_tasks

router = APIRouter()

BULKEMAILCHECKER_API_URL = "https://api.bulkemailchecker.com/real-time/"


async def verify_email_realtime(email: str, api_key: str) -> dict:
    """Call BulkEmailChecker real-time API for single email verification."""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                BULKEMAILCHECKER_API_URL,
                params={"key": api_key, "email": email}
            )
            data = response.json()

            raw_status = data.get("status", "unknown").lower()
            if raw_status == "passed":
                status = "Valid"
            elif raw_status == "failed":
                status = "Invalid"
            else:
                status = "Unknown"

            event = data.get("event", "")

            return {
                "email": email,
                "status": status,
                "event": event,
                "is_disposable": data.get("isDisposable", False),
                "is_free_service": data.get("isFreeService", False),
                "is_role_account": data.get("isRoleAccount", False),
                "email_suggested": data.get("emailSuggested"),
                "credits_remaining": data.get("creditsRemaining")
            }
    except httpx.TimeoutException:
        return {
            "email": email,
            "status": "Unknown",
            "event": "timeout",
            "is_disposable": False,
            "is_free_service": False,
            "is_role_account": False,
            "email_suggested": None
        }
    except Exception as e:
        return {
            "email": email,
            "status": "Unknown",
            "event": f"error: {str(e)}",
            "is_disposable": False,
            "is_free_service": False,
            "is_role_account": False,
            "email_suggested": None
        }


def update_contact_verification(conn, contact_id: int, result: dict):
    """Update a contact with verification results."""
    now = datetime.now().isoformat()
    is_disposable = bool(result.get("is_disposable", False))
    is_free_service = bool(result.get("is_free_service", False))
    is_role_account = bool(result.get("is_role_account", False))

    conn.execute("""
        UPDATE contacts SET
            email_status = ?,
            email_verification_event = ?,
            email_is_disposable = ?,
            email_is_free_service = ?,
            email_is_role_account = ?,
            email_suggested = ?,
            email_verified_at = ?,
            updated_at = ?
        WHERE id = ?
    """, (
        result["status"],
        result.get("event", ""),
        is_disposable,
        is_free_service,
        is_role_account,
        result.get("email_suggested", ""),
        now,
        now,
        contact_id
    ))


def verify_email_sync(email: str, api_key: str) -> dict:
    """Synchronous email verification using httpx (for thread)."""
    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.get(
                BULKEMAILCHECKER_API_URL,
                params={"key": api_key, "email": email}
            )
            data = response.json()

            if "error" in data:
                error_msg = data.get("error", "Unknown error")
                print(f"[VERIFY] API error for {email}: {error_msg}")
                return {
                    "status": "API_ERROR",
                    "event": f"api_error: {error_msg}",
                    "is_disposable": False,
                    "is_free_service": False,
                    "is_role_account": False,
                    "email_suggested": "",
                    "error": error_msg
                }

            if "status" not in data:
                print(f"[VERIFY] No status in response for {email}: {data}")
                return {
                    "status": "API_ERROR",
                    "event": "api_error: no status in response",
                    "is_disposable": False,
                    "is_free_service": False,
                    "is_role_account": False,
                    "email_suggested": "",
                    "error": "No status in response"
                }

            raw_status = data.get("status", "").lower()
            if raw_status == "passed":
                status = "Valid"
            elif raw_status == "failed":
                status = "Invalid"
            elif raw_status == "unknown":
                status = "Unknown"
            else:
                print(f"[VERIFY] Unexpected status '{raw_status}' for {email}")
                status = "Unknown"

            return {
                "status": status,
                "event": data.get("event", ""),
                "is_disposable": data.get("isDisposable", False),
                "is_free_service": data.get("isFreeService", False),
                "is_role_account": data.get("isRoleAccount", False),
                "email_suggested": data.get("emailSuggested", "")
            }
    except Exception as e:
        print(f"[VERIFY] Exception verifying {email}: {e}")
        return {
            "status": "API_ERROR",
            "event": f"exception: {str(e)}",
            "is_disposable": False,
            "is_free_service": False,
            "is_role_account": False,
            "email_suggested": "",
            "error": str(e)
        }


def run_verification_job_sync(job_id: int):
    """Synchronous background task to verify emails (runs in thread)."""
    print(f"[VERIFY THREAD] Starting job {job_id}")
    conn = None

    REQUEST_DELAY = 2.5
    RATE_LIMIT_PAUSE = 300  # pause on rate limit before retry
    MAX_CONSECUTIVE_ERRORS = 50  # only true API errors (not timeouts) count

    try:
        conn = get_db()
        job = conn.execute(
            "SELECT contact_ids FROM verification_jobs WHERE id=?", (job_id,)
        ).fetchone()
        if not job:
            print(f"[VERIFY THREAD] Job {job_id} not found")
            return

        contact_ids = [int(x) for x in job[0].split(',') if x]
        print(f"[VERIFY THREAD] Processing {len(contact_ids)} contacts (delay: {REQUEST_DELAY}s between requests)")

        api_row = conn.execute(
            "SELECT value FROM settings WHERE key='bulkemailchecker_api_key'"
        ).fetchone()
        if not api_row or not api_row[0]:
            conn.execute(
                "UPDATE verification_jobs SET status='failed', error_message='API key not configured' WHERE id=?",
                (job_id,)
            )
            conn.commit()
            return

        api_key = api_row[0]

        conn.execute(
            "UPDATE verification_jobs SET status='running', started_at=? WHERE id=?",
            (datetime.now().isoformat(), job_id)
        )
        conn.commit()

        verified = valid = invalid = unknown = skipped = 0
        api_errors = 0
        consecutive_errors = 0

        for cid in contact_ids:
            job_status = conn.execute(
                "SELECT status FROM verification_jobs WHERE id=?", (job_id,)
            ).fetchone()
            if job_status and job_status[0] == 'cancelled':
                print(f"[VERIFY THREAD] Job {job_id} was cancelled")
                break

            contact = conn.execute(
                "SELECT email, email_status, email_verified_at FROM contacts WHERE id=? AND email IS NOT NULL AND email != ''",
                (cid,)
            ).fetchone()

            if not contact:
                continue

            email, current_status, verified_at = contact[0], contact[1], contact[2]

            # Skip final states: Valid/Invalid, or Unknown that was already attempted (has verified_at)
            if current_status in ['Valid', 'Invalid'] or (current_status == 'Unknown' and verified_at):
                skipped += 1
                conn.execute(
                    "UPDATE verification_jobs SET skipped_count=?, current_email=? WHERE id=?",
                    (skipped, f"Skipped: {email}", job_id)
                )
                conn.commit()
                continue

            conn.execute(
                "UPDATE verification_jobs SET current_email=? WHERE id=?", (email, job_id)
            )
            conn.commit()

            time.sleep(REQUEST_DELAY)

            result = verify_email_sync(email, api_key)

            if result.get("status") == "API_ERROR":
                error_msg = result.get("error", "").lower()
                api_errors += 1

                # Rate limit → pause 5 min, retry once
                if "rate" in error_msg or "limit" in error_msg or "too many" in error_msg:
                    print(f"[VERIFY THREAD] Rate limit! Pausing {RATE_LIMIT_PAUSE}s...")
                    conn.execute(
                        "UPDATE verification_jobs SET current_email=? WHERE id=?",
                        (f"Rate limited - pausing {RATE_LIMIT_PAUSE}s...", job_id)
                    )
                    conn.commit()
                    time.sleep(RATE_LIMIT_PAUSE)
                    consecutive_errors = 0
                    result = verify_email_sync(email, api_key)
                    if result.get("status") == "API_ERROR":
                        print(f"[VERIFY THREAD] Still rate-limited, skipping {email}")
                        continue

                # Timeout/network → mark as Unknown immediately, move on (no pause, no retry)
                elif ("timeout" in error_msg or "timed out" in error_msg
                      or "read operation" in error_msg or "connect" in error_msg
                      or "network" in error_msg or "connection" in error_msg):
                    print(f"[VERIFY THREAD] Timeout for {email}, marking Unknown and moving on")
                    # Save Unknown with verified_at so it won't be re-queued
                    conn.execute(
                        "UPDATE contacts SET email_status='Unknown', email_verified_at=? WHERE id=?",
                        (datetime.now().isoformat(), cid)
                    )
                    conn.commit()
                    unknown += 1
                    verified += 1
                    consecutive_errors = 0
                    conn.execute("""UPDATE verification_jobs SET
                        verified_count=?, valid_count=?, invalid_count=?, unknown_count=?, skipped_count=?
                        WHERE id=?""", (verified, valid, invalid, unknown, skipped, job_id))
                    conn.commit()
                    continue

                # Other errors → skip, count consecutive, only kill on true API failures
                else:
                    consecutive_errors += 1
                    print(f"[VERIFY THREAD] API error ({consecutive_errors}/{MAX_CONSECUTIVE_ERRORS}) for {email}: {error_msg}")
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                        print(f"[VERIFY THREAD] Too many consecutive API errors, stopping job")
                        conn.execute(
                            "UPDATE verification_jobs SET status='failed', error_message=? WHERE id=?",
                            (f"Too many consecutive API errors. Last error: {error_msg}", job_id)
                        )
                        conn.commit()
                        return
                    continue
            else:
                consecutive_errors = 0

            if result.get("status") != "API_ERROR":
                update_contact_verification(conn, cid, result)
                verified += 1

                if result["status"] == "Valid":
                    valid += 1
                elif result["status"] == "Invalid":
                    invalid += 1
                else:
                    unknown += 1

                print(f"[VERIFY THREAD] Verified {email}: {result['status']}")

            conn.execute("""UPDATE verification_jobs SET
                verified_count=?, valid_count=?, invalid_count=?, unknown_count=?, skipped_count=?
                WHERE id=?""",
                (verified, valid, invalid, unknown, skipped, job_id)
            )
            conn.commit()

        conn.execute("""UPDATE verification_jobs SET
            status='completed', current_email=NULL, completed_at=?
            WHERE id=?""",
            (datetime.now().isoformat(), job_id)
        )
        conn.commit()
        print(f"[VERIFY THREAD] Job {job_id} completed: {verified} verified, {valid} valid, {invalid} invalid, {unknown} unknown, {api_errors} API errors")

    except Exception as e:
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"[VERIFY THREAD] Job {job_id} failed: {error_msg}")
        try:
            if conn:
                conn.execute(
                    "UPDATE verification_jobs SET status='failed', error_message=? WHERE id=?",
                    (str(e)[:500], job_id)
                )
                conn.commit()
        except Exception as db_err:
            print(f"[VERIFY THREAD] Failed to update job status: {db_err}")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        if job_id in background_tasks:
            del background_tasks[job_id]


def start_verification_thread(job_id: int):
    """Start verification job in a background thread."""
    thread = threading.Thread(target=run_verification_job_sync, args=(job_id,), daemon=True)
    thread.start()
    print(f"[VERIFY] Started background thread for job {job_id}")


# ==================== ENDPOINTS ====================

@router.get("/api/verify/status")
def get_verification_status():
    """Check if email verification is configured."""
    conn = get_db()
    row = conn.execute(
        "SELECT value FROM settings WHERE key='bulkemailchecker_api_key'"
    ).fetchone()
    unverified = conn.execute("""
        SELECT COUNT(*) FROM contacts
        WHERE email IS NOT NULL AND email != ''
        AND (email_status = 'Not Verified' OR email_status IS NULL)
    """).fetchone()[0]
    conn.close()
    return {"configured": bool(row and row[0]), "unverified_count": unverified}


@router.post("/api/verify/fix-unknown")
def fix_unknown_contacts():
    """Fix contacts that have 'Unknown' status - reset them to 'Not Verified'."""
    conn = get_db()
    result = conn.execute("""
        UPDATE contacts
        SET email_status = 'Not Verified',
            email_verified_at = NULL,
            email_verification_event = NULL
        WHERE email_status = 'Unknown'
    """)
    count = result.rowcount
    conn.commit()
    conn.close()
    return {"fixed": count, "message": f"Reset {count} contacts from 'Unknown' to 'Not Verified' - ready for verification"}


@router.post("/api/verify/single")
async def verify_single_email(email: str = Query(...)):
    """Verify a single email address (for testing)."""
    conn = get_db()
    row = conn.execute(
        "SELECT value FROM settings WHERE key='bulkemailchecker_api_key'"
    ).fetchone()
    conn.close()
    if not row or not row[0]:
        raise HTTPException(400, "BulkEmailChecker API key not configured. Go to Settings > Integrations.")

    result = await verify_email_realtime(email, row[0])
    return result


@router.post("/api/verify/contacts")
async def verify_contacts(contact_ids: List[int] = Query(...)):
    """Verify emails for specific contact IDs (only unverified contacts)."""
    conn = get_db()
    row = conn.execute(
        "SELECT value FROM settings WHERE key='bulkemailchecker_api_key'"
    ).fetchone()
    if not row or not row[0]:
        conn.close()
        raise HTTPException(400, "BulkEmailChecker API key not configured")

    api_key = row[0]

    placeholders = ','.join(['?'] * len(contact_ids))
    contacts = conn.execute(f"""
        SELECT id, email FROM contacts
        WHERE id IN ({placeholders})
        AND email IS NOT NULL AND email != ''
        AND (email_status IN ('Not Verified', 'Unknown') OR email_status IS NULL)
        AND email_verified_at IS NULL
    """, contact_ids).fetchall()

    stats = {"verified": 0, "valid": 0, "invalid": 0, "unknown": 0}

    for contact in contacts:
        cid, email = contact[0], contact[1]

        await asyncio.sleep(0.2)

        result = await verify_email_realtime(email, api_key)
        update_contact_verification(conn, cid, result)

        stats["verified"] += 1
        if result["status"] == "Valid":
            stats["valid"] += 1
        elif result["status"] == "Invalid":
            stats["invalid"] += 1
        elif result["status"] == "Risky":
            stats["risky"] = stats.get("risky", 0) + 1
        else:
            stats["unknown"] += 1

    conn.commit()
    conn.close()
    return stats


@router.post("/api/verify/bulk")
def start_bulk_verification(limit: int = Query(None)):
    """Start a background job to verify all unverified contacts."""
    conn = get_db()

    api_row = conn.execute(
        "SELECT value FROM settings WHERE key='bulkemailchecker_api_key'"
    ).fetchone()
    if not api_row or not api_row[0]:
        conn.close()
        raise HTTPException(400, "BulkEmailChecker API key not configured")

    query = """
        SELECT id FROM contacts
        WHERE email IS NOT NULL AND email != ''
        AND (email_status = 'Not Verified' OR email_status IS NULL)
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    unverified = conn.execute(query).fetchall()
    unverified_ids = [r[0] for r in unverified]

    if not unverified_ids:
        conn.close()
        return {"job_id": None, "message": "No contacts need verification", "total_contacts": 0}

    conn.execute(
        """INSERT INTO verification_jobs (status, total_contacts, contact_ids, created_at)
        VALUES ('pending', ?, ?, ?)""",
        (len(unverified_ids), ','.join(map(str, unverified_ids)), datetime.now().isoformat())
    )
    job_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()

    start_verification_thread(job_id)
    background_tasks[job_id] = True

    return {"job_id": job_id, "total_contacts": len(unverified_ids)}


@router.post("/api/verify/job/start")
async def start_verification_job(contact_ids: List[int] = Query(...)):
    """Start a background verification job for multiple contacts."""
    conn = get_db()

    api_row = conn.execute(
        "SELECT value FROM settings WHERE key='bulkemailchecker_api_key'"
    ).fetchone()
    if not api_row or not api_row[0]:
        conn.close()
        raise HTTPException(400, "BulkEmailChecker API key not configured")

    placeholders = ','.join(['?'] * len(contact_ids))
    unverified = conn.execute(f"""
        SELECT id FROM contacts
        WHERE id IN ({placeholders})
        AND email IS NOT NULL AND email != ''
        AND (email_status = 'Not Verified' OR email_status IS NULL)
    """, contact_ids).fetchall()

    unverified_ids = [r[0] for r in unverified]

    if not unverified_ids:
        conn.close()
        return {"job_id": None, "message": "No contacts need verification"}

    conn.execute(
        """INSERT INTO verification_jobs (status, total_contacts, contact_ids, created_at)
        VALUES ('pending', ?, ?, ?)""",
        (len(unverified_ids), ','.join(map(str, unverified_ids)), datetime.now().isoformat())
    )
    job_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()

    start_verification_thread(job_id)
    background_tasks[job_id] = True

    return {"job_id": job_id, "total_contacts": len(unverified_ids)}


@router.get("/api/verify/job/{job_id}")
def get_verification_job(job_id: int):
    """Get status of a verification job."""
    conn = get_db()
    job = conn.execute("""SELECT id, status, total_contacts, verified_count, valid_count,
        invalid_count, unknown_count, skipped_count, current_email, error_message,
        created_at, started_at, completed_at
        FROM verification_jobs WHERE id=?""", (job_id,)).fetchone()
    conn.close()

    if not job:
        raise HTTPException(404, "Job not found")

    return {
        "id": job[0],
        "status": job[1],
        "total_contacts": job[2],
        "verified_count": job[3],
        "valid_count": job[4],
        "invalid_count": job[5],
        "unknown_count": job[6],
        "skipped_count": job[7],
        "current_email": job[8],
        "error_message": job[9],
        "created_at": job[10],
        "started_at": job[11],
        "completed_at": job[12],
        "progress": round((job[3] + job[7]) / job[2] * 100, 1) if job[2] > 0 else 0
    }


@router.post("/api/verify/job/{job_id}/cancel")
def cancel_verification_job(job_id: int):
    """Cancel a running verification job."""
    conn = get_db()
    conn.execute(
        "UPDATE verification_jobs SET status='cancelled' WHERE id=? AND status='running'", (job_id,)
    )
    conn.commit()
    conn.close()
    return {"status": "cancelled"}


@router.get("/api/verify/jobs/active")
def get_active_verification_jobs():
    """Get all active (pending/running) verification jobs."""
    conn = get_db()
    jobs = conn.execute("""SELECT id, status, total_contacts, verified_count, valid_count,
        invalid_count, unknown_count, skipped_count, current_email, created_at
        FROM verification_jobs WHERE status IN ('pending', 'running')
        ORDER BY created_at DESC""").fetchall()
    conn.close()

    return [{
        "id": j[0],
        "status": j[1],
        "total_contacts": j[2],
        "verified_count": j[3],
        "valid_count": j[4],
        "invalid_count": j[5],
        "unknown_count": j[6],
        "skipped_count": j[7],
        "current_email": j[8],
        "created_at": j[9],
        "progress": round((j[3] + j[7]) / j[2] * 100, 1) if j[2] > 0 else 0
    } for j in jobs]
