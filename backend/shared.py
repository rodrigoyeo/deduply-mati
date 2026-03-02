"""
Shared state, helper functions, and utilities used across routers.
"""
import os
import hashlib
import secrets
import tempfile
from datetime import datetime
from typing import Optional

import bcrypt
import pandas as pd
from fastapi import Header

from database import get_db, USE_POSTGRES


# ---------------------------------------------------------------------------
# Global state (shared between routers)
# ---------------------------------------------------------------------------

# Global store for background verification tasks: {job_id: True}
background_tasks = {}

# Global store for background import tasks: {job_id: True}
import_tasks = {}

# Temp directory for import files
IMPORT_TEMP_DIR = os.path.join(tempfile.gettempdir(), 'deduply_imports')
os.makedirs(IMPORT_TEMP_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Password helpers (supports both legacy SHA256 and new bcrypt)
# ---------------------------------------------------------------------------

def hash_password(password: str) -> str:
    """Hash a password using bcrypt."""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, stored_hash: str) -> bool:
    """Verify password against stored hash (supports both SHA256 legacy and bcrypt)."""
    if stored_hash.startswith('$2b$') or stored_hash.startswith('$2a$'):
        return bcrypt.checkpw(password.encode(), stored_hash.encode())
    # Legacy SHA256 hash
    sha256_hash = hashlib.sha256(password.encode()).hexdigest()
    return sha256_hash == stored_hash


# ---------------------------------------------------------------------------
# Auth helper
# ---------------------------------------------------------------------------

def get_current_user(authorization: Optional[str] = Header(None)):
    if not authorization:
        return None
    token = authorization.replace("Bearer ", "").strip()
    conn = get_db()
    user = conn.execute(
        "SELECT * FROM users WHERE api_token=? AND is_active=1", (token,)
    ).fetchone()
    conn.close()
    return dict(user) if user else None


# ---------------------------------------------------------------------------
# Junction-table helpers
# ---------------------------------------------------------------------------

def get_contact_campaigns(conn, contact_id):
    """Get campaign names for a contact as comma-separated string."""
    rows = conn.execute("""
        SELECT c.name FROM contact_campaigns cc
        JOIN campaigns c ON cc.campaign_id = c.id
        WHERE cc.contact_id = ?
        ORDER BY c.name
    """, (contact_id,)).fetchall()
    return ', '.join(r[0] for r in rows) if rows else None


def get_contact_lists(conn, contact_id):
    """Get outreach list names for a contact as comma-separated string."""
    rows = conn.execute("""
        SELECT ol.name FROM contact_lists cl
        JOIN outreach_lists ol ON cl.list_id = ol.id
        WHERE cl.contact_id = ?
        ORDER BY ol.name
    """, (contact_id,)).fetchall()
    return ', '.join(r[0] for r in rows) if rows else None


def get_contact_technologies(conn, contact_id):
    """Get technology names for a contact as comma-separated string."""
    rows = conn.execute("""
        SELECT t.name FROM contact_technologies ct
        JOIN technologies t ON ct.technology_id = t.id
        WHERE ct.contact_id = ?
        ORDER BY t.name
    """, (contact_id,)).fetchall()
    return ', '.join(r[0] for r in rows) if rows else None


def set_contact_campaigns(conn, contact_id, campaign_names):
    """Set campaigns for a contact (replaces existing)."""
    conn.execute("DELETE FROM contact_campaigns WHERE contact_id=?", (contact_id,))
    if campaign_names:
        names = [n.strip() for n in campaign_names.split(',') if n.strip()]
        for name in names:
            conn.execute("INSERT OR IGNORE INTO campaigns (name) VALUES (?)", (name,))
            camp = conn.execute("SELECT id FROM campaigns WHERE name=?", (name,)).fetchone()
            if camp:
                conn.execute(
                    "INSERT OR IGNORE INTO contact_campaigns (contact_id, campaign_id) VALUES (?, ?)",
                    (contact_id, camp[0])
                )


def set_contact_lists(conn, contact_id, list_names):
    """Set outreach lists for a contact (replaces existing)."""
    conn.execute("DELETE FROM contact_lists WHERE contact_id=?", (contact_id,))
    if list_names:
        names = [n.strip() for n in list_names.split(',') if n.strip()]
        for name in names:
            conn.execute("INSERT OR IGNORE INTO outreach_lists (name) VALUES (?)", (name,))
            lst = conn.execute("SELECT id FROM outreach_lists WHERE name=?", (name,)).fetchone()
            if lst:
                conn.execute(
                    "INSERT OR IGNORE INTO contact_lists (contact_id, list_id) VALUES (?, ?)",
                    (contact_id, lst[0])
                )


def add_contact_campaign(conn, contact_id, campaign_name):
    """Add a single campaign to a contact."""
    conn.execute("INSERT OR IGNORE INTO campaigns (name) VALUES (?)", (campaign_name,))
    camp = conn.execute("SELECT id FROM campaigns WHERE name=?", (campaign_name,)).fetchone()
    if camp:
        conn.execute(
            "INSERT OR IGNORE INTO contact_campaigns (contact_id, campaign_id) VALUES (?, ?)",
            (contact_id, camp[0])
        )


def add_contact_list(conn, contact_id, list_name):
    """Add a single outreach list to a contact."""
    conn.execute("INSERT OR IGNORE INTO outreach_lists (name) VALUES (?)", (list_name,))
    lst = conn.execute("SELECT id FROM outreach_lists WHERE name=?", (list_name,)).fetchone()
    if lst:
        conn.execute(
            "INSERT OR IGNORE INTO contact_lists (contact_id, list_id) VALUES (?, ?)",
            (contact_id, lst[0])
        )


def add_contact_technology(conn, contact_id, tech_name):
    """Add a single technology to a contact."""
    conn.execute("INSERT OR IGNORE INTO technologies (name) VALUES (?)", (tech_name,))
    tech = conn.execute("SELECT id FROM technologies WHERE name=?", (tech_name,)).fetchone()
    if tech:
        conn.execute(
            "INSERT OR IGNORE INTO contact_technologies (contact_id, technology_id) VALUES (?, ?)",
            (contact_id, tech[0])
        )


def remove_contact_campaign(conn, contact_id, campaign_name):
    """Remove a single campaign from a contact."""
    camp = conn.execute("SELECT id FROM campaigns WHERE name=?", (campaign_name,)).fetchone()
    if camp:
        conn.execute(
            "DELETE FROM contact_campaigns WHERE contact_id=? AND campaign_id=?",
            (contact_id, camp[0])
        )


def remove_contact_list(conn, contact_id, list_name):
    """Remove a single outreach list from a contact."""
    lst = conn.execute("SELECT id FROM outreach_lists WHERE name=?", (list_name,)).fetchone()
    if lst:
        conn.execute(
            "DELETE FROM contact_lists WHERE contact_id=? AND list_id=?",
            (contact_id, lst[0])
        )


def enrich_contact_with_relations(conn, contact_dict):
    """Add campaigns_assigned and outreach_lists to contact dict."""
    contact_dict['campaigns_assigned'] = get_contact_campaigns(conn, contact_dict['id'])
    contact_dict['outreach_lists'] = get_contact_lists(conn, contact_dict['id'])
    return contact_dict


# ---------------------------------------------------------------------------
# Computation helpers
# ---------------------------------------------------------------------------

def compute_employee_bucket(emp):
    if emp is None or (isinstance(emp, float) and pd.isna(emp)):
        return None
    try:
        n = int(float(emp))
        if n <= 10:
            return "1-10"
        elif n <= 50:
            return "11-50"
        elif n <= 200:
            return "51-200"
        elif n <= 500:
            return "201-500"
        elif n <= 1000:
            return "501-1000"
        else:
            return "1000+"
    except Exception:
        return None


def update_counts():
    """Update campaign total_leads and outreach_list contact_count from junction tables."""
    conn = get_db()
    conn.execute("""
        UPDATE campaigns SET total_leads = (
            SELECT COUNT(DISTINCT cc.contact_id)
            FROM contact_campaigns cc
            JOIN contacts c ON cc.contact_id = c.id
            WHERE cc.campaign_id = campaigns.id AND c.is_duplicate = 0
        )
    """)
    conn.execute("""
        UPDATE outreach_lists SET contact_count = (
            SELECT COUNT(DISTINCT cl.contact_id)
            FROM contact_lists cl
            JOIN contacts c ON cl.contact_id = c.id
            WHERE cl.list_id = outreach_lists.id AND c.is_duplicate = 0
        )
    """)
    conn.commit()
    conn.close()


def recalc_rates(campaign_id, conn=None):
    should_close = conn is None
    if should_close:
        conn = get_db()
    conn.execute("""UPDATE campaigns SET
        open_rate = CASE WHEN emails_sent>0 THEN ROUND(100.0*emails_opened/emails_sent,1) ELSE 0 END,
        click_rate = CASE WHEN emails_sent>0 THEN ROUND(100.0*emails_clicked/emails_sent,1) ELSE 0 END,
        reply_rate = CASE WHEN emails_sent>0 THEN ROUND(100.0*emails_replied/emails_sent,1) ELSE 0 END WHERE id=?""",
        (campaign_id,))
    if should_close:
        conn.commit()
        conn.close()


def recalc_template_rates(template_id, conn=None):
    should_close = conn is None
    if should_close:
        conn = get_db()
    row = conn.execute(
        "SELECT COALESCE(SUM(times_sent),0), COALESCE(SUM(times_opened),0), COALESCE(SUM(times_replied),0) FROM template_campaigns WHERE template_id=?",
        (template_id,)
    ).fetchone()
    t = conn.execute(
        "SELECT times_sent, times_opened, times_replied FROM email_templates WHERE id=?",
        (template_id,)
    ).fetchone()
    ts = (t[0] or 0) + row[0]
    to = (t[1] or 0) + row[1]
    tr = (t[2] or 0) + row[2]
    conn.execute(
        "UPDATE email_templates SET open_rate=?, reply_rate=?, updated_at=? WHERE id=?",
        (
            round(100 * to / ts, 1) if ts > 0 else 0,
            round(100 * tr / ts, 1) if ts > 0 else 0,
            datetime.now().isoformat(),
            template_id
        )
    )
    if should_close:
        conn.commit()
        conn.close()
