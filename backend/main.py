#!/usr/bin/env python3
"""
Deduply v5.2 - Cold Email Operations Platform
FastAPI Backend — entry point.

All route logic lives in backend/routers/*.py
Shared state and helpers live in backend/shared.py
Pydantic models live in backend/models.py
"""

import hashlib
import secrets

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from database import get_db, USE_POSTGRES
from routers.users import router as users_router
from routers.contacts import router as contacts_router
from routers.campaigns import router as campaigns_router
from routers.analytics import router as analytics_router
from routers.webhooks import router as webhooks_router
from routers.settings import router as settings_router
from routers.verify import router as verify_router
from routers.reachinbox import router as reachinbox_router
from routers.leadgen import router as leadgen_router
from routers.agent import router as agent_router

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(title="Deduply API", version="5.2")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Register routers
# ---------------------------------------------------------------------------

app.include_router(users_router)
app.include_router(contacts_router)
app.include_router(campaigns_router)
app.include_router(analytics_router)
app.include_router(webhooks_router)
app.include_router(settings_router)
app.include_router(verify_router)
app.include_router(reachinbox_router)
app.include_router(leadgen_router)
app.include_router(agent_router)

# ---------------------------------------------------------------------------
# Database initialisation (SQLite only; PostgreSQL uses schema.sql)
# ---------------------------------------------------------------------------

def init_db():
    conn = get_db()

    if USE_POSTGRES:
        try:
            result = conn.execute("SELECT 1").fetchone()
            print("[DB] PostgreSQL connection successful")

            result = conn.execute("SELECT COUNT(*) FROM users").fetchone()
            if result[0] == 0:
                token = secrets.token_urlsafe(32)
                pwd_hash = hashlib.sha256("admin123".encode()).hexdigest()
                conn.execute(
                    "INSERT INTO users (email, password_hash, name, role, api_token) VALUES (?, ?, ?, ?, ?)",
                    ("admin@deduply.io", pwd_hash, "Admin", "admin", token)
                )
                conn.commit()
                print(f"Created admin: admin@deduply.io / admin123")
        except Exception as e:
            print(f"[DB] PostgreSQL error: {e}")
        conn.close()
        return

    print("[DB] Using SQLite database")
    conn.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL, name TEXT, role TEXT DEFAULT 'member',
        api_token TEXT UNIQUE, is_active BOOLEAN DEFAULT 1, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    conn.execute("""CREATE TABLE IF NOT EXISTS contacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, first_name TEXT, last_name TEXT, email TEXT,
        title TEXT, headline TEXT, company TEXT, seniority TEXT, first_phone TEXT, corporate_phone TEXT,
        employees INTEGER, employee_bucket TEXT, industry TEXT, keywords TEXT,
        person_linkedin_url TEXT, website TEXT, domain TEXT, company_linkedin_url TEXT,
        city TEXT, state TEXT, country TEXT,
        company_city TEXT, company_state TEXT, company_country TEXT,
        company_street_address TEXT, company_postal_code TEXT,
        annual_revenue INTEGER, annual_revenue_text TEXT,
        company_description TEXT, company_seo_description TEXT, company_founded_year INTEGER,
        region TEXT, country_strategy TEXT, status TEXT DEFAULT 'Lead',
        email_status TEXT DEFAULT 'Not Verified', times_contacted INTEGER DEFAULT 0,
        last_contacted_at TIMESTAMP, opportunities INTEGER DEFAULT 0, meetings_booked INTEGER DEFAULT 0,
        notes TEXT, source_file TEXT, is_duplicate BOOLEAN DEFAULT 0, duplicate_of INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    conn.execute("""CREATE TABLE IF NOT EXISTS campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, description TEXT,
        country TEXT, status TEXT DEFAULT 'Active', total_leads INTEGER DEFAULT 0,
        emails_sent INTEGER DEFAULT 0, emails_opened INTEGER DEFAULT 0, emails_clicked INTEGER DEFAULT 0,
        emails_replied INTEGER DEFAULT 0, emails_bounced INTEGER DEFAULT 0,
        opportunities INTEGER DEFAULT 0, meetings_booked INTEGER DEFAULT 0,
        open_rate REAL DEFAULT 0, click_rate REAL DEFAULT 0, reply_rate REAL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    conn.execute("""CREATE TABLE IF NOT EXISTS email_templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, variant TEXT DEFAULT 'A',
        step_type TEXT DEFAULT 'Main', subject TEXT, body TEXT,
        times_sent INTEGER DEFAULT 0, times_opened INTEGER DEFAULT 0, times_clicked INTEGER DEFAULT 0,
        times_replied INTEGER DEFAULT 0, open_rate REAL DEFAULT 0, reply_rate REAL DEFAULT 0,
        is_winner BOOLEAN DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    conn.execute("""CREATE TABLE IF NOT EXISTS template_campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT, template_id INTEGER NOT NULL, campaign_id INTEGER NOT NULL,
        times_sent INTEGER DEFAULT 0, times_opened INTEGER DEFAULT 0, times_replied INTEGER DEFAULT 0,
        opportunities INTEGER DEFAULT 0, meetings INTEGER DEFAULT 0,
        FOREIGN KEY (template_id) REFERENCES email_templates(id) ON DELETE CASCADE,
        FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE,
        UNIQUE(template_id, campaign_id))""")

    conn.execute("""CREATE TABLE IF NOT EXISTS contact_campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT, contact_id INTEGER NOT NULL, campaign_id INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE,
        FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE,
        UNIQUE(contact_id, campaign_id))""")

    conn.execute("""CREATE TABLE IF NOT EXISTS contact_lists (
        id INTEGER PRIMARY KEY AUTOINCREMENT, contact_id INTEGER NOT NULL, list_id INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE,
        FOREIGN KEY (list_id) REFERENCES outreach_lists(id) ON DELETE CASCADE,
        UNIQUE(contact_id, list_id))""")

    conn.execute("""CREATE TABLE IF NOT EXISTS technologies (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    conn.execute("""CREATE TABLE IF NOT EXISTS contact_technologies (
        id INTEGER PRIMARY KEY AUTOINCREMENT, contact_id INTEGER NOT NULL, technology_id INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (contact_id) REFERENCES contacts(id) ON DELETE CASCADE,
        FOREIGN KEY (technology_id) REFERENCES technologies(id) ON DELETE CASCADE,
        UNIQUE(contact_id, technology_id))""")

    conn.execute("""CREATE TABLE IF NOT EXISTS outreach_lists (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL,
        description TEXT, contact_count INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    conn.execute("""CREATE TABLE IF NOT EXISTS webhook_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, event_type TEXT, email TEXT,
        campaign_name TEXT, template_id INTEGER, payload TEXT, processed BOOLEAN DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    conn.execute("""CREATE TABLE IF NOT EXISTS settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT UNIQUE NOT NULL, value TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    # Migrations
    for col in [
        "ALTER TABLE contacts ADD COLUMN email_verified_at TIMESTAMP",
        "ALTER TABLE contacts ADD COLUMN reachinbox_lead_id TEXT",
        "ALTER TABLE contacts ADD COLUMN reachinbox_workspace TEXT",
        "ALTER TABLE contacts ADD COLUMN reachinbox_pushed_at TIMESTAMP",
        "ALTER TABLE contacts ADD COLUMN reachinbox_campaign_id INTEGER",
        "ALTER TABLE contacts ADD COLUMN pipeline_stage TEXT DEFAULT 'new'",
        "ALTER TABLE contacts ADD COLUMN enrichment_source TEXT",
        "ALTER TABLE campaigns ADD COLUMN market TEXT DEFAULT 'US'",
        "ALTER TABLE contacts ADD COLUMN email_verification_event TEXT",
        "ALTER TABLE contacts ADD COLUMN email_is_disposable BOOLEAN DEFAULT 0",
        "ALTER TABLE contacts ADD COLUMN email_is_free_service BOOLEAN DEFAULT 0",
        "ALTER TABLE contacts ADD COLUMN email_is_role_account BOOLEAN DEFAULT 0",
        "ALTER TABLE contacts ADD COLUMN email_suggested TEXT",
        "ALTER TABLE email_templates ADD COLUMN country TEXT",
        "ALTER TABLE contacts ADD COLUMN hubspot_queued BOOLEAN DEFAULT 0",
        "ALTER TABLE contacts ADD COLUMN hubspot_synced_at TIMESTAMP",
    ]:
        try:
            conn.execute(col)
        except Exception:
            pass

    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS reachinbox_push_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
            campaign_id INTEGER REFERENCES campaigns(id),
            reachinbox_campaign_id INTEGER NOT NULL,
            workspace TEXT NOT NULL,
            status TEXT DEFAULT 'pushed',
            error_message TEXT,
            pushed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    except Exception:
        pass

    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS lead_gen_jobs (
            id TEXT PRIMARY KEY,
            job_type TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            parameters TEXT,
            results_count INTEGER DEFAULT 0,
            imported_count INTEGER DEFAULT 0,
            credits_used REAL DEFAULT 0,
            workspace TEXT DEFAULT 'US',
            created_by INTEGER REFERENCES users(id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            error TEXT)""")
    except Exception:
        pass

    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS lead_gen_companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT REFERENCES lead_gen_jobs(id),
            linkedin_url TEXT,
            linkedin_id INTEGER,
            name TEXT,
            about TEXT,
            industry TEXT,
            type TEXT,
            size TEXT,
            employees_on_linkedin INTEGER,
            followers INTEGER,
            founded_year INTEGER,
            domain TEXT,
            hq_country TEXT,
            hq_city TEXT,
            hq_continent TEXT,
            raw_data TEXT,
            imported INTEGER DEFAULT 0,
            workspace TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    except Exception:
        pass

    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lead_gen_companies_job ON lead_gen_companies(job_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lead_gen_companies_domain ON lead_gen_companies(domain)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lead_gen_jobs_status ON lead_gen_jobs(status)")
    except Exception:
        pass

    result = conn.execute("SELECT COUNT(*) FROM users").fetchone()
    if result[0] == 0:
        token = secrets.token_urlsafe(32)
        pwd_hash = hashlib.sha256("admin123".encode()).hexdigest()
        conn.execute(
            "INSERT INTO users (email, password_hash, name, role, api_token) VALUES (?, ?, ?, ?, ?)",
            ("admin@deduply.io", pwd_hash, "Admin", "admin", token)
        )
        print(f"Created admin: admin@deduply.io / admin123")

    conn.commit()
    conn.close()


init_db()


if __name__ == "__main__":
    import uvicorn
    print("Starting Deduply API v5.2")
    print("Login: admin@deduply.io / admin123")
    uvicorn.run(app, host="0.0.0.0", port=8001)
