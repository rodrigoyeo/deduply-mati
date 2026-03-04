"""
Database abstraction layer for Deduply
Supports both SQLite (local development) and PostgreSQL (production/Supabase)
"""
import os
import re
import sqlite3
import socket
from contextlib import contextmanager
from urllib.parse import urlparse, urlunparse

# Force IPv4 for database connections (Railway doesn't support IPv6 outbound)
original_getaddrinfo = socket.getaddrinfo

def ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    """Force IPv4 resolution for database hosts"""
    return original_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)

# Check if we have a PostgreSQL URL (production) or use SQLite (local)
DATABASE_URL = os.getenv("DATABASE_URL")
USE_POSTGRES = DATABASE_URL is not None and DATABASE_URL.startswith("postgresql")

# Debug logging at startup
print(f"[DB CONFIG] DATABASE_URL set: {DATABASE_URL is not None}")
print(f"[DB CONFIG] USE_POSTGRES: {USE_POSTGRES}")
if DATABASE_URL:
    # Print masked URL for debugging (hide password)
    masked = DATABASE_URL[:20] + "***" + DATABASE_URL[-30:] if len(DATABASE_URL) > 50 else "***"
    print(f"[DB CONFIG] URL preview: {masked}")

if USE_POSTGRES:
    import psycopg2
    import psycopg2.extras
    from psycopg2.extras import DictCursor
    # Force IPv4 globally — Railway doesn't support IPv6 outbound
    socket.getaddrinfo = ipv4_only_getaddrinfo
    print("[DB CONFIG] psycopg2 imported successfully")
    print("[DB CONFIG] IPv4-only mode enabled for Railway")

class DatabaseConnection:
    """Wrapper to provide consistent interface for both SQLite and PostgreSQL"""

    def __init__(self, conn, is_postgres=False):
        self.conn = conn
        self.is_postgres = is_postgres
        self._cursor = None

    def execute(self, query, params=None):
        """Execute a query, converting SQLite syntax to PostgreSQL if needed"""
        if self.is_postgres:
            # Convert SQLite ? placeholders to PostgreSQL %s
            query = query.replace("?", "%s")
            # Convert AUTOINCREMENT to SERIAL (already handled in schema)
            # Convert INTEGER PRIMARY KEY to SERIAL PRIMARY KEY
            query = query.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
            # Handle INSERT OR IGNORE -> INSERT ... ON CONFLICT DO NOTHING
            if "INSERT OR IGNORE" in query:
                query = query.replace("INSERT OR IGNORE", "INSERT")
                query = query.rstrip(";") + " ON CONFLICT DO NOTHING"
            # Handle boolean differences - only for known boolean columns
            # Replace boolean column comparisons (is_duplicate=0, is_active=1, etc.)
            bool_cols = r'(is_duplicate|is_active|is_winner|processed|email_is_disposable|email_is_free_service|email_is_role_account)'
            query = re.sub(bool_cols + r'=0', r'\1=FALSE', query)
            query = re.sub(bool_cols + r'=1', r'\1=TRUE', query)
            query = re.sub(bool_cols + r' = 0', r'\1 = FALSE', query)
            query = re.sub(bool_cols + r' = 1', r'\1 = TRUE', query)
            # Handle SQLite last_insert_rowid() -> PostgreSQL LASTVAL()
            query = query.replace("last_insert_rowid()", "LASTVAL()")
            # Handle SQLite GROUP_CONCAT -> PostgreSQL STRING_AGG
            query = re.sub(r'GROUP_CONCAT\((\w+)\)', r"STRING_AGG(\1::text, ',')", query)

        # Use DictCursor for PostgreSQL to return rows with both index and key access
        if self.is_postgres:
            cursor = self.conn.cursor(cursor_factory=DictCursor)
        else:
            cursor = self.conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor

    def executemany(self, query, params_list):
        """Execute many queries"""
        if self.is_postgres:
            query = query.replace("?", "%s")
            cursor = self.conn.cursor(cursor_factory=DictCursor)
        else:
            cursor = self.conn.cursor()
        cursor.executemany(query, params_list)
        return cursor

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

    def cursor(self):
        return self.conn.cursor()


def get_db():
    """Get a database connection (SQLite for local, PostgreSQL for production)"""
    if USE_POSTGRES:
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
            conn.autocommit = False
            return DatabaseConnection(conn, is_postgres=True)
        except Exception as e:
            print(f"[DB] PostgreSQL connection failed: {e}")
            print("[DB] Falling back to SQLite")
    # PostgreSQL failed - fall back to SQLite
    db_path = os.getenv("DATABASE_PATH", "deduply.db")
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return DatabaseConnection(conn, is_postgres=False)


def init_db():
    """Initialize database tables"""
    try:
        db = get_db()
    except Exception as e:
        print(f"[DB INIT] Connection failed: {e}")
        raise

    if db.is_postgres:
        # For PostgreSQL, verify connection and create default admin if needed
        try:
            db.execute("SELECT 1")
            print("PostgreSQL connection successful")

            # Create default admin user if no users exist
            import hashlib
            import secrets
            cursor = db.execute("SELECT COUNT(*) FROM users")
            count = cursor.fetchone()[0]
            if count == 0:
                token = secrets.token_urlsafe(32)
                pwd_hash = hashlib.sha256("admin123".encode()).hexdigest()
                db.execute(
                    "INSERT INTO users (email, password_hash, name, role, api_token, is_active) VALUES (%s, %s, %s, %s, %s, %s)",
                    ("admin@deduply.io", pwd_hash, "Admin", "admin", token, True)
                )
                db.commit()
                print("Created default admin user: admin@deduply.io / admin123")
        except Exception as e:
            print(f"PostgreSQL init error: {e}")
        db.close()
        return

    # SQLite initialization (local development)
    cur = db.conn.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL, name TEXT, role TEXT DEFAULT 'member',
        api_token TEXT UNIQUE, is_active BOOLEAN DEFAULT 1, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS contacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, first_name TEXT, last_name TEXT, email TEXT,
        title TEXT, headline TEXT, company TEXT, seniority TEXT, first_phone TEXT, corporate_phone TEXT,
        employees INTEGER, employee_bucket TEXT, industry TEXT, keywords TEXT,
        person_linkedin_url TEXT, website TEXT, domain TEXT, company_linkedin_url TEXT,
        city TEXT, state TEXT, country TEXT,
        company_city TEXT, company_state TEXT, company_country TEXT,
        company_street_address TEXT, company_postal_code TEXT,
        annual_revenue INTEGER, annual_revenue_text TEXT,
        company_description TEXT, company_seo_description TEXT, company_founded_year INTEGER,
        region TEXT,
        country_strategy TEXT,
        reachinbox_workspace TEXT,
        reachinbox_lead_id TEXT, reachinbox_pushed_at TIMESTAMP,
        reachinbox_campaign_id INTEGER,
        pipeline_stage TEXT DEFAULT 'new',
        enrichment_source TEXT,
        hubspot_queued BOOLEAN DEFAULT 0, hubspot_synced_at TIMESTAMP,
        hubspot_contact_id TEXT, hubspot_deal_id TEXT,
        icp_tier INTEGER,
        blitz_company_linkedin TEXT, blitz_person_linkedin TEXT, blitz_enriched_at TIMESTAMP,
        outreach_lists TEXT, campaigns_assigned TEXT, status TEXT DEFAULT 'Lead',
        email_status TEXT DEFAULT 'Unknown',
        email_verified_at TIMESTAMP, email_verification_event TEXT,
        email_is_disposable BOOLEAN, email_is_free_service BOOLEAN,
        email_is_role_account BOOLEAN, email_suggested TEXT,
        times_contacted INTEGER DEFAULT 0,
        last_contacted_at TIMESTAMP, opportunities INTEGER DEFAULT 0, meetings_booked INTEGER DEFAULT 0,
        notes TEXT, source_file TEXT, is_duplicate BOOLEAN DEFAULT 0, duplicate_of INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, description TEXT,
        country TEXT, status TEXT DEFAULT 'Active', total_leads INTEGER DEFAULT 0,
        emails_sent INTEGER DEFAULT 0, emails_opened INTEGER DEFAULT 0, emails_clicked INTEGER DEFAULT 0,
        emails_replied INTEGER DEFAULT 0, emails_bounced INTEGER DEFAULT 0,
        opportunities INTEGER DEFAULT 0, meetings_booked INTEGER DEFAULT 0,
        open_rate REAL DEFAULT 0, click_rate REAL DEFAULT 0, reply_rate REAL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS email_templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, variant TEXT DEFAULT 'A',
        step_type TEXT DEFAULT 'Main', subject TEXT, body TEXT,
        times_sent INTEGER DEFAULT 0, times_opened INTEGER DEFAULT 0, times_clicked INTEGER DEFAULT 0,
        times_replied INTEGER DEFAULT 0, open_rate REAL DEFAULT 0, reply_rate REAL DEFAULT 0,
        is_winner BOOLEAN DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS template_campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT, template_id INTEGER NOT NULL, campaign_id INTEGER NOT NULL,
        times_sent INTEGER DEFAULT 0, times_opened INTEGER DEFAULT 0, times_replied INTEGER DEFAULT 0,
        opportunities INTEGER DEFAULT 0, meetings INTEGER DEFAULT 0,
        FOREIGN KEY (template_id) REFERENCES email_templates(id) ON DELETE CASCADE,
        FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE,
        UNIQUE(template_id, campaign_id))""")

    try:
        cur.execute("ALTER TABLE template_campaigns ADD COLUMN opportunities INTEGER DEFAULT 0")
    except: pass
    try:
        cur.execute("ALTER TABLE template_campaigns ADD COLUMN meetings INTEGER DEFAULT 0")
    except: pass

    cur.execute("""CREATE TABLE IF NOT EXISTS outreach_lists (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL,
        description TEXT, contact_count INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS webhook_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, event_type TEXT, email TEXT,
        campaign_name TEXT, template_id INTEGER, payload TEXT, processed BOOLEAN DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    # Settings table for API keys and configuration
    cur.execute("""CREATE TABLE IF NOT EXISTS settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT UNIQUE NOT NULL, value TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    # Verification jobs table for background email verification
    cur.execute("""CREATE TABLE IF NOT EXISTS verification_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        status TEXT DEFAULT 'pending',
        total_contacts INTEGER DEFAULT 0,
        verified_count INTEGER DEFAULT 0,
        valid_count INTEGER DEFAULT 0,
        invalid_count INTEGER DEFAULT 0,
        unknown_count INTEGER DEFAULT 0,
        skipped_count INTEGER DEFAULT 0,
        current_email TEXT,
        error_message TEXT,
        contact_ids TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        started_at TIMESTAMP,
        completed_at TIMESTAMP)""")

    # Import jobs table for background CSV import
    cur.execute("""CREATE TABLE IF NOT EXISTS import_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        status TEXT DEFAULT 'pending',
        total_rows INTEGER DEFAULT 0,
        processed_count INTEGER DEFAULT 0,
        imported_count INTEGER DEFAULT 0,
        merged_count INTEGER DEFAULT 0,
        duplicates_found INTEGER DEFAULT 0,
        failed_count INTEGER DEFAULT 0,
        current_row TEXT,
        error_message TEXT,
        file_name TEXT,
        file_path TEXT,
        column_mapping TEXT,
        outreach_list TEXT,
        campaigns TEXT,
        country_strategy TEXT,
        check_duplicates INTEGER DEFAULT 1,
        merge_duplicates INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        started_at TIMESTAMP,
        completed_at TIMESTAMP)""")

    # Add email verification columns to contacts table
    try:
        cur.execute("ALTER TABLE contacts ADD COLUMN email_verified_at TIMESTAMP")
    except: pass
    try:
        cur.execute("ALTER TABLE contacts ADD COLUMN email_verification_event TEXT")
    except: pass
    try:
        cur.execute("ALTER TABLE contacts ADD COLUMN email_is_disposable BOOLEAN DEFAULT 0")
    except: pass
    try:
        cur.execute("ALTER TABLE contacts ADD COLUMN email_is_free_service BOOLEAN DEFAULT 0")
    except: pass
    try:
        cur.execute("ALTER TABLE contacts ADD COLUMN email_is_role_account BOOLEAN DEFAULT 0")
    except: pass
    try:
        cur.execute("ALTER TABLE contacts ADD COLUMN email_suggested TEXT")
    except: pass

    # Lead Gen tables (pipeline-two-stage)
    cur.execute("""CREATE TABLE IF NOT EXISTS lead_gen_jobs (
        id TEXT PRIMARY KEY,
        job_type TEXT NOT NULL,
        status TEXT DEFAULT 'pending',
        parameters TEXT,
        workspace TEXT DEFAULT 'US',
        results_count INTEGER DEFAULT 0,
        error TEXT,
        approval_status TEXT DEFAULT 'pending',
        approved_at TIMESTAMP,
        approved_by INTEGER,
        completed_at TIMESTAMP,
        created_by INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS lead_gen_companies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT REFERENCES lead_gen_jobs(id) ON DELETE CASCADE,
        linkedin_url TEXT,
        linkedin_id TEXT,
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
        workspace TEXT DEFAULT 'US',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    cur.execute("""CREATE TABLE IF NOT EXISTS lead_gen_contacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT REFERENCES lead_gen_jobs(id) ON DELETE CASCADE,
        company_id INTEGER REFERENCES lead_gen_companies(id) ON DELETE SET NULL,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        title TEXT,
        linkedin_url TEXT,
        company_name TEXT,
        company_domain TEXT,
        workspace TEXT DEFAULT 'US',
        icp_tier INTEGER,
        blitz_company_linkedin TEXT,
        blitz_person_linkedin TEXT,
        blitz_enriched_at TIMESTAMP,
        status TEXT DEFAULT 'pending',
        contact_id INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")

    # BlitzAPI enrichment fields on contacts
    for col, coltype in [
        ('icp_tier', 'INTEGER'),
        ('blitz_company_linkedin', 'TEXT'),
        ('blitz_person_linkedin', 'TEXT'),
        ('blitz_enriched_at', 'TIMESTAMP'),
        ('pipeline_stage', 'TEXT'),
        ('enrichment_source', 'TEXT'),
        ('reachinbox_workspace', 'TEXT'),
        ('reachinbox_lead_id', 'TEXT'),
        ('reachinbox_pushed_at', 'TIMESTAMP'),
        ('reachinbox_campaign_id', 'INTEGER'),
        ('hubspot_queued', 'BOOLEAN'),
        ('hubspot_synced_at', 'TIMESTAMP'),
        ('hubspot_contact_id', 'TEXT'),
        ('hubspot_deal_id', 'TEXT'),
    ]:
        try:
            cur.execute(f"ALTER TABLE contacts ADD COLUMN {col} {coltype}")
        except: pass

    # Create default admin user
    import hashlib
    import secrets
    cur.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] == 0:
        token = secrets.token_urlsafe(32)
        pwd_hash = hashlib.sha256("admin123".encode()).hexdigest()
        cur.execute("INSERT INTO users (email, password_hash, name, role, api_token) VALUES (?, ?, ?, ?, ?)",
                   ("admin@deduply.com", pwd_hash, "Admin", "admin", token))


    # Campaign sequences — synced from ReachInbox analytics
    cur.execute("""CREATE TABLE IF NOT EXISTS campaign_sequences (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id INTEGER NOT NULL,
        campaign_name TEXT,
        ri_campaign_id INTEGER,
        workspace TEXT DEFAULT 'US',
        step_number INTEGER NOT NULL,
        step_type TEXT DEFAULT 'initial',
        delay_days INTEGER DEFAULT 0,
        variant_index INTEGER DEFAULT 0,
        variant_subject TEXT,
        variant_body TEXT,
        sent INTEGER DEFAULT 0,
        opened INTEGER DEFAULT 0,
        replied INTEGER DEFAULT 0,
        bounced INTEGER DEFAULT 0,
        open_rate REAL DEFAULT 0,
        reply_rate REAL DEFAULT 0,
        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    db.commit()
    db.close()
    print("SQLite database initialized")


# Export for compatibility
__all__ = ['get_db', 'init_db', 'USE_POSTGRES', 'DATABASE_URL']
