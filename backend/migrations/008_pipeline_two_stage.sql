-- Migration 008: Two-stage pipeline staging table
CREATE TABLE IF NOT EXISTS lead_gen_contacts (
    id SERIAL PRIMARY KEY,
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
    status TEXT DEFAULT 'pending',
    contact_id INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_lead_gen_contacts_job ON lead_gen_contacts(job_id);
CREATE INDEX IF NOT EXISTS idx_lead_gen_contacts_status ON lead_gen_contacts(status);
CREATE INDEX IF NOT EXISTS idx_lead_gen_contacts_email ON lead_gen_contacts(email);
ALTER TABLE lead_gen_jobs ADD COLUMN IF NOT EXISTS approval_status TEXT DEFAULT 'pending';
ALTER TABLE lead_gen_jobs ADD COLUMN IF NOT EXISTS approved_at TIMESTAMP;
ALTER TABLE lead_gen_jobs ADD COLUMN IF NOT EXISTS approved_by INTEGER;
