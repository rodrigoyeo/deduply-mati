-- Migration 005: Lead Generation Tables
-- Run in Supabase SQL Editor before deploying this PR

CREATE TABLE IF NOT EXISTS lead_gen_jobs (
    id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT DEFAULT 'pending',
    parameters JSONB,
    results_count INTEGER DEFAULT 0,
    imported_count INTEGER DEFAULT 0,
    credits_used NUMERIC DEFAULT 0,
    workspace TEXT DEFAULT 'US',
    created_by INTEGER REFERENCES users(id),
    created_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    error TEXT
);

CREATE TABLE IF NOT EXISTS lead_gen_companies (
    id SERIAL PRIMARY KEY,
    job_id TEXT REFERENCES lead_gen_jobs(id),
    linkedin_url TEXT,
    linkedin_id BIGINT,
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
    raw_data JSONB,
    imported BOOLEAN DEFAULT FALSE,
    workspace TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_lead_gen_companies_job ON lead_gen_companies(job_id);
CREATE INDEX IF NOT EXISTS idx_lead_gen_companies_domain ON lead_gen_companies(domain);
CREATE INDEX IF NOT EXISTS idx_lead_gen_jobs_status ON lead_gen_jobs(status);

SELECT 'migration 005 complete' as status;
