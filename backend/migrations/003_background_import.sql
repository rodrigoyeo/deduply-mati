-- Migration: Background Import Feature
-- Version: 003
-- Description: Add import_jobs table for background CSV import processing
-- Run this in Supabase SQL Editor

-- 1. Create import_jobs table for background import tracking
CREATE TABLE IF NOT EXISTS import_jobs (
    id SERIAL PRIMARY KEY,
    status TEXT DEFAULT 'pending',  -- pending, running, completed, failed, cancelled
    total_rows INTEGER DEFAULT 0,
    processed_count INTEGER DEFAULT 0,
    imported_count INTEGER DEFAULT 0,
    merged_count INTEGER DEFAULT 0,
    duplicates_found INTEGER DEFAULT 0,
    failed_count INTEGER DEFAULT 0,
    current_row TEXT,  -- Shows current row being processed (e.g., email)
    error_message TEXT,

    -- Import configuration (stored for job processing)
    file_name TEXT,
    file_path TEXT,  -- Temp file path for the uploaded CSV
    column_mapping TEXT,  -- JSON string of column mappings
    outreach_list TEXT,
    campaigns TEXT,
    country_strategy TEXT,
    check_duplicates BOOLEAN DEFAULT TRUE,
    merge_duplicates BOOLEAN DEFAULT TRUE,

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

-- 2. Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_import_jobs_status ON import_jobs(status);
CREATE INDEX IF NOT EXISTS idx_import_jobs_created_at ON import_jobs(created_at DESC);

-- 3. Grant permissions (if using Row Level Security)
-- ALTER TABLE import_jobs ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY "Allow all for authenticated users" ON import_jobs FOR ALL USING (true);
