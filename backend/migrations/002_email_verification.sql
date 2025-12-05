-- Migration: Email Verification Feature
-- Run this in Supabase SQL Editor

-- 1. Create verification_jobs table for background verification tracking
CREATE TABLE IF NOT EXISTS verification_jobs (
    id SERIAL PRIMARY KEY,
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
    completed_at TIMESTAMP
);

-- 2. Ensure email verification columns exist on contacts table
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS email_verified_at TIMESTAMP;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS email_verification_event TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS email_is_disposable BOOLEAN DEFAULT FALSE;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS email_is_free_service BOOLEAN DEFAULT FALSE;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS email_is_role_account BOOLEAN DEFAULT FALSE;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS email_suggested TEXT;

-- 3. Update default email_status for new contacts
ALTER TABLE contacts ALTER COLUMN email_status SET DEFAULT 'Not Verified';

-- 4. Update existing unverified contacts (those never checked via API)
-- This changes 'Unknown' to 'Not Verified' for contacts that were never actually verified
UPDATE contacts
SET email_status = 'Not Verified'
WHERE email_status = 'Unknown'
AND email_verified_at IS NULL;

-- 5. Verify the migration worked
SELECT
    'verification_jobs table' as check_item,
    CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'verification_jobs')
         THEN 'OK' ELSE 'MISSING' END as status
UNION ALL
SELECT
    'email_verified_at column',
    CASE WHEN EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'contacts' AND column_name = 'email_verified_at')
         THEN 'OK' ELSE 'MISSING' END
UNION ALL
SELECT
    'Contacts with Not Verified status',
    COUNT(*)::TEXT
FROM contacts WHERE email_status = 'Not Verified';
