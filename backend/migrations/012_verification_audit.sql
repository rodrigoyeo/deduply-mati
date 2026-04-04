-- Migration 012: Add audit trail fields to verification_jobs
-- Adds triggered_by, triggered_from, filter_description so every job
-- has a clear record of who started it and why.

ALTER TABLE verification_jobs ADD COLUMN IF NOT EXISTS triggered_by TEXT DEFAULT 'unknown';
ALTER TABLE verification_jobs ADD COLUMN IF NOT EXISTS triggered_from TEXT DEFAULT NULL;
ALTER TABLE verification_jobs ADD COLUMN IF NOT EXISTS filter_description TEXT DEFAULT NULL;

-- Backfill existing jobs with 'hermes' for the April 1-2 batch (run-b3-verification.sh loop)
-- Jobs 104-123 were all started by Hermes's run-b3-verification.sh script
UPDATE verification_jobs
SET triggered_by = 'hermes',
    triggered_from = 'run-b3-verification.sh',
    filter_description = 'HVAC US B3 - list 101766 (auto-restart loop)'
WHERE id BETWEEN 104 AND 121;

UPDATE verification_jobs
SET triggered_by = 'hermes',
    triggered_from = 'run-b3-verification.sh (double-trigger)',
    filter_description = 'MX contacts - email_status NULL or Not Verified (17903 contacts)'
WHERE id IN (122, 123);
