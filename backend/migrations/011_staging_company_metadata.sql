-- Migration 011: Add company metadata to lead_gen_contacts staging table
-- Captures industry, city, state, employee bucket from BlitzAPI company data
-- Run in Supabase SQL Editor

ALTER TABLE lead_gen_contacts ADD COLUMN IF NOT EXISTS industry TEXT;
ALTER TABLE lead_gen_contacts ADD COLUMN IF NOT EXISTS company_city TEXT;
ALTER TABLE lead_gen_contacts ADD COLUMN IF NOT EXISTS company_state TEXT;
ALTER TABLE lead_gen_contacts ADD COLUMN IF NOT EXISTS employee_bucket TEXT;
ALTER TABLE lead_gen_contacts ADD COLUMN IF NOT EXISTS icp_tier INTEGER;
ALTER TABLE lead_gen_contacts ADD COLUMN IF NOT EXISTS blitz_company_linkedin TEXT;
ALTER TABLE lead_gen_contacts ADD COLUMN IF NOT EXISTS blitz_person_linkedin TEXT;
