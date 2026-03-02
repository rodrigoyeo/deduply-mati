# Deduply v5.2 — Technical Handover Document

> **Audience:** Incoming developer taking ownership of the platform
> **Last updated:** March 2026
> **Status:** Active, deployed on Railway + Supabase

---

## Table of Contents

1. [What is Deduply](#1-what-is-deduply)
2. [Tech Stack](#2-tech-stack)
3. [Repository Structure](#3-repository-structure)
4. [Local Development Setup](#4-local-development-setup)
5. [Architecture Overview](#5-architecture-overview)
6. [Database Design](#6-database-design)
7. [Backend — FastAPI](#7-backend--fastapi)
8. [Frontend — React SPA](#8-frontend--react-spa)
9. [Data Cleaning Module](#9-data-cleaning-module)
10. [Background Jobs](#10-background-jobs)
11. [Authentication](#11-authentication)
12. [Email Verification](#12-email-verification)
13. [Webhooks](#13-webhooks)
14. [Deployment (Railway + Supabase)](#14-deployment-railway--supabase)
15. [Environment Variables](#15-environment-variables)
16. [Known Issues, Gotchas & Lessons Learned](#16-known-issues-gotchas--lessons-learned)
17. [Feature Reference](#17-feature-reference)

---

## 1. What is Deduply

Deduply is an internal cold email operations platform. Its core purpose is to help sales teams manage a large contact database, organize contacts into campaigns and outreach lists, track email performance, and maintain data quality.

### Core Use Cases

- **Import contacts** from CSV files (Apollo, LinkedIn exports, etc.)
- **Deduplicate** — detect and merge duplicate contacts automatically
- **Organize** contacts into named outreach lists and assign them to campaigns
- **Track** email send/open/reply/bounce metrics per campaign and per template
- **A/B test** email templates (variants A–E) across multiple step sequences
- **Verify** email addresses in bulk using the BulkEmailChecker API
- **Clean data** — normalize names, company names, and job titles in bulk
- **Export** filtered contacts to CSV for use in email senders (Instantly, Smartlead, etc.)

---

## 2. Tech Stack

| Layer | Technology | Version |
|-------|------------|---------|
| Frontend | React | 18 |
| Rich Text | React Quill | latest |
| Icons | Lucide React | latest |
| Backend | FastAPI + Uvicorn | Python 3.11 |
| ORM / DB Access | Raw SQL via psycopg2 / sqlite3 | — |
| Database (local) | SQLite | via `deduply.db` file |
| Database (prod) | PostgreSQL on Supabase | — |
| CSV processing | Pandas | 2.x |
| Password hashing | bcrypt (new) + SHA256 (legacy) | — |
| HTTP client | httpx | async |
| Hosting | Railway (backend + frontend) | — |
| DB hosting | Supabase | free tier |

---

## 3. Repository Structure

```
deduply-v5/
├── backend/
│   ├── main.py              # FastAPI app — all 65+ endpoints
│   ├── database.py          # SQLite/PostgreSQL abstraction layer
│   ├── data_cleaning.py     # Name, company, title cleaning logic
│   ├── requirements.txt     # Python dependencies
│   └── deduply.db           # Local SQLite file (gitignored)
├── frontend/
│   ├── src/
│   │   ├── App.jsx          # Entire React SPA (~4000 lines, single file)
│   │   └── styles/
│   │       └── app.css      # All styles (~6300 lines)
│   ├── package.json
│   └── build/               # Production build output (gitignored)
├── database/
│   └── schema.sql           # PostgreSQL schema — run in Supabase SQL editor
├── migrations/              # Database migration scripts (manual)
├── CLAUDE.md                # AI assistant context file
├── DEPLOYMENT.md            # Step-by-step Railway + Supabase deployment
├── README.md                # Quick start
└── TECHNICAL_HANDOVER.md    # This file
```

### Key Architectural Decision: Single-File Frontend

The entire frontend is one file (`App.jsx`, ~4000 lines). This is intentional for this project's scale — it avoids build complexity and makes it easy to find everything in one place. The tradeoff is that the file is large. Use your editor's search (`Cmd+F`) aggressively.

---

## 4. Local Development Setup

### Prerequisites
- Python 3.11+
- Node.js 18+
- pip

### Backend

```bash
cd backend
pip install -r requirements.txt
python main.py
# Runs on http://localhost:8001
```

On first run, `init_db()` creates all SQLite tables and a default admin user:
- Email: `admin@deduply.com`
- Password: `admin123`

### Frontend

```bash
cd frontend
npm install
npm start
# Runs on http://localhost:3000
# Proxies API calls to http://localhost:8001
```

### Environment (local)

No `.env` file needed for local development. SQLite is used automatically when `DATABASE_URL` is not set.

---

## 5. Architecture Overview

```
Browser (React SPA)
       │
       │  REST API calls (Bearer token)
       ▼
FastAPI (main.py) — port 8001
       │
       ├── database.py (DatabaseConnection wrapper)
       │       ├── SQLite  ← local dev
       │       └── PostgreSQL (Supabase) ← production
       │
       ├── data_cleaning.py (pure Python, no DB)
       │
       └── background threads (import jobs, email verification)
```

### Request Flow

1. React calls `api.fetch()` with a Bearer token
2. FastAPI validates the token via `get_current_user()` (dependency injection)
3. Handler calls `get_db()` to open a `DatabaseConnection`
4. Query is executed using the abstraction layer (see section 6)
5. Connection is closed before returning
6. React receives JSON and updates state

---

## 6. Database Design

### Dual-Database Strategy

The app runs SQLite locally and PostgreSQL in production. The `database.py` module handles the conversion transparently. See [Section 16](#16-known-issues-gotchas--lessons-learned) for critical gotchas about this.

### Core Tables

#### `contacts`
The primary data table. ~40 fields covering person info, company info, LinkedIn URLs, email verification status, funnel status, and system fields.

| Field | Notes |
|-------|-------|
| `id` | Auto-increment PK |
| `first_name`, `last_name`, `email` | Core identity |
| `title`, `headline`, `company` | Professional info |
| `seniority` | Director / VP / C-Level / etc. |
| `employees`, `employee_bucket` | Company size (raw + bucketed: "1-10", "11-50", etc.) |
| `domain` | Company domain, used for deduplication and data cleaning hints |
| `keywords` | Free-text keyword tags, comma-separated |
| `company_country`, `country_strategy` | Country of HQ vs internal strategy label |
| `status` | Funnel stage (see below) |
| `email_status` | Email verification result |
| `is_duplicate` | Boolean flag — duplicates are hidden, not deleted |
| `duplicate_of` | FK to the primary contact record |
| `source_file` | CSV filename used during import |

**Contact statuses (funnel stages):**
`Lead` → `Contacted` → `Replied` → `Scheduled` → `Show` / `No-Show` → `Qualified` → `Client`
Negative: `Not Interested`, `Bounced`, `Unsubscribed`

#### `campaigns`
Each campaign has denormalized metrics (sent, opened, replied, bounced, opportunities, meetings) that are updated via webhook events and manual edits. Rates (open_rate, reply_rate) are recalculated via `recalc_rates()` whenever metrics change.

#### `email_templates`
Templates belong to one or more campaigns via the `template_campaigns` junction table.

| Field | Notes |
|-------|-------|
| `variant` | A / B / C / D / E — for A/B testing |
| `step_type` | Main / Step 1 / Step 2 / Step 3 / Follow-up |
| `is_winner` | Boolean flag set manually |
| `times_sent`, `times_opened`, `times_replied` | Aggregate metrics (template-level only, not per-campaign) |

#### `template_campaigns` (junction)
Links templates to campaigns. **This table carries per-campaign metrics** — this is important. When a template is used in multiple campaigns, each campaign has its own row with its own sent/opened/replied counts.

```
template_campaigns
├── template_id
├── campaign_id
├── times_sent       ← per-campaign metric
├── times_opened     ← per-campaign metric
├── times_replied    ← per-campaign metric
├── opportunities    ← per-campaign metric
└── meetings         ← per-campaign metric
```

**Critical:** Never DELETE and re-INSERT rows in this table when updating campaign assignments — that wipes the metrics. Use the set-difference pattern (see backend code around line 2046).

#### `contact_campaigns` and `contact_lists` (junction tables)
Many-to-many relationships between contacts and campaigns/lists. These replaced the legacy denormalized `campaigns_assigned TEXT` and `outreach_lists TEXT` columns that still exist in the SQLite schema for backwards compatibility.

When you read a contact, `enrich_contact_with_relations()` adds the `campaigns_assigned` and `outreach_lists` fields by querying these junction tables.

#### `outreach_lists`
Named groups of contacts. `contact_count` is denormalized and updated via `update_counts()` after any bulk operation.

#### `verification_jobs` and `import_jobs`
Track long-running background operations. Each job has status (`pending` / `running` / `completed` / `failed` / `cancelled`) and progress counters. The frontend polls these every 2 seconds while active.

#### `settings`
Key-value store for API keys (e.g., BulkEmailChecker API key). Accessed via `/api/settings`.

#### `webhook_events`
Log of all incoming webhook payloads (from Instantly, Smartlead, etc.). Stored verbatim for debugging.

### Schema for Production (PostgreSQL)

Run `database/schema.sql` once in the Supabase SQL editor. It creates all tables, indexes, and a default admin user. This file is the source of truth for the production schema.

**Do NOT run the SQLite `CREATE TABLE` statements from `database.py` against PostgreSQL.** They have different syntax (AUTOINCREMENT vs SERIAL, etc.).

### Migrations

There is no migration framework. New columns are added via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` wrapped in try/except blocks in `database.py`'s `init_db()`. For production, run equivalent `ALTER TABLE` statements manually in Supabase SQL editor.

---

## 7. Backend — FastAPI

### `database.py` — The Abstraction Layer

**`get_db()`** — Opens a connection and returns a `DatabaseConnection` wrapper.

**`DatabaseConnection.execute(query, params)`** — Executes a query after doing these conversions for PostgreSQL:
- `?` → `%s` (parameter placeholders)
- `INSERT OR IGNORE` → `INSERT ... ON CONFLICT DO NOTHING`
- Boolean column comparisons: `is_duplicate=0` → `is_duplicate=FALSE`
- `GROUP_CONCAT(col)` → `STRING_AGG(col::text, ',')`
- `last_insert_rowid()` → `LASTVAL()`

**Critical limitation:** The `?` to `%s` conversion only works on literal `?` characters in the query string. If you build a query like:

```python
# DANGER: f-string with IN clause
placeholders = ','.join(['?'] * len(ids))
query = f"SELECT * FROM contacts WHERE id IN ({placeholders})"
conn.execute(query, ids)  # Works on SQLite, BREAKS on PostgreSQL
```

This is because the `?` placeholders are inside the f-string and the conversion happens after, but the conversion replaces ALL `?` with `%s` — so it should actually work. However, the actual crash was caused by a different issue: IN clauses with dynamic placeholder counts. The safe pattern that always works is N+1 queries (one query per item) rather than trying to use `IN (?, ?, ?)`.

**Always use `conn.close()` after each request.** There is no connection pooling — each API call opens and closes a new connection.

### `main.py` — API Endpoints

All endpoints are in a single file. The pattern is:

```python
@app.get("/api/resource")
def handler(param: str = Query(None)):
    conn = get_db()
    # ... query ...
    conn.close()
    return result
```

#### Endpoint Map

| Category | Endpoints |
|----------|-----------|
| Auth | `POST /api/auth/login`, `GET /api/auth/me`, `POST /api/auth/register`, `POST /api/auth/change-password` |
| Users | `GET /api/users`, `DELETE /api/users/{id}` |
| Contacts | `GET /api/contacts`, `POST /api/contacts`, `GET /api/contacts/{id}`, `PUT /api/contacts/{id}`, `DELETE /api/contacts/{id}` |
| Contacts | `GET /api/contacts/export` (CSV download), `GET /api/contacts/columns` |
| Bulk | `POST /api/contacts/bulk` |
| Import | `POST /api/import/upload`, `POST /api/import/start`, `GET /api/import/job/{id}`, `GET /api/import/jobs/active`, `POST /api/import/cancel/{id}` |
| Duplicates | `GET /api/duplicates`, `POST /api/duplicates/detect`, `POST /api/duplicates/merge`, `POST /api/duplicates/unmerge/{id}` |
| Campaigns | `GET /api/campaigns`, `POST /api/campaigns`, `GET /api/campaigns/{id}`, `PUT /api/campaigns/{id}`, `DELETE /api/campaigns/{id}` |
| Templates | `GET /api/templates`, `POST /api/templates`, `GET /api/templates/{id}`, `PUT /api/templates/{id}`, `DELETE /api/templates/{id}`, `GET /api/templates/grouped/by-step` |
| Lists | `GET /api/lists`, `POST /api/lists`, `PUT /api/lists/{id}`, `DELETE /api/lists/{id}` |
| Stats | `GET /api/stats`, `GET /api/stats/funnel`, `GET /api/stats/database`, `GET /api/stats/performance` |
| Filters | `GET /api/filters` |
| Data Cleaning | `POST /api/cleaning/names/preview`, `POST /api/cleaning/names/apply`, `POST /api/cleaning/companies/preview`, `POST /api/cleaning/companies/apply`, `POST /api/cleaning/titles/preview`, `POST /api/cleaning/titles/apply`, `POST /api/cleaning/titles/apply-all` |
| Verification | `POST /api/verify/start`, `GET /api/verify/status/{id}`, `POST /api/verify/cancel/{id}` |
| Webhooks | `POST /api/webhooks/instantly`, `POST /api/webhooks/smartlead`, `GET /api/webhooks/events` |
| Settings | `GET /api/settings`, `PUT /api/settings` |
| Health | `GET /api/health` |

#### Bulk Update Endpoint (`POST /api/contacts/bulk`)

This is one of the most complex endpoints. It accepts:

```json
{
  "contact_ids": [1, 2, 3],        // OR use filters
  "filters": { "status": "Lead", "search": "..." },
  "field": "status",               // Field to update
  "value": "Contacted",            // New value
  "action": "set",                 // "set", "add", "remove"
  "select_limit": 5000             // Optional: cap how many contacts to affect
}
```

When `filters` is provided (Select All mode), it rebuilds the same WHERE clause as `GET /api/contacts` and applies changes to all matching contacts, with an optional `LIMIT` applied before the update.

Supported fields: `status`, `country_strategy`, `outreach_lists`, `campaigns_assigned`, `notes`, `email_status`, plus `delete` as a special action.

For `outreach_lists` and `campaigns_assigned`:
- `action: "add"` — adds to existing without replacing
- `action: "remove"` — removes one list/campaign
- `action: "set"` — replaces all

#### Template Campaign Update (Critical Pattern)

When updating a template's campaign assignments, **never delete all rows and re-insert**. That destroys per-campaign metrics. Use the set-difference approach:

```python
existing_ids = set of current campaign_ids from DB
new_ids = set of incoming campaign_ids

to_remove = existing_ids - new_ids  # Only delete these
to_add = new_ids - existing_ids     # Only insert these
```

This preserves metrics for campaigns that remain assigned.

#### Helper Functions

- **`enrich_contact_with_relations(conn, contact_dict)`** — Adds `campaigns_assigned` and `outreach_lists` by querying junction tables. Called for every contact returned in list/detail views.
- **`update_counts()`** — Recomputes `total_leads` for campaigns and `contact_count` for lists from junction tables. Called after any bulk operation.
- **`recalc_rates(campaign_id)`** — Recomputes open/click/reply rates from raw counts.
- **`recalc_template_rates(template_id)`** — Aggregates metrics from all `template_campaigns` rows for a template.
- **`compute_employee_bucket(n)`** — Converts raw employee count to size bucket string.

---

## 8. Frontend — React SPA

### Structure

Everything is in `src/App.jsx`. Components are defined as named functions and used inline. The app is a single-page application using hash-based routing.

### Routing (Hash-Based)

Pages are driven by the `page` state variable. The hash in the URL (`#contacts`, `#campaigns`, etc.) is synced on mount and on page change, so refresh preserves the current page.

```javascript
const validPages = ['dashboard', 'contacts', 'duplicates', 'enrichment', 'campaigns', 'templates', 'settings'];
const getPageFromHash = () => {
  const hash = window.location.hash.slice(1);
  return validPages.includes(hash) ? hash : 'contacts';
};
const [page, setPage] = useState(getPageFromHash);
// On page change: window.location.hash = page;
```

### Global Contexts

| Context | Purpose |
|---------|---------|
| `ToastContext` | Toast notifications (success/error/info). Use `addToast(msg, type)`. |
| `ImportJobContext` | Tracks active background import job. Polls every 2s when active. Displayed as a progress indicator in the sidebar. |

### `useData(endpoint)` Hook

A simple fetch-on-mount hook. Returns `{ data, loading, refetch }`. Used for static/rarely-changing data like filter options.

```javascript
const { data: filterOptions } = useData('/filters');
```

### `api` Object

All API calls go through this singleton:

```javascript
const api = {
  token: localStorage.getItem('deduply_token'),
  setToken(t) { ... },
  async fetch(endpoint, options) { ... },
  get: (e) => api.fetch(e),
  post: (e, d) => api.fetch(e, { method: 'POST', body: JSON.stringify(d) }),
  put: (e, d) => api.fetch(e, { method: 'PUT', body: JSON.stringify(d) }),
  delete: (e) => api.fetch(e, { method: 'DELETE' }),
};
```

The base URL is `process.env.REACT_APP_API_URL || 'http://localhost:8001'`.

### Page Components

| Component | File Location (approx. line) | Description |
|-----------|------------------------------|-------------|
| `DashboardPage` | ~1260 | Overview stats + charts |
| `ContactsPage` | ~710 | Main contacts table with filtering, sorting, bulk actions, import |
| `DuplicatesPage` | ~1450 | Shows detected duplicate groups for review/merge |
| `EnrichmentPage` | ~1600 | Data cleaning tabs (Names, Companies, Titles) |
| `CampaignsPage` | ~2200 | Campaign list + detail view with template breakdown |
| `TemplatesPage` | ~2600 | Template library with list and grouped views |
| `SettingsPage` | ~3700 | User management, API keys, webhook config |

### ContactsPage — Key State

```javascript
const [contacts, setContacts] = useState([]);
const [total, setTotal] = useState(0);
const [page, setPage] = useState(1);
const [search, setSearch] = useState('');
const [filters, setFilters] = useState({});      // Active filter values
const [selected, setSelected] = useState(new Set());  // Checked row IDs
const [selectAll, setSelectAll] = useState(false);    // "Select all matching" mode
const [selectLimit, setSelectLimit] = useState('');   // Optional cap on selectAll
const [bulkField, setBulkField] = useState('');
const [bulkValue, setBulkValue] = useState('');
const [bulkAction, setBulkAction] = useState('set');  // set/add/remove
```

**Select All vs Select All + Limit:**
- `selectAll=true, selectLimit=''` → bulk affects all matching contacts (sends `filters` to backend)
- `selectAll=true, selectLimit='5000'` → bulk affects first 5000 of matching contacts (sends `filters + select_limit` to backend)
- `selectAll=false` → bulk affects only `selected` set (sends `contact_ids` to backend)

### Templates — Two Views

The templates page supports two views:
- **List view** — flat table of all templates with filter controls
- **Grouped view** — grouped by step type (Main, Step 1, Step 2, etc.) showing variants side by side

Both views apply the same set of filters: search text, step, variant, campaign, and country strategy. The grouped view does its own client-side filtering on the data it receives — it does NOT re-fetch filtered data from the backend.

### Template Country Filter

Templates don't have a `country` field on their own record (well, they do, but it's rarely used). The country filter on templates works by checking the countries of the **campaigns** the template is assigned to, not the template's own `country` field.

```javascript
// In filter logic:
const countryMatch = !countryFilter || t.campaigns?.some(c => c.country === countryFilter);
```

### Hover Preview (TemplatePreviewTooltip)

When hovering over a template name in the campaign detail view, a tooltip appears after a 300ms delay showing the subject and HTML body preview. It uses `dangerouslySetInnerHTML` on the body preview — this is safe because the content is user-generated in a controlled internal app.

### CSS / Styling

All styles are in `src/styles/app.css` (~6300 lines). The color scheme uses CSS variables:

```css
--coral: #FF6C5D         /* Primary accent */
--teal: #00C5A1          /* Secondary accent */
--blue: #4A90E2          /* Info/links */
--purple: #9B59B6        /* Tags/badges */
--coral-light: rgba(255, 108, 93, 0.1)
```

No CSS framework is used — everything is hand-written. Class naming is mostly BEM-like.

---

## 9. Data Cleaning Module

**File:** `backend/data_cleaning.py`

Three cleaning functions, each with a preview counterpart:

### `clean_name(name, preserve_case_if_mixed=True)`
- Converts ALL CAPS to Title Case
- Handles name particles (van, von, de, del, etc.) — kept lowercase in middle of name
- Handles special prefixes: McDonald, O'Brien, D'Angelo
- If `preserve_case_if_mixed=True` (default), leaves already mixed-case names alone
- Preview uses `preserve_case_if_mixed=False` to show what would change

### `clean_company_name(company, domain=None)`
- Removes business suffixes: Inc., LLC, Corp., Ltd., S.A., S.A. de C.V., etc.
- Handles parenthetical patterns: `"Company Name (ACRONYM)"` → uses domain to pick the right form
- Returns a `(cleaned_name, reason)` tuple — if `reason` is None, no change was made

### `clean_title(title)`
- Strips/collapses whitespace
- Capitalizes each word
- Keeps known acronyms in UPPERCASE: CEO, CFO, CTO, VP, SVP, HR, IT, AI, HVAC, BDR, SDR, etc.
- Keeps Spanish/English connectors lowercase (de, del, y, of, and, the, for, etc.) when not at start
- Handles slash-separated titles: `"Director/Gerente"` → `"Director/Gerente"`
- Handles hyphenated titles: `"Vice-President"` → `"Vice-President"`

### API Workflow (Preview → Apply)

1. `POST /api/cleaning/{type}/preview` — returns list of contacts that would change
2. User reviews and selects which changes to apply
3. `POST /api/cleaning/{type}/apply` — applies selected contact IDs
4. `POST /api/cleaning/{type}/apply-all` — applies all changes (no selection needed)

---

## 10. Background Jobs

### Import Jobs

CSV import is non-blocking. The flow:

1. `POST /api/import/upload` — saves the CSV to a temp directory, returns column headers for mapping
2. `POST /api/import/start` — creates a `import_jobs` DB record, spawns a background thread
3. Background thread reads CSV row by row, inserts/updates contacts, and updates `import_jobs.processed_count`
4. `GET /api/import/job/{id}` — frontend polls this every 2s via `ImportJobContext`
5. Progress shown in a sidebar badge

**Duplicate detection during import:**
- When `check_duplicates=true`, each imported contact is checked against existing contacts by email
- When `merge_duplicates=true`, a match causes the existing record to be updated rather than creating a duplicate

### Verification Jobs

Email verification via BulkEmailChecker API:

1. `POST /api/verify/start` — creates a `verification_jobs` record, spawns background thread
2. Thread iterates through contacts, calls BulkEmailChecker API per email (rate-limited)
3. Updates `contacts.email_status` with result: `Valid`, `Invalid`, `Unknown`, `Disposable`, etc.
4. `GET /api/verify/status/{id}` — frontend polls for progress
5. `POST /api/verify/cancel/{id}` — sets a cancellation flag the thread checks

---

## 11. Authentication

### Token-Based Auth

- Each user has an `api_token` (random URL-safe string) stored in the `users` table
- Login endpoint returns the token, which the frontend stores in `localStorage` as `deduply_token`
- All protected endpoints read `Authorization: Bearer {token}` header
- The `get_current_user()` FastAPI dependency validates the token and returns the user dict

### Password Hashing

Dual-mode for backwards compatibility:
- **New passwords:** bcrypt (`$2b$` prefix)
- **Legacy passwords:** SHA256 hex string (from early versions)

`verify_password()` detects which format is stored and uses the appropriate comparison. New registrations always use bcrypt.

### Default Admin

Created automatically on first run:
- Email: `admin@deduply.com` (SQLite) / `admin@deduply.io` (PostgreSQL)
- Password: `admin123`
- **Change this immediately in production.**

---

## 12. Email Verification

**Integration:** BulkEmailChecker (`bulkemailchecker.com`)

The API key is stored in the `settings` table. Set it via the Settings page in the UI.

**Verification results stored on contact:**
- `email_status`: `Valid` / `Invalid` / `Unknown` / `Disposable` / `Role Account` / `Free Service`
- `email_verified_at`: Timestamp
- `email_verification_event`: Raw event string from the API
- `email_is_disposable`: Boolean
- `email_is_free_service`: Boolean
- `email_is_role_account`: Boolean
- `email_suggested`: Suggested correction (when API provides one)

**Webhook alternative:** Instantly and Smartlead send bounce/open/reply events via webhook. These update contact email status automatically.

---

## 13. Webhooks

Two webhook endpoints are supported:

- `POST /api/webhooks/instantly` — receives events from Instantly.ai
- `POST /api/webhooks/smartlead` — receives events from Smartlead.ai

Both endpoints:
1. Log the raw payload to `webhook_events` table
2. Parse the event type (open, reply, bounce, click)
3. Look up the contact by email
4. Update `contacts.email_status` for bounces
5. Update `campaigns` metrics (emails_opened, emails_replied, etc.)
6. Trigger `recalc_rates()` for the affected campaign

Webhook URLs to configure in your email sender:
- `https://your-backend.railway.app/api/webhooks/instantly`
- `https://your-backend.railway.app/api/webhooks/smartlead`

All received events are visible in Settings → Webhook Events.

---

## 14. Deployment (Railway + Supabase)

### Infrastructure

| Service | Provider | Cost |
|---------|----------|------|
| Backend (FastAPI) | Railway | ~$5-10/mo |
| Frontend (React) | Railway | ~$5-10/mo |
| Database (PostgreSQL) | Supabase | Free tier (500MB) |

### Initial Setup

1. Create a Supabase project
2. Run `database/schema.sql` in Supabase SQL Editor
3. Copy the Supabase connection string (Settings → Database → URI)
4. Push code to GitHub
5. Create two Railway services from the repo:
   - **Backend:** Root directory = `backend`, Start command = `uvicorn main:app --host 0.0.0.0 --port $PORT`
   - **Frontend:** Root directory = `frontend`
6. Set environment variables (see next section)
7. Generate domains for both services in Railway

### Deployment Flow

Push to `main` → Railway auto-deploys both services within ~2 minutes.

### IPv4 Hack (Important)

Railway does not support outbound IPv6 connections. Supabase's connection string resolves to an IPv6 address by default. `database.py` patches `socket.getaddrinfo` to force IPv4 resolution:

```python
def ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return original_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)

socket.getaddrinfo = ipv4_only_getaddrinfo
```

This patch is applied before every `psycopg2.connect()` call and restored immediately after. **Do not remove this.** If Railway adds IPv6 support in the future, you can remove the patch.

---

## 15. Environment Variables

### Backend (Railway)

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Production only | Supabase PostgreSQL connection string: `postgresql://postgres:[password]@db.[ref].supabase.co:5432/postgres` |
| `ENVIRONMENT` | Optional | `production` or `development` |
| `DATABASE_PATH` | Optional | SQLite file path (local only, default: `deduply.db`) |

If `DATABASE_URL` is not set, the app uses SQLite automatically.

### Frontend (Railway)

| Variable | Required | Description |
|----------|----------|-------------|
| `REACT_APP_API_URL` | Yes (production) | Backend Railway URL, e.g. `https://deduply-backend-xxx.railway.app` |

In local development, the frontend defaults to `http://localhost:8001`.

---

## 16. Known Issues, Gotchas & Lessons Learned

### 1. PostgreSQL Placeholder Conversion — The IN Clause Problem

**Problem:** The `DatabaseConnection.execute()` method converts `?` to `%s` for PostgreSQL. However, if you build a query using an f-string with dynamic IN clause placeholders:

```python
# This looks like it should work...
ph = ','.join(['?'] * len(ids))
query = f"SELECT * FROM x WHERE id IN ({ph})"
conn.execute(query, ids)
```

...it will actually work *most* of the time because the `?` replacement is global. But if the query has any other quirks (boolean columns, etc.), the combined transformations can produce malformed SQL and crash the app with a `500` error.

**Solution:** Avoid dynamic IN clauses when possible. Use N+1 queries (one query per item) for small lists. This is what the template-fetching code does — it loops over each template and fetches its campaign data individually rather than joining or using `IN`.

### 2. Template-Campaign Metrics Wipe Bug (Fixed)

**The bug:** When updating which campaigns a template is assigned to, the original code did:

```python
conn.execute("DELETE FROM template_campaigns WHERE template_id=?", (template_id,))
# Then re-insert all campaign assignments
```

This wiped all per-campaign metrics (sent/opened/replied counts) every time you edited a template.

**The fix:** Use a set-difference approach — only delete removed associations, only insert new ones. Never touch rows that aren't changing. This preserves all existing metrics.

### 3. URL Routing and Page Refresh

**The bug:** Refreshing any page always landed on the contacts page because `useState('contacts')` always initialized to contacts.

**The fix:** Read the URL hash on initialization:

```javascript
const [page, setPage] = useState(getPageFromHash);
// Also sync hash when page changes
useEffect(() => { window.location.hash = page; }, [page]);
```

### 4. Grouped View Filter Bypass

**The bug:** The template list view had filter controls (search, step, variant, campaign, country). Switching to grouped view bypassed all filters — it displayed raw unfiltered data.

**The fix:** The grouped view does its own client-side filter pass using the same filter state variables. There's no second API call — filtering is done in JavaScript after the data is fetched.

### 5. Contact Denormalized Fields vs Junction Tables

The SQLite local schema still has `campaigns_assigned TEXT` and `outreach_lists TEXT` columns on the `contacts` table. These are legacy — they are not the source of truth in production. The junction tables (`contact_campaigns`, `contact_lists`) are the source of truth everywhere.

The `enrich_contact_with_relations()` function always overwrites the contact dict's `campaigns_assigned` and `outreach_lists` from the junction tables before returning. Do not rely on the text columns for anything.

### 6. Contacts Schema Mismatch Between SQLite and PostgreSQL

The SQLite schema (in `database.py`) and the PostgreSQL schema (in `database/schema.sql`) have drifted slightly over versions. The PostgreSQL schema is the authoritative, clean version. SQLite has some extra columns from early versions and some that were added via `ALTER TABLE` migrations.

When adding new columns:
- Add `ALTER TABLE` in `database.py`'s `init_db()` wrapped in try/except (for SQLite local dev)
- Run the equivalent `ALTER TABLE` manually in Supabase SQL Editor (for production)

### 7. Password Hash Migration

Early versions used SHA256 for password hashing. The `verify_password()` function supports both SHA256 and bcrypt. New users registered via the UI use bcrypt. Old users keep working with their SHA256 hash until they change their password.

### 8. Template `country` Field

Templates have a `country` column added via migration. However, the template country filter in the UI works by looking at the countries of the campaigns the template is assigned to — not the template's own `country` field. The template `country` field exists but is mostly unused by the filter logic.

### 9. File Comparison Between Local Dev and Production

In local dev (SQLite), all contacts table columns exist from the initial `CREATE TABLE`. In production (PostgreSQL), columns added after the initial schema was deployed were added via `ALTER TABLE`. If you see a column working locally but missing in production, check that you ran the corresponding `ALTER TABLE` in Supabase.

---

## 17. Feature Reference

### Data Cleaning (Enrichment Tab)

Three cleaning operations available:

| Operation | Preview Endpoint | Apply Endpoint |
|-----------|-----------------|----------------|
| Names | `POST /api/cleaning/names/preview` | `POST /api/cleaning/names/apply` |
| Companies | `POST /api/cleaning/companies/preview` | `POST /api/cleaning/companies/apply` |
| Titles | `POST /api/cleaning/titles/preview` | `POST /api/cleaning/titles/apply` |

The UI shows a before/after table, allows the user to check/uncheck individual rows, and apply only selected or apply all.

### CSV Import

1. Upload CSV → get column headers back
2. Map your CSV columns to Deduply's fields (or use auto-detection)
3. Set options: outreach list, campaign assignment, country strategy, duplicate handling
4. Start import → runs in background with progress bar in sidebar
5. Duplicate detection: if an email matches an existing contact and merge is enabled, the existing contact is updated rather than creating a new one

### Duplicate Detection

`POST /api/duplicates/detect` — scans the contacts table for:
- Same email address (exact match)
- Same company domain (fuzzy, configurable threshold)

Found duplicates are grouped. One contact is designated as "primary." Duplicates are marked `is_duplicate=1` and hidden from all regular queries.

`POST /api/duplicates/merge` — merges one or more duplicate contacts into a primary contact. Copies non-null fields from duplicates to primary if primary field is empty.

`POST /api/duplicates/unmerge/{id}` — restores a duplicate contact to active status.

### Bulk Actions

From the contacts table, select contacts (individually or "select all matching filters") and:
- Update `status`, `country_strategy`, `email_status`, `notes`
- Add/remove/set `outreach_lists` and `campaigns_assigned` (including creating new lists on the fly)
- Delete contacts permanently

When "Select All" is active, an optional "Limit to N contacts" input constrains how many contacts are actually updated.

### Contact Export

`GET /api/contacts/export` — streams a CSV file. Accepts the same filter parameters as `GET /api/contacts`. An additional `columns` parameter controls which fields are included. Also supports `valid_emails_only=true` to export only contacts with `email_status='Valid'`.

### Sales Funnel

The funnel view (`GET /api/stats/funnel`) tracks contacts through:

```
Lead → Contacted → Replied → Scheduled → Show → Qualified → Client
```

Each stage count is queried directly from the `contacts.status` column. Conversion rates between stages are calculated in the endpoint (not stored).

---

*End of technical handover document.*
