# Deduply - Agent Onboarding

This file is for Arkode's AI agents. Read it before touching anything in this repo.

## What Is Deduply?

Deduply is Arkode's **cold email operations platform** - a full production application built and owned by the team. It is NOT just a Supabase database. It is a running web app with a React frontend and FastAPI backend.

**Live since:** ~early 2026
**Status:** Active production - Hermes runs campaigns here daily
**Recent activity:** Multiple commits per day

---

## Architecture

```
deduply-mati/
├── frontend/          React 18 SPA (contact management, campaigns, analytics UI)
├── backend/
│   ├── main.py        FastAPI backend - 65+ endpoints, ALL the business logic
│   ├── database.py    SQLite/PostgreSQL abstraction layer
│   └── data_cleaning.py  Name/company normalization
├── database/
│   └── schema.sql     PostgreSQL schema - run in Supabase SQL editor
└── CLAUDE.md          Full project context for Claude Code
```

**Local:** SQLite (`backend/deduply.db`)
**Production:** PostgreSQL via Supabase → `https://obwfyzytyzwwnvkiwwhw.supabase.co`
**Deployed on:** Railway (backend service + frontend service)

---

## Key Database Tables

| Table | Purpose | Who Cares |
|-------|---------|-----------|
| `contacts` | All leads - 40+ fields, name, company, email, location | Hermes |
| `campaigns` | Campaign tracking with open/reply/bounce metrics | Hermes |
| `email_templates` | Templates with A/B variants (A-D), performance stats | Hermes |
| `template_campaigns` | Links templates to campaigns (many-to-many) | Hermes |
| `outreach_lists` | Segmented contact lists for targeting | Hermes |
| `webhook_events` | Inbound events from ReachInbox, BulkEmailChecker | Hermes + Otto |
| `contact_campaigns` | Contact-campaign relationships | Hermes |
| `users` | Auth - admin login | Otto |

---

## Webhook Endpoints (Backend)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/webhook/reachinbox` | Verification endpoint for ReachInbox setup |
| POST | `/webhook/reachinbox` | ⚠️ BROKEN SINCE JAN 24 - receives opens/replies/bounces |
| POST | `/webhook/bulkemailchecker` | Validation results from BulkEmailChecker |
| POST | `/webhook/{source}` | Generic catch-all webhook |
| GET | `/api/webhooks` | List received webhook events |

**To check webhook health:**
```bash
# Check last events received in production Supabase
python3 -c "
import requests
URL = 'https://obwfyzytyzwwnvkiwwhw.supabase.co'
KEY = 'sb_secret_3jemmCnkoK2damnRbAwh8w_dZeuk2CH'
H = {'apikey': KEY, 'Authorization': f'Bearer {KEY}'}
r = requests.get(f'{URL}/rest/v1/webhook_events?order=created_at.desc&limit=5', headers=H)
for e in r.json(): print(e)
"
```

---

## Sales Funnel (Contact Statuses)

Lead → Contacted → Replied → Scheduled → Show/No-Show → Qualified → Client → Not Interested/Bounced/Unsubscribed

---

## Local Dev Setup

```bash
cd ~/projects/deduply-mati

# Backend
cd backend && pip install -r requirements.txt
python main.py  # runs on http://localhost:8001

# Frontend (separate terminal)
cd frontend && npm install && npm start  # runs on http://localhost:3000
```

---

## Production Deployment

Push to `main` branch → Railway auto-deploys.

```bash
git add . && git commit -m "feat: description" && git push origin main
```

Check Railway dashboard for deploy status and logs.

---

## For Otto: Coding Agent System

The repo is at `~/projects/deduply-mati`. Use the coding agent system for ALL features:

```bash
bash ~/clawd/scripts/spawn-coding-agent.sh \
  "feat-<id>" \
  "<description>" \
  "~/projects/deduply-mati" \
  "feat/<branch-name>" \
  "<prompt - include CLAUDE.md exists in repo for full context>"
```

Worktrees go at `~/projects/worktrees/feat-<branch-name>`.

**The repo's CLAUDE.md has full tech stack context** - Claude Code reads it automatically.

For any Deduply task, tell Claude Code: "Read CLAUDE.md first for full project context."

---

## For Hermes: Your Ownership

You own Deduply as the GTM Engineer. This means:
- You diagnose issues (broken webhooks, missing data, wrong metrics)
- You write simple scripts to query and analyze the data
- You spec features that improve your outreach workflow
- Otto implements complex changes via the coding agent system

**Your most important task right now:** Fix `/webhook/reachinbox` - broken since Jan 24.
The endpoint EXISTS in the code (line 2155 in main.py). Likely the URL configured in ReachInbox
is wrong or the Railway service went down. Check ReachInbox webhook settings first.
