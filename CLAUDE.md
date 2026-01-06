# Deduply v5.2 - Cold Email Operations Platform

## Overview
Deduply is a full-featured cold email operations platform for managing contacts, campaigns, and email templates. It enables teams to build, track, and optimize outreach campaigns with features for duplicate detection, email verification, and campaign analytics.

## Tech Stack

| Layer | Technology |
|-------|------------|
| Frontend | React 18, React Quill (rich text), Lucide Icons |
| Backend | FastAPI (Python 3.11), Uvicorn |
| Database | SQLite (local dev) / PostgreSQL (Supabase production) |
| Deployment | Railway + Supabase |

## Project Structure
```
deduply-v5/
├── backend/           # FastAPI server (65+ endpoints)
│   ├── main.py        # Main application
│   ├── database.py    # SQLite/PostgreSQL abstraction layer
│   └── data_cleaning.py
├── frontend/          # React SPA
│   ├── src/App.jsx    # Main component
│   └── src/styles/app.css
├── database/
│   └── schema.sql     # PostgreSQL schema for Supabase
└── migrations/        # Database migrations
```

## Common Commands

### Backend
- `cd backend && python main.py` - Start backend server (port 8001)
- `pip install -r backend/requirements.txt` - Install Python dependencies

### Frontend
- `cd frontend && npm start` - Start React dev server (port 3000)
- `cd frontend && npm run build` - Build for production
- `npm install` - Install Node dependencies

### Database
- Local: SQLite file at `backend/deduply.db`
- Production: PostgreSQL via Supabase (see DEPLOYMENT.md)

## Code Style

### Python (Backend)
- Use 4-space indentation
- Follow PEP 8 conventions
- Use type hints where practical
- Use parameterized queries (prevent SQL injection)
- Database abstraction: Use `DatabaseConnection` class for SQLite/PostgreSQL compatibility

### JavaScript/React (Frontend)
- Use 2-space indentation
- Functional components with hooks
- State management via React Context API
- API calls via custom fetch wrapper with Bearer token auth

## Git Workflow
- Branch: `main` is the primary branch
- Remote: `origin` -> `https://github.com/rodrigoyeo/deduply-mati.git`
- Commit format: `type: description`
  - Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`
- Always run tests/build before committing

## API Conventions
- Base URL: `/api/`
- Auth: Bearer token in `Authorization` header
- Response format: JSON
- Error handling: Return appropriate HTTP status codes with error messages

## Key Features
1. **Contact Management** - Import CSV, 40+ fields, bulk operations
2. **Duplicate Detection** - Email/company matching, merge/unmerge
3. **Email Verification** - BulkEmailChecker integration, background jobs
4. **Background Import** - Non-blocking CSV import with progress tracking in sidebar
5. **Campaigns** - Track opens, clicks, replies, bounces
6. **Email Templates** - A/B testing (variants A-D), rich text editor, performance analytics
7. **Data Cleaning** - Name/company normalization
8. **Analytics** - Dashboard with charts and insights
9. **Sales Funnel** - Track contacts through Lead → Contacted → Replied → Scheduled → Show → Qualified → Client

## Contact Statuses (Funnel Stages)
- Lead (default)
- Contacted
- Replied
- Scheduled
- Show / No-Show
- Qualified
- Client
- Not Interested / Bounced / Unsubscribed

## Database Abstraction
The `database.py` module provides SQLite/PostgreSQL compatibility:
- Automatically converts `?` to `%s` for PostgreSQL
- Converts `GROUP_CONCAT` to `STRING_AGG`
- Handles boolean differences between databases
- Use `DatabaseConnection` context manager for all queries

## Security
- Never commit `.env` files or API keys
- Passwords hashed with bcrypt
- Use parameterized queries (no string concatenation in SQL)
- Validate all user inputs

## Environment Variables
See `.env.example` for required variables:
- `DATABASE_URL` - PostgreSQL connection (production only)
- `ENVIRONMENT` - `development` or `production`

## Documentation
- @README.md - Project overview and quick start
- @DEPLOYMENT.md - Full deployment guide for Railway + Supabase
- @.env.example - Environment variables template
