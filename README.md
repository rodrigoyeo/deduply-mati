# Deduply

Cold email operations platform for managing contacts, campaigns, and email templates.

## Features

- **Contact Management**: Import, deduplicate, and organize contacts from CSV files
- **Campaign Tracking**: Track email campaigns with open/reply rates
- **Email Templates**: A/B testing with variant tracking
- **Duplicate Detection**: Automatic detection and merging of duplicate contacts
- **Outreach Lists**: Organize contacts into lists for targeted campaigns
- **Analytics Dashboard**: Visual charts for campaign performance

## Tech Stack

- **Frontend**: React 18
- **Backend**: FastAPI (Python)
- **Database**: SQLite (local) / PostgreSQL (production via Supabase)
- **Hosting**: Railway

## Local Development

```bash
# Backend
cd backend
pip install -r requirements.txt
python main.py
# Runs on http://localhost:8001

# Frontend (in another terminal)
cd frontend
npm install
npm start
# Runs on http://localhost:3000
```

## Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md) for full deployment instructions.

**Quick overview:**
1. Push code to GitHub
2. Create Supabase project and run `database/schema.sql`
3. Deploy to Railway (backend + frontend)
4. Set environment variables

## Project Structure

```
deduply/
├── backend/
│   ├── main.py          # FastAPI application
│   ├── database.py      # Database abstraction layer
│   ├── Dockerfile       # Production container
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── App.jsx      # Main React application
│   │   └── styles/
│   ├── Dockerfile       # Production container
│   └── package.json
├── database/
│   └── schema.sql       # PostgreSQL schema for Supabase
└── DEPLOYMENT.md        # Deployment guide
```

## Environment Variables

| Variable | Description | Local | Production |
|----------|-------------|-------|------------|
| `DATABASE_URL` | PostgreSQL connection string | Not set (uses SQLite) | Supabase URL |
| `REACT_APP_API_URL` | Backend API URL | http://localhost:8001 | Railway URL |

## License

Private - Internal use only
