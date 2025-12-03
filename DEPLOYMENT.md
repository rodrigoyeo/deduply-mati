# Deduply Deployment Guide

## Overview

This guide will help you deploy Deduply to production using:
- **Supabase** - PostgreSQL database (free tier)
- **Railway** - Backend + Frontend hosting (~$5-10/month)

**Total estimated cost: $5-10/month**

---

## Prerequisites

Before starting, create accounts on:
1. **GitHub** - https://github.com (free)
2. **Supabase** - https://supabase.com (free)
3. **Railway** - https://railway.app (free trial, then ~$5/mo)

---

## Step 1: Create GitHub Repository

1. Go to https://github.com/new
2. Name it `deduply` (or any name you prefer)
3. Keep it **Private** (recommended for internal tools)
4. Click **Create repository**
5. Follow the instructions to push your code:

```bash
cd "/Users/rodrigo/Downloads/deduply-v5 2"
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/deduply.git
git push -u origin main
```

---

## Step 2: Set Up Supabase Database

### 2.1 Create Project
1. Go to https://supabase.com and sign in
2. Click **New Project**
3. Fill in:
   - **Name**: `deduply`
   - **Database Password**: Generate a strong password and **SAVE IT**
   - **Region**: Choose closest to your team (e.g., East US, West EU)
4. Click **Create new project**
5. Wait 2-3 minutes for project to be ready

### 2.2 Get Connection String
1. In Supabase dashboard, go to **Settings** (gear icon) → **Database**
2. Scroll to **Connection string** section
3. Select **URI** tab
4. Copy the connection string - it looks like:
   ```
   postgresql://postgres:[YOUR-PASSWORD]@db.xxxxx.supabase.co:5432/postgres
   ```
5. Replace `[YOUR-PASSWORD]` with your actual database password

### 2.3 Create Database Tables
1. In Supabase, go to **SQL Editor** (left sidebar)
2. Click **New query**
3. Copy and paste the contents of `database/schema.sql` (included in this project)
4. Click **Run** (or press Cmd+Enter)
5. You should see "Success" message

---

## Step 3: Deploy to Railway

### 3.1 Connect GitHub
1. Go to https://railway.app and sign in with GitHub
2. Click **New Project**
3. Select **Deploy from GitHub repo**
4. Choose your `deduply` repository
5. Railway will detect the monorepo structure

### 3.2 Deploy Backend
1. In Railway project, click **Add Service** → **GitHub Repo**
2. Select your repo
3. Set **Root Directory**: `backend`
4. Go to **Settings** tab and set:
   - **Start Command**: `uvicorn main:app --host 0.0.0.0 --port $PORT`
5. Go to **Variables** tab and add:
   ```
   DATABASE_URL = postgresql://postgres:[PASSWORD]@db.xxxxx.supabase.co:5432/postgres
   ENVIRONMENT = production
   ```
6. Go to **Settings** → **Networking** → **Generate Domain**
7. Note your backend URL (e.g., `https://deduply-backend-xxx.railway.app`)

### 3.3 Deploy Frontend
1. Click **Add Service** → **GitHub Repo**
2. Select your repo again
3. Set **Root Directory**: `frontend`
4. Go to **Variables** tab and add:
   ```
   REACT_APP_API_URL = https://your-backend-url.railway.app
   ```
   (Use the backend URL from step 3.2.7)
5. Go to **Settings** → **Networking** → **Generate Domain**
6. Your app is now live!

---

## Step 4: Access Your App

1. Open the frontend URL from Railway (e.g., `https://deduply-frontend-xxx.railway.app`)
2. Your team can now access the app from anywhere!

---

## Local Development

To continue developing locally:

```bash
# Backend (uses SQLite locally)
cd backend
pip install -r requirements.txt
python main.py

# Frontend (in another terminal)
cd frontend
npm install
npm start
```

Local development uses SQLite so you don't affect production data.

---

## Environment Variables Reference

### Backend
| Variable | Local | Production |
|----------|-------|------------|
| `DATABASE_URL` | Not set (uses SQLite) | Supabase connection string |
| `ENVIRONMENT` | `development` | `production` |

### Frontend
| Variable | Local | Production |
|----------|-------|------------|
| `REACT_APP_API_URL` | `http://localhost:8001` | Railway backend URL |

---

## Troubleshooting

### Backend won't start
- Check DATABASE_URL is correct in Railway variables
- Check logs in Railway dashboard

### Frontend can't connect to backend
- Verify REACT_APP_API_URL is set correctly
- Make sure backend is running (check Railway logs)

### Database connection failed
- Verify password in connection string
- Check Supabase project is active

---

## Updating the App

After making changes locally:

```bash
git add .
git commit -m "Your update message"
git push
```

Railway automatically deploys when you push to GitHub!

---

## Costs

| Service | Free Tier | Paid |
|---------|-----------|------|
| Supabase | 500MB database | $25/mo for more |
| Railway | $5 credit/month | ~$5-10/mo typical |
| GitHub | Unlimited private repos | Free |

**Typical monthly cost: $5-10**
