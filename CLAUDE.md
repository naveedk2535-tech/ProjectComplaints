# ComplaintsWhoo - Financial Complaints Intelligence Platform

## Project Overview
SaaS platform tracking financial complaints across banks, credit unions, and fintech companies.
AI-powered analytics using Groq API. Data from CFPB, FDIC, and other federal sources.

## Tech Stack
- **Backend**: Flask 3.1.0 + SQLAlchemy + SQLite
- **Frontend**: Bootstrap 5 dark theme + Chart.js 4.x
- **AI**: Groq API (llama-3.3-70b-versatile)
- **Deployment**: PythonAnywhere (zziai39.pythonanywhere.com)
- **Repo**: GitHub (ProjectComplaints)

## Key Commands
```bash
# Local development
python wsgi.py                    # Start Flask dev server on port 5000
python load_csv.py                # Import CSV data into SQLite

# On PythonAnywhere
cd /home/zziai39/ProjectComplaints
git pull origin main
touch /var/www/zziai39_pythonanywhere_com_wsgi.py  # Reload app
```

## Architecture
- `dashboard/app.py` - Flask app factory with all routes and API endpoints
- `models/database.py` - SQLAlchemy models (User, Complaint, AICommentary, BankProfile)
- `services/analytics.py` - Query builders for all dashboard metrics
- `services/groq_ai.py` - Groq API integration with 5 analysis prompts
- `services/cfpb_api.py` - CFPB public API data fetcher
- `data/complaints.db` - SQLite database (not in git)

## Auth System
- Session-based with werkzeug password hashing
- 30-day free trial on signup, then premium via Stripe
- Admin panel at /admin for user management
- Default admin: admin@complaintswhoo.com / admin123

## Important Notes
- REST polling only (no WebSocket) - PythonAnywhere limitation
- Database at data/complaints.db (auto-created)
- .env has API keys - never commit
- Stripe integration is placeholder until keys are configured
