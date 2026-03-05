# AgriCapture Deere Connector

A FastAPI-based system that connects to John Deere Operations Center (JDOC), synchronizes organizations, fields, and field operations into a local SQLite database, and exposes an admin dashboard and CSV exports for analytics.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Environment & Dependencies](#environment--dependencies)
- [Configuration (.env)](#configuration-env)
- [Installation & Setup](#installation--setup)
- [Running the Application](#running-the-application)
- [Farmer Onboarding Flow](#farmer-onboarding-flow)
- [Admin Dashboard](#admin-dashboard)
- [Data Model](#data-model)
- [Sync & Cron Jobs](#sync--cron-jobs)
- [Maintenance & Operations](#maintenance--operations)
- [Security Notes](#security-notes)

---

## Overview

The AgriCapture Deere Connector allows farmers and agribusinesses to securely connect their John Deere Operations Center accounts and keep their farm data synchronized into AgriCapture systems.

Key capabilities:

- OAuth2 integration with John Deere (authorization code flow).
- Sync of **organizations**, **fields** (with boundaries and area), and **operations** (raw and normalized).
- Admin dashboard to:
  - Inspect and manually trigger syncs.
  - Monitor counts and summary metrics.
  - Export core tables as CSV.
- Weekly automatic sync of all connected farmers and their organizations via a cron job on the EC2 instance.

Deployed environment (current client setup):

- **EC2 app URL:** `http://54.91.102.42:8000`
- **Landing page (farmer onboarding):** `http://54.91.102.42:8000/`
- **Admin dashboard:** `http://54.91.102.42:8000/admin`

---

## Architecture

### Components

- **Backend**
  - FastAPI application (`app/main.py`).
  - JDOC API client (`app/jdoc_api.py`) using `httpx`.
  - Database helper for SQLite (`app/database.py`).
  - Auth module for Deere OAuth flows (`app/auth.py`).

- **Frontend**
  - Static HTML/CSS/JS files served by FastAPI.
  - Landing page for farmers (`frontend/index.html` and/or `templates/landing.html`).
  - Admin dashboard (`frontend/index.html` for admin view).

- **Database**
  - SQLite database (`agricapture.db`) stored in the project root on EC2.
  - Accessed via a simple wrapper that creates tables on startup and exposes query helpers.

- **Integrations**
  - John Deere Operations Center API:
    - OAuth token exchange (authorization code → access/refresh tokens).
    - Organizations, fields, and operations endpoints.
  - AWS S3 (optional):
    - Bucket name configured in `.env` (`AWS_BUCKET_NAME`).
    - Used by helper functions in `app/s3_storage.py` for storing exports or raw JDOC payloads.

- **Scheduled Jobs**
  - `scripts/auto_sync_all_orgs.py`:
    - Loops through all farmers with tokens and their organizations.
    - Calls `/admin/sync/farmer` for each (farmer, org) pair.
  - Weekly cron on EC2 (Sunday 03:00) triggers this script.

---

## Project Structure

```text
deere-connector/
├── app/
│   ├── auth.py              # John Deere OAuth helpers
│   ├── config.py            # Settings loader (reads .env)
│   ├── database.py          # SQLite helper and schema init
│   ├── jdoc_api.py          # JDOC API client logic
│   ├── logging_config.py    # Logging setup
│   ├── main.py              # FastAPI app, routes, admin UI
│   ├── models.py            # Pydantic models / schemas
│   └── s3_storage.py        # S3 upload/list helper functions
│
├── frontend/
│   └── index.html           # Admin dashboard UI (tabs, JS)
│
├── scripts/
│   └── auto_sync_all_orgs.py  # Weekly sync script (cron)
│
├── templates/
│   └── landing.html         # Landing page template (used by root route)
│
├── logs/                    # Runtime logs (if configured)
├── agricapture.db           # SQLite database (local, not in Git)
├── Dockerfile               # Container build file (optional)
├── requirements.txt         # Python dependencies
├── README.md                # This file
└── .env                     # Environment configuration (not in Git)

##Environment & Dependencies
Python Version
Python 3.10+ (current EC2 uses 3.12 in the venv).

Core Dependencies (see requirements.txt)
Examples (not exhaustive):

fastapi

uvicorn

httpx

python-dotenv

pydantic

jinja2

sqlite3 (standard library)

boto3 (if using S3)

Install from requirements.txt:

bash
pip install -r requirements.txt




##Configuration (.env)
The application uses a .env file in the project root. Example:

text
# John Deere Application Credentials
CLIENT_ID=your-deere-client-id
CLIENT_SECRET=your-deere-client-secret

# Your Application URLs
BASE_URL=http://54.91.102.42:8000
REDIRECT_URI=http://54.91.102.42:8000/auth/callback

# Environment (sandbox or production)
ENVIRONMENT=sandbox

# Database
DATABASE_URL=sqlite:///./agricapture.db

# AWS S3
AWS_BUCKET_NAME=agricapture-deere-data
AWS_REGION=us-east-1

# Security
SECRET_KEY=your-secure-random-secret
Notes:

CLIENT_ID / CLIENT_SECRET must match the John Deere app in their developer portal.

REDIRECT_URI must exactly match the redirect URL configured with Deere.

ENVIRONMENT can be sandbox or production.

DATABASE_URL points at the SQLite database file in the project root.

SECRET_KEY is used for signing / security; treat it as sensitive.

.env must not be committed to Git (already in .gitignore).




##Installation & Setup
#1. Clone repository
bash
git clone https://github.com/KapilPushkar/deere-connector.git
cd deere-connector
#2. Create and activate virtual environment
bash
python -m venv venv
source venv/bin/activate            # Linux / macOS
# On Windows: venv\Scripts\activate
33. Install Python dependencies
bash
pip install -r requirements.txt
#4. Create .env
Create .env in the project root and fill in the correct values (see Configuration).

#5. Database initialization
On first run, the app will create all required tables in agricapture.db. No manual migration is required.

If you need a clean DB:

bash
rm agricapture.db
# Then start the app; schema will be recreated automatically.


##Running the Application
#Local development
With the venv activated:

bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
Then open:

Landing page: http://localhost:8000/

Admin dashboard: http://localhost:8000/admin

#EC2 / Production (systemd)
On the client EC2 instance, the app is managed via a systemd service (service name may be deere-connector):

bash
sudo systemctl status deere-connector
sudo systemctl restart deere-connector
sudo journalctl -u deere-connector -f   # tail logs



##Farmer Onboarding Flow
The “Connect Your Farm Data” landing page is used by farmers to link their Deere account.

Farmer opens:

text-
http://54.91.102.42:8000/
They see the landing page with:

AgriCapture branding.

Short description of what they’ll get.

Email input field (“Enter your John Deere email to connect”).

Connect John Deere Account button.

They enter their John Deere email and click the button.

The frontend calls the /auth/login?farmer_id=EMAIL endpoint, which redirects them to the official John Deere OAuth login page.

After they log in and approve the application, Deere redirects back to:

text-
/auth/callback?code=...&state=...
The backend:

Exchanges the authorization code for an access/refresh token.

Stores token data in the user_tokens table, keyed by user_id (farmer email).

Optionally creates an entry in the farmers table.

After this, the farmer is “connected” and available for sync via UI or cron.


##Admin Dashboard
Admin URL: http://54.91.102.42:8000/admin

The dashboard is a single-page app (HTML + JS) with navigation tabs and calls into the backend API.

Overview
Shows summary metrics:

Organizations count

Fields count

Total area (hectares)

Operations count

Farmers count

“Refresh from JDOC” button:

Triggers a sync for a configured farmer via /admin/sync/farmer.

Organizations
List organizations for a farmer

Input: farmer_id (email)

Calls GET /api/organizations?farmer_id=...

Displays JDOC organizations known for that user.

Sync organizations to DB

Calls POST /admin/sync/organizations?farmer_id=...

Pulls organizations from JDOC and persists them in the organizations table.

Fields
Fetch fields from JDOC (JSON)

Inputs: farmer email and organization ID.

Calls GET /api/organizations/{org_id}/fields?farmer_id=....

Displays raw JDOC JSON response (for debugging / inspection).

Download fields from DB

Uses the fields table.

Exposed via export endpoints and the Exports tab.

Operations
Fetch normalized operations

Inputs: farmer email, organization ID, field ID.

Calls backend endpoints that:

Fetch operations from JDOC.

Store raw JSON in operations_raw.

Store normalized rows in operations_normalized.

View / download operations

Admin UI and export endpoints allow JSON/CSV downloads of normalized operations.

Sync & Logs
Manual “Refresh from JDOC” button:

For a chosen farmer, calls /admin/sync/farmer without org filter, causing a full sync of all orgs/fields/operations for that farmer.

Stats:

Shows current counts for orgs, fields, operations, farmers.

“Refresh stats” reloads from DB.

Exports
Predefined CSV exports

Organizations CSV.

Fields CSV.

Operations CSV.

Download any table

Dropdown includes:

organizations

fields

operations_raw

operations_normalized

field_sync_state

connected_organizations

user_tokens

farmers

Button builds URL GET /admin/tables/{table_name}/download and streams CSV.




##Data Model
At a high level (column names simplified):

user_tokens
user_id (TEXT) – Deere user email.

access_token, refresh_token.

expires_at (timestamp).

Additional token metadata.

farmers
id or email (TEXT, primary key).

name (optional).

created_at (timestamp).

organizations
org_id (TEXT, primary key).

farmer_id (TEXT, references user_tokens.user_id).

name, type, external_id, etc.

fields
field_id (TEXT, primary key).

org_id (TEXT).

farmer_id (TEXT).

name.

external_id.

area_ha (REAL).

boundary_json (TEXT) – serialized boundary geometry.

operations_raw
id (TEXT, primary key).

field_id, org_id, farmer_id.

raw_json (TEXT) – original JDOC operation payload.

created_at.

operations_normalized
id (TEXT, primary key).

field_id, org_id, farmer_id.

operation_type.

start_time, end_time.

machine, product, etc., depending on JDOC payload.

area_ha (REAL) – operation area if provided.

field_sync_state, connected_organizations
Internal bookkeeping tables for sync progress, last sync timestamps, and mappings between farmers and orgs.




##Sync & Cron Jobs
Manual sync via admin UI
Organizations tab:

POST /admin/sync/organizations?farmer_id=...

Sync & Logs tab:

POST /admin/sync/farmer?farmer_id=... (optionally with org_id when called by scripts).

These call into jdoc_api.py functions (get_organizations, get_fields, get_operations, etc.) and write into the database.

Automated weekly sync (EC2 cron)
Script: scripts/auto_sync_all_orgs.py

Behavior:

Connects to agricapture.db.

Reads all farmers:

sql
SELECT DISTINCT user_id FROM user_tokens;
For each farmer:

Reads orgs from organizations where farmer_id = ?.

For every org, calls:

text
POST /admin/sync/farmer?farmer_id=<farmer>&org_id=<org_id>
Logs progress and results to stdout.

Cron job for user ubuntu on the client EC2:

text
# Weekly auto sync for all farmers (every Sunday at 03:00)
0 3 * * 0 cd /opt/deere-connector && . venv/bin/activate && DEERE_CONNECTOR_API_URL="http://127.0.0.1:8000" python scripts/auto_sync_all_orgs.py >> /var/log/auto_sync_all_orgs.log 2>&1
Check last run:

bash
tail -n 100 /var/log/auto_sync_all_orgs.log




##Maintenance & Operations
Reset the database
For UAT resets or clean starts:

bash
cd /opt/deere-connector
sudo systemctl stop deere-connector
rm agricapture.db
sudo systemctl start deere-connector
The app will recreate all tables on startup. You will need to:

Reconnect Deere accounts via the landing page.

Re-sync organizations/fields/operations manually or wait for the weekly cron.

Change the landing page copy
Edit:

bash
cd /opt/deere-connector
sudo nano frontend/index.html      # and/or templates/landing.html
Modify only text, branding, or layout. Do not remove the email input or the “Connect John Deere Account” JS handler.

Check basic health
bash
curl -s http://127.0.0.1:8000/health
Expected: JSON with a healthy status.

Inspect DB counts
bash
cd /opt/deere-connector
sqlite3 agricapture.db "SELECT COUNT(*) FROM organizations;"
sqlite3 agricapture.db "SELECT COUNT(*) FROM fields;"
sqlite3 agricapture.db "SELECT COUNT(*) FROM operations_normalized;"
sqlite3 agricapture.db "SELECT user_id FROM user_tokens;"




##Security Notes
Never commit .env, agricapture.db, or any private keys (*.pem) to Git. They are ignored via .gitignore.

Treat CLIENT_SECRET, SECRET_KEY, and all tokens as sensitive.

The current deployment uses HTTP on port 8000; for production, placing the app behind an HTTPS‑terminating load balancer or reverse proxy is recommended.

If a Deere user revokes access or tokens expire and cannot be refreshed, sync will fail for that farmer until they reconnect via the landing page.
