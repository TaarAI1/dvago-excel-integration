# RetailPro Prism Integration

A full-stack application that polls an FTP server for CSV files, stores them in MongoDB, and syncs documents to the RetailPro Prism API via a dashboard.

## Architecture

- **Backend**: FastAPI + APScheduler + Motor (async MongoDB) + Polars
- **Frontend**: React/Vite + MUI + TanStack Query + Zustand
- **Database**: MongoDB (Atlas M0 or local)
- **Deployment**: 2 Railway services (api + frontend)

## Local Development

### Prerequisites
- Docker & Docker Compose
- Node.js 20+
- Python 3.11+

### 1. Clone and configure
```bash
cp .env.example .env
# Edit .env with your FTP, MongoDB, and RetailPro credentials
```

### 2. Start with Docker Compose
```bash
docker-compose up --build
```

- Frontend: http://localhost
- Backend API docs: http://localhost:8000/docs
- Default login: `admin` / `admin123` (set in .env)

### 3. Run backend only (development)
```bash
cd backend
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

### 4. Run frontend only (development)
```bash
cd frontend
npm install
npm run dev                     # Runs on http://localhost:3000
```

### 5. Run backend tests
```bash
cd backend
pip install pytest pytest-asyncio
pytest tests/ -v
```

## Configuration

All configuration is via environment variables (see `.env.example`):

| Variable | Description |
|---|---|
| `MONGODB_URL` | MongoDB connection string |
| `FTP_HOST/PORT/USER/PASSWORD` | FTP server credentials |
| `FTP_BASE_PATH` | Directory on FTP to scan for CSV files |
| `RETAILPRO_BASE_URL` | RetailPro API base URL |
| `RETAILPRO_API_KEY` | API key / bearer token |
| `RETAILPRO_CLIENT` | `mock` (dev) or `real` (production) |
| `DOCUMENT_TYPE_ENDPOINTS` | JSON mapping of doc type → API endpoint |
| `DOCUMENT_TYPE_FIELD_MAPS` | JSON mapping of CSV columns → MongoDB fields |
| `DASHBOARD_USERNAME/PASSWORD` | Dashboard login credentials |
| `JWT_SECRET_KEY` | Secret for JWT signing (use long random string) |
| `POLL_CRON_SCHEDULE` | Cron expression for FTP polling (default: `*/15 * * * *`) |

## Document Type Detection

CSV files are matched to document types by filename pattern:
- Contains `item` or `master` → `item_master`
- Contains `receiv` or `voucher` → `receiving_voucher`
- Contains `adjust` or `inventory` → `inventory_adjustment`

Extend `infer_document_type()` in `backend/app/services/csv_processor.py` once real filenames are known.

## Adding RetailPro API Support

1. Set `RETAILPRO_CLIENT=real` in `.env`
2. Set `RETAILPRO_BASE_URL` and `RETAILPRO_API_KEY`
3. Configure `DOCUMENT_TYPE_ENDPOINTS` to map document types to your API paths
4. Configure `DOCUMENT_TYPE_FIELD_MAPS` to rename CSV columns to the expected API field names

## Railway Deployment

### Step 1: Set up MongoDB Atlas
1. Create a free cluster at [MongoDB Atlas](https://cloud.mongodb.com)
2. Whitelist all IPs (`0.0.0.0/0`) for Railway
3. Copy the connection string

### Step 2: Deploy API service
1. Create a new Railway project
2. Add a service → Deploy from GitHub repo
3. Set root directory to `/backend`
4. Set start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
5. Add all environment variables from `.env.example`

### Step 3: Deploy Frontend service
1. Add another service in the same Railway project
2. Set root directory to `/frontend`
3. Update `frontend/nginx.conf`: replace `http://api:8000` with the Railway internal URL of your api service
4. No additional env vars needed

### Step 4: Update CORS
Set `ALLOWED_ORIGINS` in the api service env to your frontend Railway URL (or keep `*` for simplicity).

## API Documentation

When the backend is running, visit `/docs` for the interactive Swagger UI covering all endpoints:
- `POST /api/auth/login` — get JWT token
- `GET /api/schedule/status` — scheduler status
- `POST /api/schedule/configure` — update cron
- `POST /api/process/trigger` — manual FTP poll
- `POST /api/process/retry/{id}` — retry failed document
- `GET /api/documents` — list with filters
- `GET /api/documents/stats` — aggregated stats
- `GET /api/logs` — activity log with filters
- `GET /api/logs/export` — download CSV/JSON
- `GET /api/stream/dashboard` — SSE live stats
- `GET /api/stream/logs` — SSE live log feed
- `GET /api/health` — health check
