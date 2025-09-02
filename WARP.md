# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## 🚀 Quick Start

Get MetronAuditOps running locally in under 5 minutes:

```bash
# Clone and enter repository
git clone https://github.com/your-org/MetronAuditOps.git
cd MetronAuditOps

# Set up Python environment (Python 3.11 required)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install backend dependencies
pip install -r backend/requirements.txt

# Install frontend dependencies
cd frontend
npm ci
cd ..

# Start backend (Terminal 1)
cd backend
uvicorn main:app --reload --port 8000

# Start frontend (Terminal 2)
cd frontend
npm run dev
```

Access the application:
- **Frontend**: http://localhost:5173
- **Backend API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs

## 🗂️ Repository Structure & Architecture

MetronAuditOps is a full-stack audit console for the Metron System that captures → detects → matches → verifies → reports pan/line audits with AI and automation.

### Directory Structure
```
MetronAuditOps/
├── backend/                    # FastAPI backend service
│   ├── app/
│   │   ├── api/               # API route handlers
│   │   ├── utils/             # Configuration & DynamoDB clients
│   │   ├── audit_service.py   # Core audit business logic
│   │   ├── scheduler.py       # Background job scheduler (4PM & 8PM PT)
│   │   ├── models.py          # Pydantic data models
│   │   ├── *_service.py       # External service integrations
│   │   └── ...
│   ├── main.py                # FastAPI app entry point
│   ├── requirements*.txt      # Python dependencies
│   └── Dockerfile             # Multi-stage ML container build
├── frontend/                   # React + Vite frontend
│   ├── src/
│   │   ├── pages/             # React pages (Home, Audit)
│   │   └── utils/             # Frontend utilities
│   ├── package.json
│   └── Dockerfile             # Nginx-based production build
├── audit_automation/           # AI/ML automation modules (imported by scheduler)
├── system/                     # Shared system utilities
├── CODE_QUALITY.md            # Comprehensive code quality documentation
└── .pre-commit-config.yaml    # Git hooks for quality checks
```

### Data Flow Architecture
```
S3 Buckets ──┐
             ├──→ Scheduler (4PM/8PM PT) ──→ CSV Processing ──→ AI/ML Pipeline ──┐
             │                                                                   │
             └──→ Manual Download Triggers ─────────────────────────────────────┘
                                                                                 │
                                                                                 ▼
DynamoDB Tables ◄── FastAPI Backend ◄── React Frontend ◄── Audit UI Workflows
     ▲                    ▲
     │                    │
     └─── Skoopin API ────┘
```

**Key Components:**
- **Scheduler**: Automated S3 downloads at 4:00 PM & 8:00 PM PT
- **AI Pipeline**: YOLOv8, Corner detection, GenAI for pan identification
- **Audit Service**: CRUD operations coordinating Skoopin API & DynamoDB
- **Multi-service Integration**: AWS S3, DynamoDB, Skoopin API, Redis/RQ

## 💻 Local Development

### Backend Development

```bash
# Navigate to backend directory
cd backend

# Start development server with hot reload
uvicorn main:app --reload --port 8000

# Run with custom host/port
uvicorn main:app --reload --host 0.0.0.0 --port 8080

# Start interactive Python shell with app context
python -c "from main import app; import uvicorn; uvicorn.run(app, reload=True)"
```

**Key Backend Files:**
- `main.py` - FastAPI app with CORS, lifespan events, service initialization
- `app/api/routes.py` - All API endpoints (1400+ lines of routes)
- `app/scheduler.py` - Background jobs for S3 download & AI processing
- `app/audit_service.py` - Core audit session management & CRUD operations

### Frontend Development

```bash
# Navigate to frontend directory
cd frontend

# Start development server (default port 5173)
npm run dev

# Build for production
npm run build

# Preview production build locally
npm run preview

# Lint JavaScript/TypeScript
npm run lint
```

**Frontend Stack:**
- React 19 with React Router DOM
- Vite for build tooling
- Material-UI (MUI) for components
- Axios for API communication
- ESLint for code quality

### Full-Stack Development

For concurrent development, use two terminal sessions or Warp's split panes:

```bash
# Terminal 1: Backend
cd backend && uvicorn main:app --reload --port 8000

# Terminal 2: Frontend
cd frontend && npm run dev
```

## 🧪 Testing & Code Quality

This project has comprehensive automated code quality checks. See `CODE_QUALITY.md` for full details.

### Quick Quality Checks

```bash
# Install pre-commit hooks (one-time setup)
pip install pre-commit
pre-commit install

# Run all quality checks on staged files
pre-commit run

# Run all quality checks on entire codebase
pre-commit run --all-files
```

### Backend Testing & Linting

```bash
cd backend

# Code formatting
black .
isort .

# Linting
flake8 .

# Type checking
mypy app/

# Security scanning
bandit -r app/
safety check

# Run comprehensive CRUD integration tests
python test_crud_operations.py

# Run pytest with coverage
pytest tests/ -v --cov=app --cov-report=html
# Coverage report: htmlcov/index.html
```

### Frontend Testing & Linting

```bash
cd frontend

# Lint JavaScript/TypeScript
npm run lint

# Format code (if Prettier is configured)
npm run format

# Type checking
npx tsc --noEmit

# Build test
npm run build
```

### Pre-commit Quality Gates

The repository enforces quality through pre-commit hooks:
- **Python**: Black, isort, flake8, mypy, bandit, safety
- **Frontend**: ESLint, Prettier
- **General**: Trailing whitespace, YAML validation, large file checks

## 🐳 Docker & Container Workflows

### Local Docker Development

```bash
# Build backend image (includes ML dependencies)
docker build -f backend/Dockerfile -t metron-audit-backend .

# Build frontend image (nginx-based)
docker build -f frontend/Dockerfile -t metron-audit-frontend .

# Run backend container
docker run -p 8080:8080 metron-audit-backend

# Run frontend container
docker run -p 3000:3000 metron-audit-frontend
```

### Production Container Features

**Backend Dockerfile:**
- Multi-stage build optimized for ML libraries (PyTorch, OpenCV, Ultralytics)
- Python 3.11 with system dependencies for computer vision
- Includes audit_automation & system modules for AI pipeline
- Health checks and proper signal handling

**Frontend Dockerfile:**
- Multi-stage Node.js build with nginx serving
- Optimized for production with static asset serving
- Custom nginx configuration

### Cloud Run Deployment

The project includes `backend/cloudbuild.yaml` and `frontend/cloudbuild.yaml` for Google Cloud Run deployment.

```bash
# Deploy backend to Cloud Run
gcloud run deploy metron-audit-backend --source=backend/

# Deploy frontend to Cloud Run
gcloud run deploy metron-audit-frontend --source=frontend/
```

## ⚙️ Useful Commands & Scripts

### Database Operations

```bash
# Test database connectivity
curl http://localhost:8000/api/db/ping

# Check scheduler status
curl http://localhost:8000/api/scheduler/status

# Trigger manual catch-up
curl -X POST http://localhost:8000/api/scheduler/catch-up

# Force immediate data download for a specific date
curl -X POST "http://localhost:8000/api/force_redownload" \
  -H "Content-Type: application/json" \
  -d '{"date": "2024-12-01"}'
```

### AI/ML Pipeline

```bash
# Trigger pan AI workflow for a specific date
curl -X POST "http://localhost:8000/api/pan_ai/run" \
  -H "Content-Type: application/json" \
  -d '{"date": "2024-12-01"}'

# Check AI processing status
curl "http://localhost:8000/api/pan_ai/status?date=2024-12-01"

# Test pan download functionality
curl -X POST "http://localhost:8000/api/test/pan-download?restaurant_id=157"
```

### Development Utilities

```bash
# Format all Python code
find backend -name "*.py" -exec black {} +

# Check for security vulnerabilities in dependencies
cd backend && safety check

# Generate requirements.txt from current environment
cd backend && pip freeze > requirements-frozen.txt

# Clear Python cache files
find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null || true
find . -name "*.pyc" -delete

# Reset frontend node_modules
cd frontend && rm -rf node_modules package-lock.json && npm install
```

## 🔧 Configuration & Environment

### Backend Configuration

The backend uses a configuration system that expects:
- `backend/config.template.yaml` - Template configuration file
- Environment variables for sensitive data (AWS credentials, DB passwords)
- DynamoDB table configuration for audit sessions and scan data

### Environment Variables

```bash
# Typical environment setup
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"
export AWS_DEFAULT_REGION="us-west-2"
export DATABASE_PASSWORD="your-db-password"
```

## 🐛 Troubleshooting

### Common Issues

**MyPy Type Checking Errors:**
```bash
# Ignore specific errors temporarily
# type: ignore[error-code]

# Skip type checking for entire file
# mypy: disable-file
```

**Pre-commit Hook Failures:**
```bash
# Skip hooks for emergency commits (use sparingly)
git commit --no-verify

# Update pre-commit hook versions
pre-commit autoupdate
```

**Docker Build Issues:**
- Ensure you're building from repository root (not backend/ directory)
- ML dependencies may require substantial memory during build
- Check Docker daemon memory limits if builds fail

**Database Connection Issues:**
- Verify SSH tunnel configuration in backend configuration
- Check that database credentials are correctly set
- Test with `/api/db/ping` endpoint

### Performance Tips

- Use pre-commit hooks to catch issues before CI/CD
- Run specific linting tools during development rather than full suite
- Cache Docker layers by organizing Dockerfile instructions efficiently
- Use `npm ci` instead of `npm install` for faster, reproducible builds

## 📚 Reference Documentation

- **API Documentation**: http://localhost:8000/docs (when backend is running)
- **Code Quality Standards**: See `CODE_QUALITY.md` for comprehensive quality guidelines
- **Frontend Setup**: See `frontend/README.md` for React/Vite specifics
- **Pre-commit Configuration**: See `.pre-commit-config.yaml` for all quality gates

### Key Technologies

- **Backend**: FastAPI, Python 3.11, APScheduler, Boto3, PyMySQL
- **Frontend**: React 19, Vite, Material-UI, React Router DOM
- **AI/ML**: PyTorch, Ultralytics YOLOv8, OpenCV, Google Generative AI
- **Infrastructure**: AWS S3, DynamoDB, Google Cloud Run, Docker
- **Quality**: Black, isort, flake8, mypy, ESLint, pre-commit hooks

## 🚀 WARP Workflow Suggestions

Save these as WARP Workflows for quick access:

1. **"Start Full Stack"** - `cd MetronAuditOps && (cd backend && uvicorn main:app --reload --port 8000 &) && cd frontend && npm run dev`

2. **"Run All Quality Checks"** - `pre-commit run --all-files`

3. **"Backend Dev Setup"** - `cd backend && source .venv/bin/activate && uvicorn main:app --reload --port 8000`

4. **"Test API Health"** - `curl http://localhost:8000/api/health && curl http://localhost:8000/api/status`

5. **"Docker Build & Run"** - `docker build -f backend/Dockerfile -t metron-backend . && docker run -p 8080:8080 metron-backend`
