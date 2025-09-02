import sys
from pathlib import Path

# Add the backend directory to Python path
backend_dir = Path(__file__).resolve().parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router as api_router
from app.audit_service import AuditService
from app.aws_service import AWSService
from app.database_service import DatabaseService
from app.dynamo_service import DynamoDBService
from app.scheduler import start_scheduler
from app.skoopin_service import SkoopinService
from app.utils.config import *
from app.utils.dynamo_client import *


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🔗 Initializing DynamoDB connection...")
    init_dynamodb()
    print("🔗 Initializing Skoopin service...")
    app.state.skoopin_service = SkoopinService()
    app.state.dynamo_service = DynamoDBService()
    app.state.aws_service = AWSService()
    print("🔗 Initializing Database service...")
    app.state.database_service = DatabaseService()
    print("🔗 Initializing Audit service...")
    app.state.audit_service = AuditService(
        skoopin_service=app.state.skoopin_service,
        dynamo_service=app.state.dynamo_service,
    )
    print("🕒 Starting scheduler (16:00 & 20:00 PT / 4:00 PM & 8:00 PM)…")
    start_scheduler()
    yield


app = FastAPI(lifespan=lifespan)

# Allow frontend (React) access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Health check endpoint for Cloud Run
@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "automation-backend",
        "timestamp": "2024-01-01T00:00:00Z",
    }


app.include_router(api_router, prefix="/api")
