"""
FastAPI server for Backtest Export API
Run with: uvicorn financial_data_collector.server:app --host 0.0.0.0 --port 8000
"""
import asyncio
from contextlib import asynccontextmanager
from typing import Dict

from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel

from .api import BacktestExportAPI
from .dashboard_routes import router as dashboard_router
from .export_service import ExportRequest, ExportService
from .repository import Repository


# Configuration
DB_PATH = "data/financial_data.db"

# Global service instance
export_service: ExportService = None


class ExportRequestBody(BaseModel):
    market_code: str
    index_codes: list[str]
    series_names: list[str] | None = None
    date_from: str
    date_to: str
    include_issues: bool = False
    output_format: str = "parquet"
    output_path: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global export_service
    repo = Repository(DB_PATH)
    app.state.repo = repo
    export_service = ExportService(repo)
    print(f"✅ Export service initialized with DB: {DB_PATH}")
    yield
    # Shutdown
    print("👋 Shutting down...")


app = FastAPI(
    title="KRX Backtest Export API",
    description="Export KRX market data to Parquet files for backtesting",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(dashboard_router)

api = None  # Will be initialized after startup


def get_api() -> BacktestExportAPI:
    if export_service is None:
        raise HTTPException(status_code=500, detail="Service not initialized")
    return BacktestExportAPI(export_service)


def run_export_job(job_id: str):
    """Background task to run export job"""
    try:
        export_service.run_job(job_id)
        print(f"✅ Export job {job_id} completed")
    except Exception as e:
        print(f"❌ Export job {job_id} failed: {e}")


@app.get("/")
async def root():
    return {
        "service": "KRX Backtest Export API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "create_export": "POST /api/v1/backtest/exports",
            "get_job": "GET /api/v1/backtest/exports/{job_id}",
            "get_manifest": "GET /api/v1/backtest/exports/{job_id}/manifest",
        },
    }


@app.get("/health")
async def health_check():
    return {"status": "healthy", "db_path": DB_PATH}


@app.post("/api/v1/backtest/exports", status_code=202)
async def create_export(request: ExportRequestBody, background_tasks: BackgroundTasks):
    """
    Create a new export job and run it in the background.
    Returns immediately with job_id.
    """
    api = get_api()
    status_code, response = api.post_exports(request.model_dump())

    if status_code == 202:
        job_id = response["job_id"]
        # Run export in background
        background_tasks.add_task(run_export_job, job_id)

    return response


@app.get("/api/v1/backtest/exports/{job_id}")
async def get_export_status(job_id: str):
    """Get the status of an export job"""
    api = get_api()
    status_code, response = api.get_export(job_id)

    if status_code == 404:
        raise HTTPException(status_code=404, detail=response.get("error"))

    return response


@app.get("/api/v1/backtest/exports/{job_id}/manifest")
async def get_manifest(job_id: str):
    """Get the manifest of a completed export job"""
    api = get_api()
    status_code, response = api.get_manifest(job_id)

    if status_code == 404:
        raise HTTPException(status_code=404, detail=response.get("error"))

    return response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
