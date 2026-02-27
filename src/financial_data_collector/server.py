"""
FastAPI server for Backtest Export API
Run with: uvicorn financial_data_collector.server:app --host 0.0.0.0 --port 8000
"""
import os
from contextlib import asynccontextmanager

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .api import BacktestExportAPI
from .dashboard_routes import router as dashboard_router
from .export_service import ExportService
from .repository import Repository

DATABASE_URL = os.getenv("DATABASE_URL", "")

export_service: ExportService | None = None


class ExportRequestBody(BaseModel):
    market_codes: list[str] | None = None
    market_code: str | None = None
    index_codes: list[str]
    series_names: list[str] | None = None
    date_from: str
    date_to: str
    include_issues: bool = False
    output_format: str = "parquet"
    output_path: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    global export_service
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required")

    repo = Repository(DATABASE_URL)
    repo.init_schema()
    app.state.repo = repo
    export_service = ExportService(repo)
    print("Export service initialized with PostgreSQL")
    yield
    print("Shutting down")


app = FastAPI(
    title="KRX Backtest Export API",
    description="Export KRX market data to Parquet files for backtesting",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(dashboard_router)


def get_api() -> BacktestExportAPI:
    if export_service is None:
        raise HTTPException(status_code=500, detail="Service not initialized")
    return BacktestExportAPI(export_service)


def run_export_job(job_id: str):
    try:
        export_service.run_job(job_id)
    except Exception as exc:
        print(f"Export job {job_id} failed: {exc}")


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
    try:
        Repository(DATABASE_URL).query("SELECT 1 as ok")
        return {"status": "healthy", "db_backend": "postgresql"}
    except Exception as exc:
        return {"status": "unhealthy", "db_backend": "postgresql", "error": str(exc)}


@app.post("/api/v1/backtest/exports")
async def create_export(request: ExportRequestBody, background_tasks: BackgroundTasks):
    api = get_api()
    status_code, response = api.post_exports(request.model_dump())

    if status_code == 202:
        background_tasks.add_task(run_export_job, response["job_id"])

    if status_code >= 400:
        raise HTTPException(status_code=status_code, detail=response.get("error"))

    return JSONResponse(content=response, status_code=status_code)


@app.get("/api/v1/backtest/exports/{job_id}")
async def get_export_status(job_id: str):
    api = get_api()
    status_code, response = api.get_export(job_id)

    if status_code == 404:
        raise HTTPException(status_code=404, detail=response.get("error"))

    return response


@app.get("/api/v1/backtest/exports/{job_id}/manifest")
async def get_manifest(job_id: str):
    api = get_api()
    status_code, response = api.get_manifest(job_id)

    if status_code == 404:
        raise HTTPException(status_code=404, detail=response.get("error"))

    return response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
