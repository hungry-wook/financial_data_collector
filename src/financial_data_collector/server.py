"""
FastAPI server for minimal backtest data browsing.
"""
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .dashboard_routes import router as dashboard_router
from .repository import Repository

DATABASE_URL = os.getenv("DATABASE_URL", "")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required")
    repo = Repository(DATABASE_URL)
    repo.init_schema()
    app.state.repo = repo
    yield


app = FastAPI(
    title="KRX Swing Backtest API",
    description="Read-only API for minimal KRX backtest datasets",
    version="2.0.0",
    lifespan=lifespan,
)

app.include_router(dashboard_router)


@app.get("/")
async def root():
    return {
        "service": "KRX Swing Backtest API",
        "version": "2.0.0",
        "status": "running",
        "endpoints": {
            "instruments": "GET /api/v1/instruments",
            "instrument_daily": "GET /api/v1/instruments/{external_code}/daily",
            "benchmark_daily": "GET /api/v1/benchmarks/{index_code}/daily",
            "calendar": "GET /api/v1/calendar",
        },
    }


@app.get("/health")
async def health_check():
    try:
        Repository(DATABASE_URL).query("SELECT 1 as ok")
        return {"status": "healthy", "db_backend": "postgresql"}
    except Exception as exc:
        return {"status": "unhealthy", "db_backend": "postgresql", "error": str(exc)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)