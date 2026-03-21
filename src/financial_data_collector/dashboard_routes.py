"""Dashboard routes for minimal backtest data browsing."""
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse

router = APIRouter()

DASHBOARD_HTML = Path(__file__).parent / "dashboard.html"
DASHBOARD_CSS = Path(__file__).parent / "dashboard.css"
DASHBOARD_JS = Path(__file__).parent / "dashboard.js"


@router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard(request: Request):
    return HTMLResponse(content=DASHBOARD_HTML.read_text(encoding="utf-8"))


@router.get("/dashboard/assets/dashboard.css", include_in_schema=False)
async def dashboard_css():
    if not DASHBOARD_CSS.exists():
        raise HTTPException(status_code=404, detail="dashboard.css not found")
    return FileResponse(DASHBOARD_CSS, media_type="text/css")


@router.get("/dashboard/assets/dashboard.js", include_in_schema=False)
async def dashboard_js():
    if not DASHBOARD_JS.exists():
        raise HTTPException(status_code=404, detail="dashboard.js not found")
    return FileResponse(DASHBOARD_JS, media_type="application/javascript")


@router.get("/api/v1/instruments")
async def get_instruments(request: Request, search: str = Query(""), listed_status: str = Query(""), limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)):
    return request.app.state.repo.list_instruments(search=search, listed_status=listed_status, limit=limit, offset=offset)


@router.get("/api/v1/instruments/{external_code}")
async def get_instrument_profile(external_code: str, request: Request):
    payload = request.app.state.repo.get_instrument_profile(external_code)
    if not payload:
        raise HTTPException(status_code=404, detail="instrument not found")
    return payload


@router.get("/api/v1/instruments/{external_code}/daily")
async def get_prices(external_code: str, request: Request, date_from: str = Query(""), date_to: str = Query(""), limit: int = Query(250, ge=1, le=2000), offset: int = Query(0, ge=0)):
    return request.app.state.repo.get_instrument_daily(external_code=external_code, date_from=date_from, date_to=date_to, limit=limit, offset=offset)


@router.get("/api/v1/benchmarks")
async def get_benchmarks(request: Request):
    return request.app.state.repo.list_benchmark_series()


@router.get("/api/v1/benchmarks/{index_code}/daily")
async def get_benchmark_series(index_code: str, request: Request, series_name: str = Query(""), date_from: str = Query(""), date_to: str = Query(""), limit: int = Query(250, ge=1, le=2000), offset: int = Query(0, ge=0)):
    return request.app.state.repo.get_benchmark_daily(index_code=index_code, series_name=series_name, date_from=date_from, date_to=date_to, limit=limit, offset=offset)


@router.get("/api/v1/calendar")
async def get_calendar(request: Request, market_codes: str = Query(...), date_from: str = Query(...), date_to: str = Query(...)):
    codes = [code.strip().upper() for code in market_codes.split(",") if code.strip()]
    return request.app.state.repo.get_calendar(codes, date_from, date_to)


@router.get("/api/v1/adjustments/coverage")
async def get_adjustment_coverage(request: Request, date_from: str = Query(...), date_to: str = Query(...), as_of_date: str = Query("9999-12-31")):
    return request.app.state.repo.get_adjustment_coverage(date_from=date_from, date_to=date_to, as_of_date=as_of_date)


@router.get("/api/v1/dashboard/summary")
async def get_summary(request: Request):
    repo = request.app.state.repo
    instrument_count = repo.query("SELECT COUNT(*) AS cnt FROM instruments")[0]["cnt"]
    price_row = repo.query("SELECT COUNT(*) AS cnt, MIN(trade_date) AS date_from, MAX(trade_date) AS date_to FROM daily_market_data")[0]
    bench_row = repo.query("SELECT COUNT(*) AS cnt, MIN(trade_date) AS date_from, MAX(trade_date) AS date_to FROM benchmark_index_data")[0]
    return {
        "instrument_count": instrument_count,
        "price_count": price_row["cnt"],
        "price_date_from": price_row["date_from"],
        "price_date_to": price_row["date_to"],
        "benchmark_count": bench_row["cnt"],
        "benchmark_date_from": bench_row["date_from"],
        "benchmark_date_to": bench_row["date_to"],
    }