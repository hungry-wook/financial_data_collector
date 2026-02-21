"""Dashboard routes for KRX data collection status visualization."""
from pathlib import Path

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

router = APIRouter()

DASHBOARD_HTML = Path(__file__).parent / "dashboard.html"


@router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard(request: Request):
    return HTMLResponse(content=DASHBOARD_HTML.read_text(encoding="utf-8"))


@router.get("/api/v1/dashboard/summary")
async def get_summary(request: Request):
    repo = request.app.state.repo
    instrument_count = repo.query("SELECT COUNT(*) AS cnt FROM instruments")[0]["cnt"]
    price_row = repo.query(
        "SELECT COUNT(*) AS cnt, MIN(trade_date) AS date_from, MAX(trade_date) AS date_to FROM daily_market_data"
    )[0]
    bench_row = repo.query(
        "SELECT COUNT(*) AS cnt, MIN(trade_date) AS date_from, MAX(trade_date) AS date_to FROM benchmark_index_data"
    )[0]
    trading_days = repo.query("SELECT COUNT(DISTINCT trade_date) AS cnt FROM daily_market_data")[0]["cnt"]
    open_issues = repo.query("SELECT COUNT(*) AS cnt FROM data_quality_issues WHERE resolved_at IS NULL")[0]["cnt"]

    return {
        "instrument_count": instrument_count,
        "price_count": price_row["cnt"],
        "price_date_from": price_row["date_from"],
        "price_date_to": price_row["date_to"],
        "benchmark_count": bench_row["cnt"],
        "benchmark_date_from": bench_row["date_from"],
        "benchmark_date_to": bench_row["date_to"],
        "trading_days": trading_days,
        "open_issues": open_issues,
    }


@router.get("/api/v1/dashboard/runs")
async def get_runs(request: Request, limit: int = Query(20, ge=1, le=100)):
    repo = request.app.state.repo
    rows = repo.query(
        """
        SELECT run_id, pipeline_name, source_name, window_start, window_end,
               status, started_at, finished_at, success_count, failure_count, warning_count
        FROM collection_runs
        ORDER BY started_at DESC
        LIMIT %s
        """,
        (limit,),
    )
    return rows


@router.get("/api/v1/dashboard/instruments")
async def get_instruments(
    request: Request,
    search: str = Query(""),
    external_code: str = Query(""),
    instrument_name: str = Query(""),
    market_code: str = Query(""),
    security_group: str = Query(""),
    sector_name: str = Query(""),
    listed_status: str = Query(""),
    sort_by: str = Query("market_code"),
    sort_order: str = Query("asc"),
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=200),
):
    repo = request.app.state.repo
    offset = (page - 1) * size

    where_clauses: list[str] = []
    params: list = []

    if search:
        pattern = f"%{search}%"
        where_clauses.append("(instrument_name ILIKE %s OR external_code ILIKE %s)")
        params.extend([pattern, pattern])
    if external_code:
        where_clauses.append("external_code ILIKE %s")
        params.append(f"%{external_code}%")
    if instrument_name:
        where_clauses.append("instrument_name ILIKE %s")
        params.append(f"%{instrument_name}%")
    if market_code:
        where_clauses.append("market_code ILIKE %s")
        params.append(f"%{market_code}%")
    if security_group:
        where_clauses.append("security_group ILIKE %s")
        params.append(f"%{security_group}%")
    if sector_name:
        where_clauses.append("sector_name ILIKE %s")
        params.append(f"%{sector_name}%")

    status = listed_status.lower()
    if status == "listed":
        where_clauses.append("delisting_date IS NULL")
    elif status == "delisted":
        where_clauses.append("delisting_date IS NOT NULL")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    sort_columns = {
        "external_code": "external_code",
        "instrument_name": "instrument_name",
        "market_code": "market_code",
        "security_group": "security_group",
        "sector_name": "sector_name",
        "listing_date": "listing_date",
        "delisting_date": "delisting_date",
    }
    order_column = sort_columns.get(sort_by, "market_code")
    order_direction = "DESC" if sort_order.lower() == "desc" else "ASC"

    total = repo.query(
        f"""
        SELECT COUNT(*) AS cnt FROM instruments
        {where_sql}
        """,
        tuple(params),
    )[0]["cnt"]

    rows = repo.query(
        f"""
        SELECT external_code, market_code, instrument_name,
               listing_date, delisting_date, security_group, sector_name
        FROM instruments
        {where_sql}
        ORDER BY {order_column} {order_direction}, external_code ASC
        LIMIT %s OFFSET %s
        """,
        tuple(params + [size, offset]),
    )

    return {"total": total, "page": page, "size": size, "items": rows}


@router.get("/api/v1/dashboard/prices")
async def get_prices(
    request: Request,
    external_code: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
):
    if not external_code:
        return {"items": []}

    repo = request.app.state.repo
    instrument = repo.query(
        """
        SELECT instrument_id, external_code
        FROM instruments
        WHERE external_code = %s
        ORDER BY (delisting_date IS NULL) DESC, listing_date DESC
        LIMIT 1
        """,
        (external_code.strip(),),
    )
    if not instrument:
        return {"items": []}

    params = [instrument[0]["instrument_id"]]
    where_clauses = ["instrument_id = %s"]
    if date_from:
        where_clauses.append("trade_date >= %s")
        params.append(date_from)
    if date_to:
        where_clauses.append("trade_date <= %s")
        params.append(date_to)
    where = " AND ".join(where_clauses)

    rows = repo.query(
        f"""
        SELECT trade_date, open, high, low, close, volume, turnover_value,
               change_rate, record_status
        FROM daily_market_data
        WHERE {where}
        ORDER BY trade_date DESC
        LIMIT 500
        """,
        tuple(params),
    )
    return {"items": rows}


@router.get("/api/v1/dashboard/benchmarks")
async def get_benchmarks(request: Request):
    repo = request.app.state.repo
    return repo.query(
        """
        SELECT index_code,
               COUNT(*) as record_count,
               MIN(trade_date) as date_from,
               MAX(trade_date) as date_to
        FROM benchmark_index_data
        GROUP BY index_code
        ORDER BY index_code
        """
    )


@router.get("/api/v1/dashboard/benchmark-series")
async def get_benchmark_series_list(
    request: Request,
    index_code: str = Query(""),
):
    if not index_code:
        return []
    repo = request.app.state.repo
    return repo.query(
        """
        SELECT index_name,
               COUNT(*) as record_count,
               MIN(trade_date) as date_from,
               MAX(trade_date) as date_to
        FROM benchmark_index_data
        WHERE index_code = %s
        GROUP BY index_name
        ORDER BY index_name
        """,
        (index_code,),
    )


@router.get("/api/v1/dashboard/benchmarks/{index_code}")
async def get_benchmark_series(
    index_code: str,
    request: Request,
    series_name: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    if not series_name:
        return {"items": [], "total": 0}

    repo = request.app.state.repo
    params = [index_code, series_name]
    where_clauses = ["index_code = %s", "index_name = %s"]
    if date_from:
        where_clauses.append("trade_date >= %s")
        params.append(date_from)
    if date_to:
        where_clauses.append("trade_date <= %s")
        params.append(date_to)
    where = " AND ".join(where_clauses)

    total = repo.query(
        f"""
        SELECT COUNT(*) as cnt
        FROM benchmark_index_data
        WHERE {where}
        """,
        tuple(params),
    )[0]["cnt"]

    rows = repo.query(
        f"""
        SELECT trade_date, open, high, low, close, volume, change_rate, record_status
        FROM benchmark_index_data
        WHERE {where}
        ORDER BY trade_date DESC
        LIMIT %s OFFSET %s
        """,
        tuple(params + [limit, offset]),
    )
    return {"items": rows, "total": total}


@router.get("/api/v1/dashboard/quality-issues")
async def get_quality_issues(
    request: Request,
    severity: str = Query(""),
    limit: int = Query(50, ge=1, le=200),
):
    repo = request.app.state.repo
    params: list = []
    where_clauses = ["q.resolved_at IS NULL"]
    if severity:
        where_clauses.append("q.severity = %s")
        params.append(severity)
    where = " AND ".join(where_clauses)
    params.append(limit)

    rows = repo.query(
        f"""
        SELECT q.issue_id, q.dataset_name, q.trade_date,
               i.external_code, q.index_code,
               q.issue_code, q.severity, q.issue_detail, q.detected_at
        FROM data_quality_issues q
        LEFT JOIN instruments i ON i.instrument_id = q.instrument_id
        WHERE {where}
        ORDER BY q.detected_at DESC
        LIMIT %s
        """,
        tuple(params),
    )
    return rows
