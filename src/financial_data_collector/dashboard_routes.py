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
    with repo.connect() as conn:
        instrument_count = conn.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]

        price_row = conn.execute(
            "SELECT COUNT(*) as cnt, MIN(trade_date) as date_from, MAX(trade_date) as date_to FROM daily_market_data"
        ).fetchone()

        bench_row = conn.execute(
            "SELECT COUNT(*) as cnt, MIN(trade_date) as date_from, MAX(trade_date) as date_to FROM benchmark_index_data"
        ).fetchone()

        trading_days = conn.execute(
            "SELECT COUNT(DISTINCT trade_date) FROM daily_market_data"
        ).fetchone()[0]

        open_issues = conn.execute(
            "SELECT COUNT(*) FROM data_quality_issues WHERE resolved_at IS NULL"
        ).fetchone()[0]

    return {
        "instrument_count": instrument_count,
        "price_count": price_row[0],
        "price_date_from": price_row[1],
        "price_date_to": price_row[2],
        "benchmark_count": bench_row[0],
        "benchmark_date_from": bench_row[1],
        "benchmark_date_to": bench_row[2],
        "trading_days": trading_days,
        "open_issues": open_issues,
    }


@router.get("/api/v1/dashboard/runs")
async def get_runs(request: Request, limit: int = Query(20, ge=1, le=100)):
    repo = request.app.state.repo
    with repo.connect() as conn:
        rows = conn.execute(
            """
            SELECT run_id, pipeline_name, source_name, window_start, window_end,
                   status, started_at, finished_at, success_count, failure_count, warning_count
            FROM collection_runs
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


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
        where_clauses.append("(instrument_name LIKE ? OR external_code LIKE ?)")
        params.extend([pattern, pattern])
    if external_code:
        where_clauses.append("external_code LIKE ?")
        params.append(f"%{external_code}%")
    if instrument_name:
        where_clauses.append("instrument_name LIKE ?")
        params.append(f"%{instrument_name}%")
    if market_code:
        where_clauses.append("market_code LIKE ?")
        params.append(f"%{market_code}%")
    if security_group:
        where_clauses.append("security_group LIKE ?")
        params.append(f"%{security_group}%")
    if sector_name:
        where_clauses.append("sector_name LIKE ?")
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

    with repo.connect() as conn:
        total = conn.execute(
            f"""
            SELECT COUNT(*) FROM instruments
            {where_sql}
            """,
            tuple(params),
        ).fetchone()[0]

        rows = conn.execute(
            f"""
            SELECT instrument_id, external_code, market_code, instrument_name,
                   listing_date, delisting_date, security_group, sector_name
            FROM instruments
            {where_sql}
            ORDER BY {order_column} {order_direction}, external_code ASC
            LIMIT ? OFFSET ?
            """,
            tuple(params + [size, offset]),
        ).fetchall()

    return {"total": total, "page": page, "size": size, "items": [dict(r) for r in rows]}


@router.get("/api/v1/dashboard/prices")
async def get_prices(
    request: Request,
    instrument_id: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
):
    if not instrument_id:
        return {"items": []}
    repo = request.app.state.repo
    params = [instrument_id]
    where_clauses = ["instrument_id = ?"]
    if date_from:
        where_clauses.append("trade_date >= ?")
        params.append(date_from)
    if date_to:
        where_clauses.append("trade_date <= ?")
        params.append(date_to)
    where = " AND ".join(where_clauses)
    with repo.connect() as conn:
        rows = conn.execute(
            f"""
            SELECT trade_date, open, high, low, close, volume, turnover_value,
                   change_rate, record_status
            FROM daily_market_data
            WHERE {where}
            ORDER BY trade_date DESC
            LIMIT 500
            """,
            tuple(params),
        ).fetchall()
    return {"items": [dict(r) for r in rows]}


@router.get("/api/v1/dashboard/benchmarks")
async def get_benchmarks(request: Request):
    repo = request.app.state.repo
    with repo.connect() as conn:
        rows = conn.execute(
            """
            SELECT index_code,
                   COUNT(*) as record_count,
                   MIN(trade_date) as date_from,
                   MAX(trade_date) as date_to
            FROM benchmark_index_data
            GROUP BY index_code
            ORDER BY index_code
            """,
        ).fetchall()
    return [dict(r) for r in rows]


@router.get("/api/v1/dashboard/benchmark-series")
async def get_benchmark_series_list(
    request: Request,
    index_code: str = Query(""),
):
    if not index_code:
        return []
    repo = request.app.state.repo
    with repo.connect() as conn:
        rows = conn.execute(
            """
            SELECT index_name,
                   COUNT(*) as record_count,
                   MIN(trade_date) as date_from,
                   MAX(trade_date) as date_to
            FROM benchmark_index_data
            WHERE index_code = ?
            GROUP BY index_name
            ORDER BY index_name
            """,
            (index_code,),
        ).fetchall()
    return [dict(r) for r in rows]


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
    where_clauses = ["index_code = ?", "index_name = ?"]
    if date_from:
        where_clauses.append("trade_date >= ?")
        params.append(date_from)
    if date_to:
        where_clauses.append("trade_date <= ?")
        params.append(date_to)
    where = " AND ".join(where_clauses)
    with repo.connect() as conn:
        total = conn.execute(
            f"""
            SELECT COUNT(*) as cnt
            FROM benchmark_index_data
            WHERE {where}
            """,
            tuple(params),
        ).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT trade_date, open, high, low, close, volume, change_rate, record_status
            FROM benchmark_index_data
            WHERE {where}
            ORDER BY trade_date DESC
            LIMIT ? OFFSET ?
            """,
            tuple(params + [limit, offset]),
        ).fetchall()
    return {"items": [dict(r) for r in rows], "total": total}


@router.get("/api/v1/dashboard/quality-issues")
async def get_quality_issues(
    request: Request,
    severity: str = Query(""),
    limit: int = Query(50, ge=1, le=200),
):
    repo = request.app.state.repo
    params: list = []
    where_clauses = ["resolved_at IS NULL"]
    if severity:
        where_clauses.append("severity = ?")
        params.append(severity)
    where = " AND ".join(where_clauses)
    params.append(limit)
    with repo.connect() as conn:
        rows = conn.execute(
            f"""
            SELECT issue_id, dataset_name, trade_date, instrument_id, index_code,
                   issue_code, severity, issue_detail, detected_at
            FROM data_quality_issues
            WHERE {where}
            ORDER BY detected_at DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
    return [dict(r) for r in rows]
