import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional


class Repository:
    def __init__(self, db_path: str):
        self.db_path = db_path

    @contextmanager
    def connect(self):
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS instruments (
                    instrument_id TEXT PRIMARY KEY,
                    external_code TEXT NOT NULL,
                    market_code TEXT NOT NULL,
                    instrument_name TEXT NOT NULL,
                    instrument_name_abbr TEXT NULL,
                    instrument_name_eng TEXT NULL,
                    listing_date TEXT NOT NULL,
                    delisting_date TEXT NULL,
                    listed_shares INTEGER NULL,
                    security_group TEXT NULL,
                    sector_name TEXT NULL,
                    stock_type TEXT NULL,
                    par_value REAL NULL,
                    source_name TEXT NOT NULL,
                    collected_at TEXT NOT NULL,
                    updated_at TEXT NULL,
                    UNIQUE (market_code, external_code),
                    CHECK (delisting_date IS NULL OR delisting_date >= listing_date)
                );

                CREATE TABLE IF NOT EXISTS collection_runs (
                    run_id TEXT PRIMARY KEY,
                    pipeline_name TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    window_start TEXT NOT NULL,
                    window_end TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NULL,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    failure_count INTEGER NOT NULL DEFAULT 0,
                    warning_count INTEGER NOT NULL DEFAULT 0,
                    metadata TEXT NULL,
                    CHECK (status IN ('RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED')),
                    CHECK (window_end >= window_start)
                );

                CREATE TABLE IF NOT EXISTS trading_calendar (
                    market_code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    is_open INTEGER NOT NULL,
                    holiday_name TEXT NULL,
                    source_name TEXT NOT NULL,
                    collected_at TEXT NOT NULL,
                    run_id TEXT NULL,
                    PRIMARY KEY (market_code, trade_date),
                    FOREIGN KEY (run_id) REFERENCES collection_runs(run_id)
                );

                CREATE TABLE IF NOT EXISTS daily_market_data (
                    instrument_id TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume INTEGER NOT NULL,
                    turnover_value REAL NULL,
                    market_value REAL NULL,
                    price_change REAL NULL,
                    change_rate REAL NULL,
                    listed_shares INTEGER NULL,
                    is_trade_halted INTEGER NOT NULL DEFAULT 0,
                    is_under_supervision INTEGER NOT NULL DEFAULT 0,
                    record_status TEXT NOT NULL DEFAULT 'VALID',
                    source_name TEXT NOT NULL,
                    collected_at TEXT NOT NULL,
                    run_id TEXT NULL,
                    PRIMARY KEY (instrument_id, trade_date),
                    CHECK (high >= MAX(open, close, low)),
                    CHECK (low <= MIN(open, close, high)),
                    CHECK (volume >= 0),
                    CHECK (turnover_value IS NULL OR turnover_value >= 0),
                    CHECK (market_value IS NULL OR market_value >= 0),
                    CHECK (record_status IN ('VALID', 'INVALID', 'MISSING')),
                    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id),
                    FOREIGN KEY (run_id) REFERENCES collection_runs(run_id)
                );

                CREATE TABLE IF NOT EXISTS benchmark_index_data (
                    index_code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume INTEGER NULL,
                    turnover_value REAL NULL,
                    market_cap REAL NULL,
                    price_change REAL NULL,
                    change_rate REAL NULL,
                    index_name TEXT NULL,
                    source_name TEXT NOT NULL,
                    collected_at TEXT NOT NULL,
                    run_id TEXT NULL,
                    PRIMARY KEY (index_code, trade_date),
                    CHECK (high >= MAX(open, close, low)),
                    CHECK (low <= MIN(open, close, high)),
                    CHECK (volume IS NULL OR volume >= 0),
                    FOREIGN KEY (run_id) REFERENCES collection_runs(run_id)
                );

                CREATE TABLE IF NOT EXISTS data_quality_issues (
                    issue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_name TEXT NOT NULL,
                    trade_date TEXT NULL,
                    instrument_id TEXT NULL,
                    index_code TEXT NULL,
                    issue_code TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    issue_detail TEXT NULL,
                    source_name TEXT NULL,
                    detected_at TEXT NOT NULL,
                    run_id TEXT NULL,
                    resolved_at TEXT NULL,
                    CHECK (severity IN ('INFO', 'WARN', 'ERROR')),
                    FOREIGN KEY (instrument_id) REFERENCES instruments(instrument_id),
                    FOREIGN KEY (run_id) REFERENCES collection_runs(run_id)
                );

                DROP VIEW IF EXISTS core_market_dataset_v1;
                CREATE VIEW core_market_dataset_v1 AS
                SELECT d.instrument_id,
                    i.external_code,
                    i.market_code,
                    i.instrument_name,
                    i.listing_date,
                    i.delisting_date,
                    d.trade_date,
                    d.open,
                    d.high,
                    d.low,
                    d.close,
                    d.volume,
                    d.turnover_value,
                    d.market_value,
                    d.is_trade_halted,
                    d.is_under_supervision,
                    d.record_status,
                    d.source_name,
                    d.collected_at
                FROM daily_market_data d
                JOIN instruments i ON i.instrument_id = d.instrument_id;

                DROP VIEW IF EXISTS benchmark_dataset_v1;
                CREATE VIEW benchmark_dataset_v1 AS
                SELECT index_code, trade_date, open, high, low, close, source_name, collected_at
                FROM benchmark_index_data;

                CREATE VIEW IF NOT EXISTS trading_calendar_v1 AS
                SELECT market_code, trade_date, is_open, holiday_name, source_name, collected_at
                FROM trading_calendar;
                """
            )

    def insert_run(self, run: Dict) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO collection_runs(
                    run_id, pipeline_name, source_name, window_start, window_end, status,
                    started_at, finished_at, success_count, failure_count, warning_count, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run["run_id"],
                    run["pipeline_name"],
                    run["source_name"],
                    run["window_start"],
                    run["window_end"],
                    run["status"],
                    run["started_at"],
                    run.get("finished_at"),
                    run.get("success_count", 0),
                    run.get("failure_count", 0),
                    run.get("warning_count", 0),
                    run.get("metadata"),
                ),
            )

    def update_run(self, run_id: str, fields: Dict) -> None:
        with self.connect() as conn:
            sets = ", ".join([f"{k}=?" for k in fields.keys()])
            conn.execute(f"UPDATE collection_runs SET {sets} WHERE run_id = ?", (*fields.values(), run_id))

    def upsert_instruments(self, rows: Iterable[Dict]) -> None:
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO instruments(
                    instrument_id, external_code, market_code, instrument_name, instrument_name_abbr,
                    instrument_name_eng, listing_date, delisting_date, listed_shares, security_group,
                    sector_name, stock_type, par_value, source_name, collected_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id) DO UPDATE SET
                    external_code=excluded.external_code,
                    market_code=excluded.market_code,
                    instrument_name=excluded.instrument_name,
                    instrument_name_abbr=excluded.instrument_name_abbr,
                    instrument_name_eng=excluded.instrument_name_eng,
                    listing_date=excluded.listing_date,
                    delisting_date=excluded.delisting_date,
                    listed_shares=excluded.listed_shares,
                    security_group=excluded.security_group,
                    sector_name=excluded.sector_name,
                    stock_type=excluded.stock_type,
                    par_value=excluded.par_value,
                    source_name=excluded.source_name,
                    collected_at=excluded.collected_at,
                    updated_at=excluded.updated_at
                """,
                [
                    (
                        r["instrument_id"],
                        r["external_code"],
                        r["market_code"],
                        r["instrument_name"],
                        r.get("instrument_name_abbr"),
                        r.get("instrument_name_eng"),
                        r["listing_date"],
                        r.get("delisting_date"),
                        r.get("listed_shares"),
                        r.get("security_group"),
                        r.get("sector_name"),
                        r.get("stock_type"),
                        r.get("par_value"),
                        r["source_name"],
                        r["collected_at"],
                        r.get("updated_at"),
                    )
                    for r in rows
                ],
            )

    def upsert_daily_market(self, rows: Iterable[Dict]) -> None:
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO daily_market_data(
                    instrument_id, trade_date, open, high, low, close, volume, turnover_value,
                    market_value, price_change, change_rate, listed_shares, is_trade_halted,
                    is_under_supervision, record_status, source_name, collected_at, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instrument_id, trade_date) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    turnover_value=excluded.turnover_value,
                    market_value=excluded.market_value,
                    price_change=excluded.price_change,
                    change_rate=excluded.change_rate,
                    listed_shares=excluded.listed_shares,
                    is_trade_halted=excluded.is_trade_halted,
                    is_under_supervision=excluded.is_under_supervision,
                    record_status=excluded.record_status,
                    source_name=excluded.source_name,
                    collected_at=excluded.collected_at,
                    run_id=excluded.run_id
                """,
                [
                    (
                        r["instrument_id"],
                        r["trade_date"],
                        r["open"],
                        r["high"],
                        r["low"],
                        r["close"],
                        r["volume"],
                        r.get("turnover_value"),
                        r.get("market_value"),
                        r.get("price_change"),
                        r.get("change_rate"),
                        r.get("listed_shares"),
                        1 if r.get("is_trade_halted") else 0,
                        1 if r.get("is_under_supervision") else 0,
                        r.get("record_status", "VALID"),
                        r["source_name"],
                        r["collected_at"],
                        r.get("run_id"),
                    )
                    for r in rows
                ],
            )

    def upsert_benchmark(self, rows: Iterable[Dict]) -> None:
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO benchmark_index_data(
                    index_code, trade_date, open, high, low, close, volume, turnover_value,
                    market_cap, price_change, change_rate, index_name, source_name, collected_at, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(index_code, trade_date) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    turnover_value=excluded.turnover_value,
                    market_cap=excluded.market_cap,
                    price_change=excluded.price_change,
                    change_rate=excluded.change_rate,
                    index_name=excluded.index_name,
                    source_name=excluded.source_name,
                    collected_at=excluded.collected_at,
                    run_id=excluded.run_id
                """,
                [
                    (
                        r["index_code"],
                        r["trade_date"],
                        r["open"],
                        r["high"],
                        r["low"],
                        r["close"],
                        r.get("volume"),
                        r.get("turnover_value"),
                        r.get("market_cap"),
                        r.get("price_change"),
                        r.get("change_rate"),
                        r.get("index_name"),
                        r["source_name"],
                        r["collected_at"],
                        r.get("run_id"),
                    )
                    for r in rows
                ],
            )

    def upsert_trading_calendar(self, rows: Iterable[Dict]) -> None:
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO trading_calendar(
                    market_code, trade_date, is_open, holiday_name, source_name, collected_at, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_code, trade_date) DO UPDATE SET
                    is_open=excluded.is_open,
                    holiday_name=excluded.holiday_name,
                    source_name=excluded.source_name,
                    collected_at=excluded.collected_at,
                    run_id=excluded.run_id
                """,
                [
                    (
                        r["market_code"],
                        r["trade_date"],
                        1 if r["is_open"] else 0,
                        r.get("holiday_name"),
                        r["source_name"],
                        r["collected_at"],
                        r.get("run_id"),
                    )
                    for r in rows
                ],
            )

    def insert_issues(self, rows: Iterable[Dict]) -> None:
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO data_quality_issues(
                    dataset_name, trade_date, instrument_id, index_code, issue_code, severity,
                    issue_detail, source_name, detected_at, run_id, resolved_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        r["dataset_name"],
                        r.get("trade_date"),
                        r.get("instrument_id"),
                        r.get("index_code"),
                        r["issue_code"],
                        r["severity"],
                        r.get("issue_detail"),
                        r.get("source_name"),
                        r["detected_at"],
                        r.get("run_id"),
                        r.get("resolved_at"),
                    )
                    for r in rows
                ],
            )

    def query(self, sql: str, params: tuple = ()) -> List[Dict]:
        with self.connect() as conn:
            cur = conn.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

    def get_core_market(self, market_code: str, date_from: str, date_to: str) -> List[Dict]:
        return self.query(
            """
            SELECT *
            FROM core_market_dataset_v1
            WHERE market_code = ?
              AND trade_date BETWEEN ? AND ?
            ORDER BY trade_date, instrument_id
            """,
            (market_code, date_from, date_to),
        )

    def get_benchmark(self, index_codes: Iterable[str], date_from: str, date_to: str) -> List[Dict]:
        codes = list(index_codes)
        placeholders = ", ".join(["?"] * len(codes))
        params = codes + [date_from, date_to]
        return self.query(
            f"""
            SELECT index_code, trade_date, open, high, low, close
            FROM benchmark_dataset_v1
            WHERE index_code IN ({placeholders})
              AND trade_date BETWEEN ? AND ?
            ORDER BY trade_date, index_code
            """,
            tuple(params),
        )

    def get_calendar(self, market_code: str, date_from: str, date_to: str) -> List[Dict]:
        return self.query(
            """
            SELECT market_code, trade_date, is_open, holiday_name
            FROM trading_calendar_v1
            WHERE market_code = ?
              AND trade_date BETWEEN ? AND ?
            ORDER BY trade_date
            """,
            (market_code, date_from, date_to),
        )

    def get_issues(self, date_from: str, date_to: str) -> List[Dict]:
        return self.query(
            """
            SELECT dataset_name, trade_date, instrument_id, index_code, issue_code,
                   severity, issue_detail, run_id, detected_at
            FROM data_quality_issues
            WHERE trade_date BETWEEN ? AND ?
              AND severity IN ('WARN', 'ERROR')
            ORDER BY detected_at
            """,
            (date_from, date_to),
        )

    def resolve_halted_issues(self, resolved_at: Optional[str] = None) -> int:
        resolved_time = resolved_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with self.connect() as conn:
            cur = conn.execute(
                """
                UPDATE data_quality_issues
                SET resolved_at = ?
                WHERE resolved_at IS NULL
                  AND dataset_name = 'daily_market_data'
                  AND issue_code = 'INVALID_DAILY_MARKET_ROW'
                  AND issue_detail = 'high is inconsistent'
                """,
                (resolved_time,),
            )
            return int(cur.rowcount or 0)
