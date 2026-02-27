from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Json


class Repository:
    def __init__(self, database_url: str, schema: Optional[str] = None):
        self.database_url = database_url
        self.schema = schema

    @contextmanager
    def connect(self):
        conn = psycopg.connect(self.database_url, row_factory=dict_row)
        try:
            if self.schema:
                conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(self.schema)))
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init_schema(self) -> None:
        schema_path = Path(__file__).resolve().parents[2] / "sql" / "platform_schema.sql"
        ddl = schema_path.read_text(encoding="utf-8-sig")
        with self.connect() as conn:
            exists = conn.execute("SELECT to_regclass('instruments') AS table_name").fetchone()
            if not exists or not exists.get("table_name"):
                conn.execute(ddl)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS instrument_delisting_snapshot (
                    delisting_snapshot_id BIGSERIAL PRIMARY KEY,
                    market_code VARCHAR(20) NOT NULL,
                    external_code VARCHAR(20) NOT NULL,
                    delisting_date DATE NOT NULL,
                    delisting_reason TEXT NULL,
                    note TEXT NULL,
                    source_name VARCHAR(30) NOT NULL,
                    collected_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NULL,
                    run_id UUID NULL,
                    UNIQUE (market_code, external_code)
                )
                """
            )
            conn.execute(
                """
                ALTER TABLE instrument_delisting_snapshot
                DROP CONSTRAINT IF EXISTS fk_instrument_delisting_snapshot_run
                """
            )
            conn.execute(
                """
                ALTER TABLE instrument_delisting_snapshot
                ADD CONSTRAINT fk_instrument_delisting_snapshot_run
                FOREIGN KEY (run_id) REFERENCES collection_runs(run_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_delisting_snapshot_market_date
                ON instrument_delisting_snapshot(market_code, delisting_date)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_delisting_snapshot_external_code
                ON instrument_delisting_snapshot(external_code)
                """
            )
            conn.execute("ALTER TABLE collection_runs DROP CONSTRAINT IF EXISTS collection_runs_status_check")
            conn.execute(
                """
                ALTER TABLE collection_runs
                ADD CONSTRAINT collection_runs_status_check
                CHECK (status IN ('RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED'))
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS export_jobs (
                    job_id UUID PRIMARY KEY,
                    status VARCHAR(20) NOT NULL,
                    progress INTEGER NOT NULL DEFAULT 0,
                    submitted_at TIMESTAMP NOT NULL,
                    started_at TIMESTAMP NULL,
                    finished_at TIMESTAMP NULL,
                    output_path TEXT NULL,
                    files JSONB NULL,
                    row_counts JSONB NULL,
                    error_code VARCHAR(50) NULL,
                    error_message TEXT NULL,
                    request_payload JSONB NOT NULL,
                    CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED')),
                    CHECK (progress >= 0 AND progress <= 100)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_export_jobs_status_submitted_at
                ON export_jobs(status, submitted_at DESC)
                """
            )

    def insert_run(self, run: Dict) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO collection_runs(
                    run_id, pipeline_name, source_name, window_start, window_end, status,
                    started_at, finished_at, success_count, failure_count, warning_count, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        if not fields:
            return
        assignments = sql.SQL(", ").join(
            sql.SQL("{} = %s").format(sql.Identifier(k)) for k in fields.keys()
        )
        query = sql.SQL("UPDATE collection_runs SET {} WHERE run_id = %s").format(assignments)
        with self.connect() as conn:
            conn.execute(query, [*fields.values(), run_id])

    def insert_export_job(self, job: Dict) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO export_jobs(
                    job_id, status, progress, submitted_at, started_at, finished_at,
                    output_path, files, row_counts, error_code, error_message, request_payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    job["job_id"],
                    job["status"],
                    job.get("progress", 0),
                    job["submitted_at"],
                    job.get("started_at"),
                    job.get("finished_at"),
                    job.get("output_path"),
                    Json(job["files"]) if "files" in job and job["files"] is not None else None,
                    Json(job["row_counts"]) if "row_counts" in job and job["row_counts"] is not None else None,
                    job.get("error_code"),
                    job.get("error_message"),
                    Json(job["request_payload"]),
                ),
            )

    def update_export_job(self, job_id: str, fields: Dict) -> None:
        if not fields:
            return
        adapted_fields = {
            key: (Json(value) if isinstance(value, (dict, list)) else value) for key, value in fields.items()
        }
        assignments = sql.SQL(", ").join(
            sql.SQL("{} = %s").format(sql.Identifier(k)) for k in adapted_fields.keys()
        )
        query = sql.SQL("UPDATE export_jobs SET {} WHERE job_id = %s").format(assignments)
        with self.connect() as conn:
            conn.execute(query, [*adapted_fields.values(), job_id])

    def get_export_job(self, job_id: str) -> Optional[Dict]:
        rows = self.query(
            """
            SELECT job_id, status, progress, submitted_at, started_at, finished_at,
                   output_path, files, row_counts, error_code, error_message, request_payload
            FROM export_jobs
            WHERE job_id = %s
            """,
            (job_id,),
        )
        return rows[0] if rows else None

    def upsert_instruments(self, rows: Iterable[Dict]) -> None:
        payload = [
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
        ]
        if not payload:
            return

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO instruments(
                        instrument_id, external_code, market_code, instrument_name, instrument_name_abbr,
                        instrument_name_eng, listing_date, delisting_date, listed_shares, security_group,
                        sector_name, stock_type, par_value, source_name, collected_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    payload,
                )

    def upsert_daily_market(self, rows: Iterable[Dict]) -> None:
        payload = [
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
                bool(r.get("is_trade_halted", False)),
                bool(r.get("is_under_supervision", False)),
                r.get("record_status", "VALID"),
                r["source_name"],
                r["collected_at"],
                r.get("run_id"),
            )
            for r in rows
        ]
        if not payload:
            return

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO daily_market_data(
                        instrument_id, trade_date, open, high, low, close, volume, turnover_value,
                        market_value, price_change, change_rate, listed_shares, is_trade_halted,
                        is_under_supervision, record_status, source_name, collected_at, run_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    payload,
                )

    def upsert_benchmark(self, rows: Iterable[Dict]) -> None:
        payload = [
            (
                r["index_code"],
                (r.get("index_name") or r["index_code"]),
                r["trade_date"],
                r["open"],
                r["high"],
                r["low"],
                r["close"],
                r.get("raw_open"),
                r.get("raw_high"),
                r.get("raw_low"),
                r.get("raw_close"),
                r.get("volume"),
                r.get("turnover_value"),
                r.get("market_cap"),
                r.get("price_change"),
                r.get("change_rate"),
                r.get("record_status", "VALID"),
                r["source_name"],
                r["collected_at"],
                r.get("run_id"),
            )
            for r in rows
        ]
        if not payload:
            return

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO benchmark_index_data(
                        index_code, index_name, trade_date, open, high, low, close,
                        raw_open, raw_high, raw_low, raw_close,
                        volume, turnover_value, market_cap, price_change, change_rate,
                        record_status, source_name, collected_at, run_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(index_code, index_name, trade_date) DO UPDATE SET
                        open=excluded.open,
                        high=excluded.high,
                        low=excluded.low,
                        close=excluded.close,
                        raw_open=excluded.raw_open,
                        raw_high=excluded.raw_high,
                        raw_low=excluded.raw_low,
                        raw_close=excluded.raw_close,
                        volume=excluded.volume,
                        turnover_value=excluded.turnover_value,
                        market_cap=excluded.market_cap,
                        price_change=excluded.price_change,
                        change_rate=excluded.change_rate,
                        record_status=excluded.record_status,
                        source_name=excluded.source_name,
                        collected_at=excluded.collected_at,
                        run_id=excluded.run_id
                    """,
                    payload,
                )

    def upsert_trading_calendar(self, rows: Iterable[Dict]) -> None:
        payload = [
            (
                r["market_code"],
                r["trade_date"],
                bool(r["is_open"]),
                r.get("holiday_name"),
                r["source_name"],
                r["collected_at"],
                r.get("run_id"),
            )
            for r in rows
        ]
        if not payload:
            return

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO trading_calendar(
                        market_code, trade_date, is_open, holiday_name, source_name, collected_at, run_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(market_code, trade_date) DO UPDATE SET
                        is_open=excluded.is_open,
                        holiday_name=excluded.holiday_name,
                        source_name=excluded.source_name,
                        collected_at=excluded.collected_at,
                        run_id=excluded.run_id
                    """,
                    payload,
                )

    def insert_issues(self, rows: Iterable[Dict]) -> None:
        payload = [
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
        ]
        if not payload:
            return

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO data_quality_issues(
                        dataset_name, trade_date, instrument_id, index_code, issue_code, severity,
                        issue_detail, source_name, detected_at, run_id, resolved_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    payload,
                )

    def bulk_update_delisting_dates(self, rows: Iterable[Dict], source_name: str, run_id: Optional[str] = None) -> Dict:
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        issues = []
        matched = 0
        updated = 0
        unchanged = 0
        unmatched = 0
        invalid = 0

        for row in rows:
            market_code = str(row.get("market_code") or "").upper()
            external_code = str(row.get("external_code") or "").strip()
            delisting_date = row.get("delisting_date")

            if not market_code or not external_code or not delisting_date:
                invalid += 1
                issues.append(
                    {
                        "dataset_name": "instruments",
                        "trade_date": None,
                        "instrument_id": None,
                        "index_code": None,
                        "issue_code": "DELISTING_ROW_INVALID",
                        "severity": "ERROR",
                        "issue_detail": f"market_code={market_code}, external_code={external_code}, delisting_date={delisting_date}",
                        "source_name": source_name,
                        "detected_at": now,
                        "run_id": run_id,
                        "resolved_at": None,
                    }
                )
                continue

            target = self.query(
                """
                SELECT instrument_id, listing_date, delisting_date
                FROM instruments
                WHERE market_code = %s AND external_code = %s
                LIMIT 1
                """,
                (market_code, external_code),
            )
            if not target:
                unmatched += 1
                # Delisting feed can include historical symbols outside current master coverage.
                continue

            matched += 1
            instrument_id = target[0]["instrument_id"]
            listing_date = target[0]["listing_date"]
            existing = target[0]["delisting_date"]

            if listing_date and str(delisting_date) < str(listing_date):
                invalid += 1
                issues.append(
                    {
                        "dataset_name": "instruments",
                        "trade_date": delisting_date,
                        "instrument_id": instrument_id,
                        "index_code": None,
                        "issue_code": "DELISTING_DATE_BEFORE_LISTING_DATE",
                        "severity": "ERROR",
                        "issue_detail": f"listing_date={listing_date}, delisting_date={delisting_date}",
                        "source_name": source_name,
                        "detected_at": now,
                        "run_id": run_id,
                        "resolved_at": None,
                    }
                )
                continue

            if existing == delisting_date:
                unchanged += 1
                continue

            with self.connect() as conn:
                conn.execute(
                    """
                    UPDATE instruments
                    SET delisting_date = %s,
                        source_name = %s,
                        collected_at = %s,
                        updated_at = %s
                    WHERE instrument_id = %s
                    """,
                    (delisting_date, source_name, now, now, instrument_id),
                )
            updated += 1

        if issues:
            self.insert_issues(issues)

        return {
            "matched": matched,
            "updated": updated,
            "unchanged": unchanged,
            "unmatched": unmatched,
            "invalid": invalid,
        }

    def upsert_delisting_snapshot(self, rows: Iterable[Dict], source_name: str, run_id: Optional[str] = None) -> Dict:
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        payload = []
        invalid = 0
        for row in rows:
            market_code = str(row.get("market_code") or "").upper().strip()
            external_code = str(row.get("external_code") or "").strip()
            delisting_date = row.get("delisting_date")
            if not market_code or not external_code or not delisting_date:
                invalid += 1
                continue
            payload.append(
                (
                    market_code,
                    external_code,
                    delisting_date,
                    row.get("delisting_reason"),
                    row.get("note"),
                    source_name,
                    row.get("collected_at") or now,
                    now,
                    run_id,
                )
            )
        if not payload:
            return {"upserted": 0, "invalid": invalid}

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO instrument_delisting_snapshot(
                        market_code, external_code, delisting_date, delisting_reason, note,
                        source_name, collected_at, updated_at, run_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(market_code, external_code) DO UPDATE SET
                        delisting_date = excluded.delisting_date,
                        delisting_reason = excluded.delisting_reason,
                        note = excluded.note,
                        source_name = excluded.source_name,
                        collected_at = excluded.collected_at,
                        updated_at = excluded.updated_at,
                        run_id = excluded.run_id
                    """,
                    payload,
                )

        return {"upserted": len(payload), "invalid": invalid}

    def query(self, query_text: str, params: tuple = ()) -> List[Dict]:
        if "?" in query_text and "%s" not in query_text:
            query_text = query_text.replace("?", "%s")
        with self.connect() as conn:
            cur = conn.execute(query_text, params)
            out: List[Dict] = []
            for row in cur.fetchall():
                normalized: Dict = {}
                for key, value in dict(row).items():
                    if isinstance(value, (date, datetime)):
                        normalized[key] = value.isoformat()
                    elif isinstance(value, UUID):
                        normalized[key] = str(value)
                    elif isinstance(value, Decimal):
                        normalized[key] = float(value)
                    else:
                        normalized[key] = value
                out.append(normalized)
            return out

    def get_core_market(self, market_codes: Iterable[str], date_from: str, date_to: str) -> List[Dict]:
        codes = [str(c).upper() for c in market_codes if str(c).strip()]
        if not codes:
            return []
        placeholders = ", ".join(["%s"] * len(codes))
        return self.query(
            f"""
            SELECT *
            FROM core_market_dataset_v1
            WHERE market_code IN ({placeholders})
              AND trade_date BETWEEN %s AND %s
              AND trade_date >= listing_date
              AND (delisting_date IS NULL OR trade_date < delisting_date)
            ORDER BY trade_date, market_code, instrument_id
            """,
            tuple(codes + [date_from, date_to]),
        )

    def get_signal_market(
        self,
        market_codes: Iterable[str],
        date_from: str,
        date_to: str,
        require_positive_volume: bool = False,
    ) -> List[Dict]:
        codes = [str(c).upper() for c in market_codes if str(c).strip()]
        if not codes:
            return []
        placeholders = ", ".join(["%s"] * len(codes))
        volume_clause = "AND volume > 0" if require_positive_volume else ""
        return self.query(
            f"""
            SELECT *
            FROM core_market_dataset_v1
            WHERE market_code IN ({placeholders})
              AND trade_date BETWEEN %s AND %s
              AND trade_date >= listing_date
              AND (delisting_date IS NULL OR trade_date < delisting_date)
              AND record_status = 'VALID'
              AND is_trade_halted = FALSE
              AND is_under_supervision = FALSE
              {volume_clause}
            ORDER BY trade_date, market_code, instrument_id
            """,
            tuple(codes + [date_from, date_to]),
        )

    def get_benchmark(
        self,
        index_codes: Iterable[str],
        date_from: str,
        date_to: str,
        series_names: Optional[Iterable[str]] = None,
    ) -> List[Dict]:
        codes = list(index_codes)
        if not codes:
            return []
        code_placeholders = ", ".join(["%s"] * len(codes))
        params: List = list(codes) + [date_from, date_to]
        where_series = ""
        if series_names:
            names = [n for n in series_names if n]
            if names:
                series_placeholders = ", ".join(["%s"] * len(names))
                where_series = f" AND index_name IN ({series_placeholders})"
                params.extend(names)
        else:
            where_series = " AND index_name = index_code"
        return self.query(
            f"""
            SELECT index_code, index_name, trade_date, open, high, low, close, record_status
            FROM benchmark_dataset_v1
            WHERE index_code IN ({code_placeholders})
              AND trade_date BETWEEN %s AND %s
              {where_series}
            ORDER BY trade_date, index_code, index_name
            """,
            tuple(params),
        )

    def get_calendar(self, market_codes: Iterable[str], date_from: str, date_to: str) -> List[Dict]:
        codes = [str(c).upper() for c in market_codes if str(c).strip()]
        if not codes:
            return []
        placeholders = ", ".join(["%s"] * len(codes))
        return self.query(
            f"""
            SELECT market_code, trade_date, is_open, holiday_name
            FROM trading_calendar_v1
            WHERE market_code IN ({placeholders})
              AND trade_date BETWEEN %s AND %s
            ORDER BY market_code, trade_date
            """,
            tuple(codes + [date_from, date_to]),
        )

    def get_issues(self, date_from: str, date_to: str) -> List[Dict]:
        return self.query(
            """
            SELECT dataset_name, trade_date, instrument_id, index_code, issue_code,
                   severity, issue_detail, run_id, detected_at
            FROM data_quality_issues
            WHERE trade_date BETWEEN %s AND %s
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
                SET resolved_at = %s
                WHERE resolved_at IS NULL
                  AND dataset_name = 'daily_market_data'
                  AND issue_code = 'INVALID_DAILY_MARKET_ROW'
                  AND issue_detail = 'high is inconsistent'
                """,
                (resolved_time,),
            )
            return int(cur.rowcount or 0)

