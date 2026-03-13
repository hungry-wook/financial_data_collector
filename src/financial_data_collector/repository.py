from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
import time
from typing import Dict, Iterable, List, Optional
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Json


class Repository:
    DEFAULT_BENCHMARK_SERIES_CANDIDATES = {
        "KOSDAQ": ["KOSDAQ", "코스닥"],
        "KOSPI": ["KOSPI", "코스피"],
    }

    def __init__(self, database_url: str, schema: Optional[str] = None):
        self.database_url = database_url
        self.schema = schema

    @contextmanager
    def connect(self):
        conn = None
        last_error = None
        for attempt in range(3):
            try:
                conn = psycopg.connect(self.database_url, row_factory=dict_row)
                break
            except psycopg.OperationalError as exc:
                last_error = exc
                if attempt == 2:
                    raise
                # Burst reconnects on Windows can transiently fail with socket exhaustion.
                time.sleep(0.05 * (attempt + 1))
        if conn is None:
            raise last_error
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
        sql_dir = Path(__file__).resolve().parents[2] / "sql"
        ddl = (sql_dir / "platform_schema.sql").read_text(encoding="utf-8-sig")
        migrations = (sql_dir / "platform_postgres_migrations.sql").read_text(encoding="utf-8-sig")
        with self.connect() as conn:
            exists = conn.execute("SELECT to_regclass('instruments') AS table_name").fetchone()
            if not exists or not exists.get("table_name"):
                conn.execute(ddl)
            conn.execute(migrations)


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

    def get_core_market(
        self,
        market_codes: Iterable[str],
        date_from: str,
        date_to: str,
        series_type: str = "raw",
        as_of_timestamp: Optional[str] = None,
    ) -> List[Dict]:
        codes = [str(c).upper() for c in market_codes if str(c).strip()]
        if not codes:
            return []
        placeholders = ", ".join(["%s"] * len(codes))
        as_of_date = "9999-12-31"
        if as_of_timestamp:
            as_of_date = str(as_of_timestamp).strip().split("T", 1)[0]

        normalized_series = str(series_type or "raw").strip().lower()
        if normalized_series not in {"raw", "adjusted", "both"}:
            raise ValueError("series_type must be one of: raw, adjusted, both")

        if normalized_series == "raw":
            select_clause = """
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
            JOIN instruments i ON i.instrument_id = d.instrument_id
            """
        elif normalized_series == "adjusted":
            select_clause = """
            SELECT d.instrument_id,
                   i.external_code,
                   i.market_code,
                   i.instrument_name,
                   i.listing_date,
                   i.delisting_date,
                   d.trade_date,
                   d.open * COALESCE(p.cumulative_factor, 1.0) AS open,
                   d.high * COALESCE(p.cumulative_factor, 1.0) AS high,
                   d.low * COALESCE(p.cumulative_factor, 1.0) AS low,
                   d.close * COALESCE(p.cumulative_factor, 1.0) AS close,
                   d.volume / COALESCE(NULLIF(p.cumulative_factor, 0), 1.0) AS volume,
                   d.turnover_value,
                   d.market_value,
                   d.is_trade_halted,
                   d.is_under_supervision,
                   d.record_status,
                   d.source_name,
                   d.collected_at
            FROM daily_market_data d
            JOIN instruments i ON i.instrument_id = d.instrument_id
            LEFT JOIN price_adjustment_factors p
              ON p.instrument_id = d.instrument_id
             AND p.trade_date = d.trade_date
             AND p.as_of_date = %s
            """
        else:
            select_clause = """
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
                   d.open * COALESCE(p.cumulative_factor, 1.0) AS adj_open,
                   d.high * COALESCE(p.cumulative_factor, 1.0) AS adj_high,
                   d.low * COALESCE(p.cumulative_factor, 1.0) AS adj_low,
                   d.close * COALESCE(p.cumulative_factor, 1.0) AS adj_close,
                   d.volume,
                   d.volume / COALESCE(NULLIF(p.cumulative_factor, 0), 1.0) AS adj_volume,
                   d.turnover_value,
                   d.market_value,
                   d.is_trade_halted,
                   d.is_under_supervision,
                   d.record_status,
                   d.source_name,
                   d.collected_at
            FROM daily_market_data d
            JOIN instruments i ON i.instrument_id = d.instrument_id
            LEFT JOIN price_adjustment_factors p
              ON p.instrument_id = d.instrument_id
             AND p.trade_date = d.trade_date
             AND p.as_of_date = %s
            """

        sql_text = f"""
            {select_clause}
            WHERE i.market_code IN ({placeholders})
              AND d.trade_date BETWEEN %s AND %s
              AND d.trade_date >= i.listing_date
              AND (i.delisting_date IS NULL OR d.trade_date < i.delisting_date)
            ORDER BY d.trade_date, i.market_code, d.instrument_id
        """
        params: List = []
        if normalized_series in {"adjusted", "both"}:
            params.append(as_of_date)
        params.extend(codes)
        params.extend([date_from, date_to])
        return self.query(sql_text, tuple(params))
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
        names = [str(name).strip() for name in (series_names or []) if str(name).strip()]
        if names:
            series_placeholders = ", ".join(["%s"] * len(names))
            where_series = f" AND index_name IN ({series_placeholders})"
            params.extend(names)
        else:
            default_series = self.get_default_benchmark_series_map(codes)
            if not default_series:
                return []
            pair_filters = []
            for code in codes:
                default_name = default_series.get(code)
                if not default_name:
                    continue
                pair_filters.append("(index_code = %s AND index_name = %s)")
                params.extend([code, default_name])
            if not pair_filters:
                return []
            where_series = f" AND ({' OR '.join(pair_filters)})"
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

    def get_default_benchmark_series(self, index_code: str) -> Optional[str]:
        preferred_names = self.DEFAULT_BENCHMARK_SERIES_CANDIDATES.get(str(index_code).strip().upper(), [])
        rows = self.query(
            """
            SELECT index_name
            FROM (
                SELECT DISTINCT index_code, index_name
                FROM benchmark_dataset_v1
                WHERE index_code = %s
            ) series
            ORDER BY CASE
                WHEN index_name = %s THEN 0
                WHEN index_name = %s THEN 1
                WHEN index_name = index_code THEN 2
                ELSE 3
            END,
            index_name
            LIMIT 1
            """,
            (
                index_code,
                preferred_names[0] if len(preferred_names) > 0 else "",
                preferred_names[1] if len(preferred_names) > 1 else "",
            ),
        )
        return rows[0]["index_name"] if rows else None

    def get_default_benchmark_series_map(self, index_codes: Iterable[str]) -> Dict[str, str]:
        codes = [str(code).strip() for code in index_codes if str(code).strip()]
        if not codes:
            return {}
        placeholders = ", ".join(["%s"] * len(codes))
        candidate_order = []
        for code in codes:
            for rank, name in enumerate(self.DEFAULT_BENCHMARK_SERIES_CANDIDATES.get(code.upper(), [])):
                candidate_order.append((code, name, rank))
        candidate_values_sql = ", ".join(["(%s, %s, %s)"] * len(candidate_order)) if candidate_order else ""
        preferred_join = (
            f"""
                LEFT JOIN (
                    VALUES {candidate_values_sql}
                ) AS preferred(index_code, index_name, preferred_rank)
                  ON preferred.index_code = series.index_code
                 AND preferred.index_name = series.index_name
            """
            if candidate_order
            else ""
        )
        preferred_params: List = []
        for code, name, rank in candidate_order:
            preferred_params.extend([code, name, rank])
        rows = self.query(
            f"""
            SELECT index_code, index_name
            FROM (
                SELECT series.index_code,
                       series.index_name,
                       ROW_NUMBER() OVER (
                           PARTITION BY series.index_code
                           ORDER BY COALESCE(preferred.preferred_rank,
                                             CASE WHEN series.index_name = series.index_code THEN 2 ELSE 3 END),
                                    series.index_name
                       ) AS rn
                FROM (
                    SELECT DISTINCT index_code, index_name
                    FROM benchmark_dataset_v1
                    WHERE index_code IN ({placeholders})
                ) series
                {preferred_join}
            ) ranked
            WHERE rn = 1
            ORDER BY index_code
            """,
            tuple(codes + preferred_params),
        )
        return {row["index_code"]: row["index_name"] for row in rows}

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

    def get_adjustment_factor_gaps(
        self,
        market_codes: Iterable[str],
        date_from: str,
        date_to: str,
        as_of_timestamp: Optional[str] = None,
        event_types: Optional[Iterable[str]] = None,
    ) -> List[Dict]:
        codes = [str(c).upper() for c in market_codes if str(c).strip()]
        types = [str(t).upper() for t in (event_types or []) if str(t).strip()]
        if not codes or not types:
            return []

        market_placeholders = ", ".join(["%s"] * len(codes))
        type_placeholders = ", ".join(["%s"] * len(types))
        as_of_date = "9999-12-31"
        params: List = list(codes) + list(types) + [date_to]
        as_of_clause = ""
        if as_of_timestamp:
            as_of_date = str(as_of_timestamp).strip().split("T", 1)[0]
            as_of_clause = " AND COALESCE(e.announce_date, e.effective_date) <= %s"
            params.append(as_of_date)
        params.extend([date_from, date_to, as_of_date])

        return self.query(
            f"""
            WITH eligible_instruments AS (
                SELECT DISTINCT e.instrument_id
                FROM corporate_events e
                JOIN instruments i ON i.instrument_id = e.instrument_id
                WHERE i.market_code IN ({market_placeholders})
                  AND e.status = 'ACTIVE'
                  AND e.event_type IN ({type_placeholders})
                  AND e.effective_date IS NOT NULL
                  AND e.effective_date <= %s
                  {as_of_clause}
            )
            SELECT d.instrument_id,
                   i.external_code,
                   i.market_code,
                   COUNT(*) AS missing_trade_dates,
                   MIN(d.trade_date) AS first_missing_trade_date,
                   MAX(d.trade_date) AS last_missing_trade_date
            FROM eligible_instruments ei
            JOIN daily_market_data d
              ON d.instrument_id = ei.instrument_id
             AND d.trade_date BETWEEN %s AND %s
            JOIN instruments i ON i.instrument_id = d.instrument_id
            LEFT JOIN price_adjustment_factors p
              ON p.instrument_id = d.instrument_id
             AND p.trade_date = d.trade_date
             AND p.as_of_date = %s
            WHERE p.instrument_id IS NULL
            GROUP BY d.instrument_id, i.external_code, i.market_code
            ORDER BY missing_trade_dates DESC, d.instrument_id
            """,
            tuple(params),
        )

    def upsert_corporate_events(self, rows: Iterable[Dict]) -> int:
        payload = [
            (
                r["event_id"],
                int(r.get("event_version", 1)),
                r["instrument_id"],
                r["event_type"],
                r.get("announce_date"),
                r.get("effective_date"),
                r.get("source_event_id"),
                r["source_name"],
                r["collected_at"],
                r.get("run_id"),
                r.get("raw_factor"),
                r.get("confidence", "MEDIUM"),
                r.get("status", "ACTIVE"),
                Json(r.get("payload")) if r.get("payload") is not None else None,
            )
            for r in rows
        ]
        if not payload:
            return 0

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO corporate_events(
                        event_id, event_version, instrument_id, event_type,
                        announce_date, effective_date, source_event_id,
                        source_name, collected_at, run_id,
                        raw_factor, confidence, status, payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(event_id, event_version) DO UPDATE SET
                        instrument_id=excluded.instrument_id,
                        event_type=excluded.event_type,
                        announce_date=excluded.announce_date,
                        effective_date=excluded.effective_date,
                        source_event_id=excluded.source_event_id,
                        source_name=excluded.source_name,
                        collected_at=excluded.collected_at,
                        run_id=excluded.run_id,
                        raw_factor=excluded.raw_factor,
                        confidence=excluded.confidence,
                        status=excluded.status,
                        payload=excluded.payload
                    """,
                    payload,
                )
        return len(payload)

    def insert_event_validation_results(self, rows: Iterable[Dict]) -> int:
        payload = [
            (
                r["source_event_id"],
                r["check_name"],
                r["result"],
                r.get("detail"),
                r["validated_at"],
            )
            for r in rows
        ]
        if not payload:
            return 0
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO event_validation_results(
                        source_event_id, check_name, result, detail, validated_at
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    payload,
                )
        return len(payload)

    def upsert_price_adjustment_factors(self, rows: Iterable[Dict]) -> int:
        payload = [
            (
                r["instrument_id"],
                r["trade_date"],
                r.get("as_of_date", "9999-12-31"),
                r["factor"],
                r["cumulative_factor"],
                r.get("factor_source", "corporate_event"),
                r.get("confidence", "MEDIUM"),
                r["created_at"],
                r.get("run_id"),
            )
            for r in rows
        ]
        if not payload:
            return 0

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO price_adjustment_factors(
                        instrument_id, trade_date, as_of_date,
                        factor, cumulative_factor, factor_source,
                        confidence, created_at, run_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(instrument_id, trade_date, as_of_date) DO UPDATE SET
                        factor=excluded.factor,
                        cumulative_factor=excluded.cumulative_factor,
                        factor_source=excluded.factor_source,
                        confidence=excluded.confidence,
                        created_at=excluded.created_at,
                        run_id=excluded.run_id
                    """,
                    payload,
                )
        return len(payload)

    def clear_price_adjustment_factors(self, date_from: str, date_to: str, as_of_date: str = "9999-12-31") -> int:
        with self.connect() as conn:
            cur = conn.execute(
                """
                DELETE FROM price_adjustment_factors
                WHERE trade_date BETWEEN %s AND %s
                  AND as_of_date = %s
                """,
                (date_from, date_to, as_of_date),
            )
            return int(cur.rowcount or 0)

    def get_corporate_events_for_period(
        self,
        date_from: str,
        date_to: str,
        as_of_date: Optional[str] = None,
        statuses: Optional[Iterable[str]] = None,
    ) -> List[Dict]:
        status_values = [str(s).upper() for s in (statuses or ["ACTIVE"]) if str(s).strip()]
        if not status_values:
            status_values = ["ACTIVE"]
        status_placeholders = ", ".join(["%s"] * len(status_values))
        params: List = [date_from, date_to, *status_values]
        as_of_clause = ""
        if as_of_date:
            as_of_clause = " AND COALESCE(announce_date, effective_date) <= %s"
            params.append(as_of_date)

        return self.query(
            f"""
            SELECT instrument_id, event_type, announce_date, effective_date,
                   source_event_id, raw_factor, confidence, status, payload
            FROM corporate_events
            WHERE effective_date BETWEEN %s AND %s
              AND status IN ({status_placeholders})
              {as_of_clause}
            ORDER BY instrument_id, effective_date, event_id, event_version
            """,
            tuple(params),
        )

    def get_market_trade_dates(self, date_from: str, date_to: str) -> List[Dict]:
        return self.query(
            """
            SELECT instrument_id, trade_date
            FROM daily_market_data
            WHERE trade_date BETWEEN %s AND %s
            ORDER BY instrument_id, trade_date
            """,
            (date_from, date_to),
        )


    def find_split_trade_date(
        self,
        instrument_id: str,
        date_from: str,
        date_to: str,
        expected_factor: float,
        tolerance: float = 0.002,
    ) -> Optional[str]:
        if not instrument_id or expected_factor is None:
            return None
        try:
            factor = float(expected_factor)
        except (TypeError, ValueError):
            return None
        if factor <= 0:
            return None

        rows = self.query(
            '''
            SELECT trade_date, close, listed_shares, market_value
            FROM daily_market_data
            WHERE instrument_id = %s
              AND trade_date BETWEEN %s AND %s
            ORDER BY trade_date
            ''',
            (instrument_id, date_from, date_to),
        )
        if len(rows) < 2:
            return None

        previous = None
        for row in rows:
            if previous is None:
                previous = row
                continue
            prev_shares = previous.get('listed_shares')
            curr_shares = row.get('listed_shares')
            if prev_shares and curr_shares and prev_shares > 0 and curr_shares > 0:
                observed = float(prev_shares) / float(curr_shares)
                if abs(observed - factor) <= tolerance:
                    return row.get('trade_date')
            previous = row
        return None

    def get_latest_trade_date(self) -> Optional[str]:
        rows = self.query(
            """
            SELECT MAX(trade_date) AS latest_trade_date
            FROM daily_market_data
            """
        )
        if not rows:
            return None
        return rows[0].get("latest_trade_date")

    def get_existing_instrument_ids(self, instrument_ids: Iterable[str]) -> set[str]:
        ids = [str(x).strip() for x in instrument_ids if str(x).strip()]
        if not ids:
            return set()
        placeholders = ", ".join(["%s"] * len(ids))
        rows = self.query(
            f"""
            SELECT instrument_id
            FROM instruments
            WHERE instrument_id IN ({placeholders})
            """,
            tuple(ids),
        )
        return {str(row["instrument_id"]) for row in rows if row.get("instrument_id")}

    def get_instrument_id_by_external_code(self, external_code: str, market_code: Optional[str] = None) -> Optional[str]:
        if not external_code:
            return None
        if market_code:
            rows = self.query(
                """
                SELECT instrument_id
                FROM instruments
                WHERE external_code = %s AND market_code = %s
                LIMIT 1
                """,
                (external_code, str(market_code).upper()),
            )
        else:
            rows = self.query(
                """
                SELECT instrument_id
                FROM instruments
                WHERE external_code = %s
                ORDER BY listing_date DESC
                LIMIT 1
                """,
                (external_code,),
            )
        return rows[0]["instrument_id"] if rows else None


    def get_instrument_id_by_corp_code_history(self, corp_code: str) -> Optional[str]:
        if not corp_code:
            return None
        rows = self.query(
            """
            SELECT instrument_id
            FROM corporate_events
            WHERE payload->>'corp_code' = %s
            ORDER BY collected_at DESC
            LIMIT 1
            """,
            (corp_code,),
        )
        return rows[0]["instrument_id"] if rows else None

    def get_latest_factor_for_chain(self, corp_code: str, event_type: str, revision_anchor: str, chain_date: Optional[str] = None) -> Optional[float]:
        if not corp_code or not event_type or not revision_anchor:
            return None
        params = [corp_code, event_type, revision_anchor]
        chain_date_clause = ''
        if chain_date:
            chain_date_clause = " AND COALESCE(effective_date, announce_date) = %s::date"
            params.append(chain_date)
        rows = self.query(
            f"""
            SELECT raw_factor
            FROM corporate_events
            WHERE payload->>'corp_code' = %s
              AND event_type = %s
              AND payload->>'revision_anchor' = %s
              {chain_date_clause}
              AND status = 'ACTIVE'
              AND raw_factor IS NOT NULL
            ORDER BY collected_at DESC
            LIMIT 1
            """,
            tuple(params),
        )
        if not rows:
            return None
        try:
            return float(rows[0]["raw_factor"])
        except (TypeError, ValueError):
            return None
    def delete_corporate_events_by_source_ids(self, source_event_ids: Iterable[str]) -> int:
        ids = [str(x).strip() for x in source_event_ids if str(x).strip()]
        if not ids:
            return 0
        placeholders = ", ".join(["%s"] * len(ids))
        with self.connect() as conn:
            cur = conn.execute(
                f"DELETE FROM corporate_events WHERE source_event_id IN ({placeholders})",
                tuple(ids),
            )
            return int(cur.rowcount or 0)

    def delete_outdated_revision_chain_events(self, chain_keys: Iterable[Dict[str, str]]) -> int:
        normalized_keys = []
        seen = set()
        for key in chain_keys:
            corp_code = str((key or {}).get("corp_code") or "").strip()
            event_type = str((key or {}).get("event_type") or "").strip().upper()
            revision_anchor = str((key or {}).get("revision_anchor") or "").strip()
            chain_date = str((key or {}).get("chain_date") or "").strip()
            if not corp_code or not event_type or not revision_anchor or not chain_date:
                continue
            dedupe_key = (corp_code, event_type, revision_anchor, chain_date)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            normalized_keys.append(dedupe_key)

        if not normalized_keys:
            return 0

        values_sql = ", ".join(["(%s, %s, %s, %s)"] * len(normalized_keys))
        params: List[str] = []
        for corp_code, event_type, revision_anchor, chain_date in normalized_keys:
            params.extend([corp_code, event_type, revision_anchor, chain_date])

        with self.connect() as conn:
            cur = conn.execute(
                f"""
                WITH target_chains(corp_code, event_type, revision_anchor, chain_date) AS (
                    VALUES {values_sql}
                ),
                ranked AS (
                    SELECT e.event_id,
                           e.event_version,
                           ROW_NUMBER() OVER (
                               PARTITION BY e.payload->>'corp_code', e.event_type, e.payload->>'revision_anchor', COALESCE(e.effective_date, e.announce_date)
                               ORDER BY e.source_event_id DESC NULLS LAST,
                                        e.collected_at DESC,
                                        e.event_id DESC,
                                        e.event_version DESC
                           ) AS rn
                    FROM corporate_events e
                    JOIN target_chains t
                      ON e.payload->>'corp_code' = t.corp_code
                     AND e.event_type = t.event_type
                     AND e.payload->>'revision_anchor' = t.revision_anchor
                     AND COALESCE(e.effective_date, e.announce_date) = t.chain_date::date
                    WHERE COALESCE(e.payload->>'corp_code', '') <> ''
                      AND COALESCE(e.payload->>'revision_anchor', '') <> ''
                )
                DELETE FROM corporate_events e
                USING ranked r
                WHERE e.event_id = r.event_id
                  AND e.event_version = r.event_version
                  AND r.rn > 1
                """,
                tuple(params),
            )
            return int(cur.rowcount or 0)

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






