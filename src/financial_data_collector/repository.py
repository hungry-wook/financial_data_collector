from contextlib import closing, contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
import time
from typing import Dict, Iterable, Iterator, List, Optional
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
                time.sleep(0.05 * (attempt + 1))
        if conn is None:
            raise last_error
        try:
            if self.schema:
                conn.execute(sql.SQL("SET search_path TO {}") .format(sql.Identifier(self.schema)))
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

    def query(self, query_text: str, params: tuple = ()) -> List[Dict]:
        if "?" in query_text and "%s" not in query_text:
            query_text = query_text.replace("?", "%s")
        with self.connect() as conn:
            cur = conn.execute(query_text, params)
            return [self._normalize_row(dict(row)) for row in cur.fetchall()]

    def stream_query(self, query_text: str, params: tuple = (), fetch_size: int = 10000) -> Iterator[Dict]:
        if "?" in query_text and "%s" not in query_text:
            query_text = query_text.replace("?", "%s")
        conn_ctx = self.connect()
        conn = conn_ctx.__enter__()
        try:
            with closing(conn.cursor()) as cur:
                cur.execute(query_text, params)
                while True:
                    rows = cur.fetchmany(fetch_size)
                    if not rows:
                        break
                    for row in rows:
                        yield self._normalize_row(dict(row))
        except Exception:
            conn_ctx.__exit__(*__import__("sys").exc_info())
            raise
        else:
            conn_ctx.__exit__(None, None, None)

    @staticmethod
    def _normalize_row(row: Dict) -> Dict:
        normalized: Dict = {}
        for key, value in row.items():
            if isinstance(value, (date, datetime)):
                normalized[key] = value.isoformat()
            elif isinstance(value, UUID):
                normalized[key] = str(value)
            elif isinstance(value, Decimal):
                normalized[key] = float(value)
            else:
                normalized[key] = value
        return normalized

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
                    run["run_id"], run["pipeline_name"], run["source_name"], run["window_start"], run["window_end"],
                    run["status"], run["started_at"], run.get("finished_at"), run.get("success_count", 0),
                    run.get("failure_count", 0), run.get("warning_count", 0), Json(run.get("metadata")) if run.get("metadata") is not None else None,
                ),
            )

    def update_run(self, run_id: str, fields: Dict) -> None:
        if not fields:
            return
        adapted = {k: (Json(v) if isinstance(v, (dict, list)) else v) for k, v in fields.items()}
        assignments = sql.SQL(", ").join(sql.SQL("{} = %s").format(sql.Identifier(k)) for k in adapted.keys())
        query = sql.SQL("UPDATE collection_runs SET {} WHERE run_id = %s").format(assignments)
        with self.connect() as conn:
            conn.execute(query, [*adapted.values(), run_id])

    def upsert_instruments(self, rows: Iterable[Dict]) -> None:
        payload = [
            (
                r["instrument_id"], r["external_code"], r["market_code"], r["instrument_name"], r["listing_date"],
                r.get("delisting_date"), r.get("listed_shares"), r["source_name"], r["collected_at"], r.get("updated_at"),
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
                        instrument_id, external_code, market_code, instrument_name, listing_date,
                        delisting_date, listed_shares, source_name, collected_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(instrument_id) DO UPDATE SET
                        external_code=excluded.external_code,
                        market_code=excluded.market_code,
                        instrument_name=excluded.instrument_name,
                        listing_date=excluded.listing_date,
                        delisting_date=excluded.delisting_date,
                        listed_shares=excluded.listed_shares,
                        source_name=excluded.source_name,
                        collected_at=excluded.collected_at,
                        updated_at=excluded.updated_at
                    """,
                    payload,
                )

    def upsert_daily_market(self, rows: Iterable[Dict]) -> None:
        payload = [
            (
                r["instrument_id"], r["trade_date"], r["open"], r["high"], r["low"], r["close"], r["volume"],
                r.get("turnover_value"), r.get("market_value"), r.get("listed_shares"), r.get("base_price"),
                bool(r.get("is_trade_halted", False)), r.get("record_status", "VALID"), r["source_name"], r["collected_at"], r.get("run_id"),
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
                        market_value, listed_shares, base_price, is_trade_halted, record_status,
                        source_name, collected_at, run_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(instrument_id, trade_date) DO UPDATE SET
                        open=excluded.open,
                        high=excluded.high,
                        low=excluded.low,
                        close=excluded.close,
                        volume=excluded.volume,
                        turnover_value=excluded.turnover_value,
                        market_value=excluded.market_value,
                        listed_shares=excluded.listed_shares,
                        base_price=excluded.base_price,
                        is_trade_halted=excluded.is_trade_halted,
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
                r["index_code"], r.get("index_name") or r["index_code"], r["trade_date"], r["open"], r["high"], r["low"], r["close"],
                r.get("volume"), r.get("turnover_value"), r.get("market_cap"), r.get("record_status", "VALID"), r["source_name"], r["collected_at"], r.get("run_id"),
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
                        volume, turnover_value, market_cap, record_status, source_name, collected_at, run_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(index_code, index_name, trade_date) DO UPDATE SET
                        open=excluded.open,
                        high=excluded.high,
                        low=excluded.low,
                        close=excluded.close,
                        volume=excluded.volume,
                        turnover_value=excluded.turnover_value,
                        market_cap=excluded.market_cap,
                        record_status=excluded.record_status,
                        source_name=excluded.source_name,
                        collected_at=excluded.collected_at,
                        run_id=excluded.run_id
                    """,
                    payload,
                )

    def upsert_trading_calendar(self, rows: Iterable[Dict]) -> None:
        payload = [
            (r["market_code"], r["trade_date"], bool(r["is_open"]), r.get("holiday_name"), r["source_name"], r["collected_at"], r.get("run_id"))
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
                r["dataset_name"], r.get("trade_date"), r.get("instrument_id"), r.get("index_code"), r["issue_code"],
                r["severity"], r.get("issue_detail"), r.get("source_name"), r["detected_at"], r.get("run_id"), r.get("resolved_at"),
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
        matched = updated = unchanged = unmatched = invalid = 0
        issues = []
        for row in rows:
            market_code = str(row.get("market_code") or "").upper().strip()
            external_code = str(row.get("external_code") or "").strip()
            delisting_date = row.get("delisting_date")
            if not market_code or not external_code or not delisting_date:
                invalid += 1
                continue
            target = self.query(
                "SELECT instrument_id, listing_date, delisting_date FROM instruments WHERE market_code = %s AND external_code = %s LIMIT 1",
                (market_code, external_code),
            )
            if not target:
                unmatched += 1
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
                    "UPDATE instruments SET delisting_date = %s, source_name = %s, collected_at = %s, updated_at = %s WHERE instrument_id = %s",
                    (delisting_date, source_name, now, now, instrument_id),
                )
            updated += 1
        if issues:
            self.insert_issues(issues)
        return {"matched": matched, "updated": updated, "unchanged": unchanged, "unmatched": unmatched, "invalid": invalid}

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
            payload.append((market_code, external_code, delisting_date, row.get("delisting_reason"), row.get("note"), source_name, row.get("collected_at") or now, now, run_id))
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

    def upsert_price_adjustment_factors(self, rows: Iterable[Dict]) -> int:
        payload = [
            (r["instrument_id"], r["trade_date"], r.get("as_of_date", "9999-12-31"), r["factor"], r["cumulative_factor"], r["created_at"], r.get("run_id"))
            for r in rows
        ]
        if not payload:
            return 0
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO price_adjustment_factors(
                        instrument_id, trade_date, as_of_date, factor, cumulative_factor, created_at, run_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(instrument_id, trade_date, as_of_date) DO UPDATE SET
                        factor=excluded.factor,
                        cumulative_factor=excluded.cumulative_factor,
                        created_at=excluded.created_at,
                        run_id=excluded.run_id
                    """,
                    payload,
                )
        return len(payload)

    def clear_price_adjustment_factors(self, date_from: str, date_to: str, as_of_date: str = "9999-12-31") -> int:
        with self.connect() as conn:
            cur = conn.execute(
                "DELETE FROM price_adjustment_factors WHERE trade_date BETWEEN %s AND %s AND as_of_date = %s",
                (date_from, date_to, as_of_date),
            )
            return int(cur.rowcount or 0)

    def get_market_adjustment_inputs(self, date_from: str, date_to: str) -> List[Dict]:
        return self.query(
            """
            WITH ranked AS (
                SELECT instrument_id,
                       trade_date,
                       close,
                       base_price,
                       listed_shares,
                       LAG(close) OVER (
                           PARTITION BY instrument_id
                           ORDER BY trade_date
                       ) AS prev_close,
                       LAG(listed_shares) OVER (
                           PARTITION BY instrument_id
                           ORDER BY trade_date
                       ) AS prev_listed_shares
                FROM daily_market_data
            )
            SELECT instrument_id, trade_date, close, base_price, listed_shares, prev_close, prev_listed_shares
            FROM ranked
            WHERE trade_date BETWEEN %s AND %s
            ORDER BY instrument_id, trade_date
            """,
            (date_from, date_to),
        )

    def get_existing_instrument_ids(self, instrument_ids: Iterable[str]) -> set[str]:
        ids = [str(x).strip() for x in instrument_ids if str(x).strip()]
        if not ids:
            return set()
        placeholders = ", ".join(["%s"] * len(ids))
        rows = self.query(f"SELECT instrument_id FROM instruments WHERE instrument_id IN ({placeholders})", tuple(ids))
        return {str(row["instrument_id"]) for row in rows if row.get("instrument_id")}

    def get_instrument_id_by_external_code(self, external_code: str, market_code: Optional[str] = None) -> Optional[str]:
        if not external_code:
            return None
        if market_code:
            rows = self.query(
                "SELECT instrument_id FROM instruments WHERE external_code = %s AND market_code = %s LIMIT 1",
                (external_code, str(market_code).upper()),
            )
        else:
            rows = self.query(
                "SELECT instrument_id FROM instruments WHERE external_code = %s ORDER BY (delisting_date IS NULL) DESC, listing_date DESC LIMIT 1",
                (external_code,),
            )
        return rows[0]["instrument_id"] if rows else None

    def list_instruments(self, search: str = "", listed_status: str = "", limit: int = 50, offset: int = 0) -> Dict:
        where_clauses = []
        params: List = []
        if search:
            pattern = f"%{search}%"
            where_clauses.append("(i.instrument_name ILIKE %s OR i.external_code ILIKE %s)")
            params.extend([pattern, pattern])
        status = str(listed_status or "").lower()
        if status == "listed":
            where_clauses.append("i.delisting_date IS NULL")
        elif status == "delisted":
            where_clauses.append("i.delisting_date IS NOT NULL")
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        total = self.query(f"SELECT COUNT(*) AS cnt FROM instruments i {where_sql}", tuple(params))[0]["cnt"]
        rows = self.query(
            f"""
            SELECT i.external_code, i.market_code, i.instrument_name, i.listing_date, i.delisting_date,
                   CASE WHEN i.delisting_date IS NULL THEN 'listed' ELSE 'delisted' END AS listed_status,
                   ds.delisting_reason, ds.note AS delisting_note
            FROM instruments i
            LEFT JOIN instrument_delisting_snapshot ds
              ON ds.market_code = i.market_code
             AND ds.external_code = i.external_code
            {where_sql}
            ORDER BY (i.delisting_date IS NULL) DESC, i.market_code ASC, i.external_code ASC
            LIMIT %s OFFSET %s
            """,
            tuple(params + [limit, offset]),
        )
        return {"total": total, "items": rows, "limit": limit, "offset": offset}

    def get_instrument_profile(self, external_code: str) -> Dict:
        rows = self.query(
            """
            SELECT i.instrument_id, i.external_code, i.instrument_name, i.market_code,
                   i.listing_date, i.delisting_date, i.listed_shares,
                   ds.delisting_reason, ds.note AS delisting_note,
                   CASE WHEN i.delisting_date IS NULL THEN 'listed' ELSE 'delisted' END AS listed_status
            FROM instruments i
            LEFT JOIN instrument_delisting_snapshot ds
              ON ds.market_code = i.market_code
             AND ds.external_code = i.external_code
            WHERE i.external_code = %s
            ORDER BY (i.delisting_date IS NULL) DESC, i.listing_date DESC
            LIMIT 1
            """,
            (external_code.strip(),),
        )
        return rows[0] if rows else {}

    def get_instrument_daily(self, external_code: str, date_from: str = "", date_to: str = "", limit: int = 250, offset: int = 0) -> Dict:
        params: List = [external_code.strip()]
        where_date = ""
        if date_from:
            where_date += " AND trade_date >= %s"
            params.append(date_from)
        if date_to:
            where_date += " AND trade_date <= %s"
            params.append(date_to)
        total = self.query(
            f"SELECT COUNT(*) AS cnt FROM instrument_daily_v1 WHERE external_code = %s {where_date}",
            tuple(params),
        )[0]["cnt"]
        rows = self.query(
            f"""
            SELECT *
            FROM instrument_daily_v1
            WHERE external_code = %s {where_date}
            ORDER BY trade_date DESC
            LIMIT %s OFFSET %s
            """,
            tuple(params + [limit, offset]),
        )
        return {"total": total, "items": rows, "limit": limit, "offset": offset, "has_more": offset + len(rows) < total}

    def get_default_benchmark_series_map(self, index_codes: Iterable[str]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for code in [str(c).strip().upper() for c in index_codes if str(c).strip()]:
            preferred = self.DEFAULT_BENCHMARK_SERIES_CANDIDATES.get(code, [])
            rows = self.query(
                """
                SELECT index_name
                FROM benchmark_daily_v1
                WHERE index_code = %s
                GROUP BY index_name
                ORDER BY CASE
                    WHEN index_name = %s THEN 0
                    WHEN index_name = %s THEN 1
                    WHEN index_name = %s THEN 2
                    ELSE 3
                END, index_name
                LIMIT 1
                """,
                (code, preferred[0] if len(preferred) > 0 else "", preferred[1] if len(preferred) > 1 else "", code),
            )
            if rows:
                out[code] = rows[0]["index_name"]
        return out

    def get_benchmark_daily(self, index_code: str, series_name: str = "", date_from: str = "", date_to: str = "", limit: int = 250, offset: int = 0) -> Dict:
        selected_series = series_name.strip()
        if not selected_series:
            selected_series = self.get_default_benchmark_series_map([index_code]).get(str(index_code).upper(), "")
        params: List = [str(index_code).upper()]
        where_series = ""
        if selected_series:
            where_series = " AND index_name = %s"
            params.append(selected_series)
        where_date = ""
        if date_from:
            where_date += " AND trade_date >= %s"
            params.append(date_from)
        if date_to:
            where_date += " AND trade_date <= %s"
            params.append(date_to)
        total = self.query(
            f"SELECT COUNT(*) AS cnt FROM benchmark_daily_v1 WHERE index_code = %s {where_series} {where_date}",
            tuple(params),
        )[0]["cnt"]
        rows = self.query(
            f"""
            SELECT *
            FROM benchmark_daily_v1
            WHERE index_code = %s {where_series} {where_date}
            ORDER BY trade_date DESC, index_name
            LIMIT %s OFFSET %s
            """,
            tuple(params + [limit, offset]),
        )
        return {"total": total, "items": rows, "limit": limit, "offset": offset, "series_name": selected_series, "has_more": offset + len(rows) < total}

    def get_calendar(self, market_codes: Iterable[str], date_from: str, date_to: str) -> List[Dict]:
        codes = [str(c).upper() for c in market_codes if str(c).strip()]
        if not codes:
            return []
        placeholders = ", ".join(["%s"] * len(codes))
        return self.query(
            f"SELECT market_code, trade_date, is_open, holiday_name FROM trading_calendar_v1 WHERE market_code IN ({placeholders}) AND trade_date BETWEEN %s AND %s ORDER BY market_code, trade_date",
            tuple(codes + [date_from, date_to]),
        )

    def get_latest_trade_date(self) -> Optional[str]:
        rows = self.query("SELECT MAX(trade_date) AS latest_trade_date FROM daily_market_data")
        return rows[0].get("latest_trade_date") if rows else None