from datetime import date, datetime, timedelta, timezone
from typing import Dict, List
from uuid import UUID, uuid5

from .repository import Repository

UUID_COERCE_NAMESPACE = UUID("7c76f04a-fca0-494d-96f8-6a68f1f21e84")


def _to_iso(value) -> str:
    if value is None:
        return None
    return str(value)


def _normalize_date(value) -> str:
    if value is None:
        raise ValueError("date is required")
    if isinstance(value, date):
        return value.isoformat()
    parsed = date.fromisoformat(str(value))
    return parsed.isoformat()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _coerce_uuid(value: str) -> str:
    if value is None:
        return value
    raw = str(value).strip()
    if not raw:
        return raw
    try:
        return str(UUID(raw))
    except ValueError:
        return str(uuid5(UUID_COERCE_NAMESPACE, raw))


def _resolve_existing_run_id(repo: Repository, run_id: str) -> str:
    if not run_id:
        return None
    normalized = _coerce_uuid(run_id)
    rows = repo.query("SELECT run_id FROM collection_runs WHERE run_id = %s", (normalized,))
    return normalized if rows else None


def _issue(
    dataset_name: str,
    issue_code: str,
    severity: str,
    source_name: str,
    detected_at: str,
    run_id: str = None,
    trade_date: str = None,
    instrument_id: str = None,
    index_code: str = None,
    issue_detail: str = None,
) -> Dict:
    return {
        "dataset_name": dataset_name,
        "trade_date": trade_date,
        "instrument_id": instrument_id,
        "index_code": index_code,
        "issue_code": issue_code,
        "severity": severity,
        "issue_detail": issue_detail or issue_code,
        "source_name": source_name,
        "detected_at": detected_at,
        "run_id": run_id,
        "resolved_at": None,
    }


class InstrumentCollector:
    def __init__(self, repo: Repository):
        self.repo = repo

    def collect(self, rows: List[Dict], source_name: str) -> int:
        now = _utc_now_iso()
        normalized = []
        issues = []
        for r in rows:
            if not r.get("instrument_id") or not r.get("external_code") or not r.get("listing_date"):
                continue
            try:
                listing_date = _normalize_date(r["listing_date"])
                delisting_date = _normalize_date(r.get("delisting_date")) if r.get("delisting_date") else None
            except ValueError:
                issues.append(
                    _issue(
                        dataset_name="instruments",
                        issue_code="DATE_NORMALIZATION_FAILED",
                        severity="WARN",
                        source_name=source_name,
                        detected_at=now,
                        instrument_id=None,
                        issue_detail=f"listing_date={r.get('listing_date')}, delisting_date={r.get('delisting_date')}",
                    )
                )
                continue
            normalized.append(
                {
                    "instrument_id": _coerce_uuid(r["instrument_id"]),
                    "external_code": r["external_code"],
                    "market_code": r["market_code"].upper(),
                    "instrument_name": r.get("instrument_name", r["external_code"]),
                    "instrument_name_abbr": r.get("instrument_name_abbr"),
                    "instrument_name_eng": r.get("instrument_name_eng"),
                    "listing_date": listing_date,
                    "delisting_date": delisting_date,
                    "listed_shares": r.get("listed_shares"),
                    "security_group": r.get("security_group"),
                    "sector_name": r.get("sector_name"),
                    "stock_type": r.get("stock_type"),
                    "par_value": r.get("par_value"),
                    "source_name": source_name,
                    "collected_at": now,
                    "updated_at": now,
                }
            )
        if normalized:
            self.repo.upsert_instruments(normalized)
        if issues:
            self.repo.insert_issues(issues)
        return len(normalized)


class DailyMarketCollector:
    def __init__(self, repo: Repository):
        self.repo = repo

    def collect(self, rows: List[Dict], source_name: str, run_id: str) -> int:
        now = _utc_now_iso()
        persisted_run_id = _resolve_existing_run_id(self.repo, run_id)
        normalized = []
        issues = []
        for r in rows:
            instrument_id = r.get("instrument_id")
            normalized_instrument_id = _coerce_uuid(instrument_id) if instrument_id else None
            try:
                trade_date = _normalize_date(r.get("trade_date"))
                open_price = float(r["open"])
                high_price = float(r["high"])
                low_price = float(r["low"])
                close_price = float(r["close"])
                volume = int(r["volume"])
                turnover_value = float(r["turnover_value"]) if r.get("turnover_value") is not None else None
                market_value = float(r["market_value"]) if r.get("market_value") is not None else None

                if high_price < max(open_price, close_price, low_price):
                    raise ValueError("high is inconsistent")
                if low_price > min(open_price, close_price, high_price):
                    raise ValueError("low is inconsistent")
                if volume < 0:
                    raise ValueError("volume must be non-negative")
                if turnover_value is not None and turnover_value < 0:
                    raise ValueError("turnover_value must be non-negative")
                if market_value is not None and market_value < 0:
                    raise ValueError("market_value must be non-negative")

                normalized.append(
                    {
                        "instrument_id": normalized_instrument_id,
                        "external_code": r.get("external_code"),
                        "market_code": str(r.get("market_code", "")).upper() if r.get("market_code") else None,
                        "trade_date": trade_date,
                        "open": open_price,
                        "high": high_price,
                        "low": low_price,
                        "close": close_price,
                        "volume": volume,
                        "turnover_value": turnover_value,
                        "market_value": market_value,
                        "price_change": float(r["price_change"]) if r.get("price_change") is not None else None,
                        "change_rate": float(r["change_rate"]) if r.get("change_rate") is not None else None,
                        "listed_shares": r.get("listed_shares"),
                        "is_trade_halted": bool(r.get("is_trade_halted", False)),
                        "is_under_supervision": bool(r.get("is_under_supervision", False)),
                        "record_status": r.get("record_status", "VALID"),
                        "source_name": source_name,
                        "collected_at": now,
                        "run_id": persisted_run_id,
                    }
                )
            except (KeyError, TypeError, ValueError) as exc:
                issues.append(
                    _issue(
                        dataset_name="daily_market_data",
                        issue_code="INVALID_DAILY_MARKET_ROW",
                        severity="ERROR",
                        source_name=source_name,
                        detected_at=now,
                        run_id=persisted_run_id,
                        trade_date=_to_iso(r.get("trade_date")),
                        instrument_id=normalized_instrument_id,
                        issue_detail=str(exc),
                    )
                )
        if normalized:
            # Backfill minimal instrument master rows when daily rows arrive first.
            instrument_ids = {r["instrument_id"] for r in normalized if r.get("instrument_id")}
            missing_rows = []
            for iid in instrument_ids:
                exists = self.repo.query("SELECT 1 AS ok FROM instruments WHERE instrument_id = %s LIMIT 1", (iid,))
                if exists:
                    continue
                sample = next((r for r in normalized if r.get("instrument_id") == iid), None)
                if not sample:
                    continue
                external_code = str(sample.get("external_code") or iid)[:20]
                market_code = str(sample.get("market_code") or "UNKNOWN").upper()[:20]
                listing_date = sample.get("trade_date")
                missing_rows.append(
                    {
                        "instrument_id": iid,
                        "external_code": external_code,
                        "market_code": market_code,
                        "instrument_name": external_code,
                        "instrument_name_abbr": external_code,
                        "instrument_name_eng": external_code,
                        "listing_date": listing_date,
                        "delisting_date": None,
                        "listed_shares": None,
                        "security_group": None,
                        "sector_name": None,
                        "stock_type": None,
                        "par_value": None,
                        "source_name": source_name,
                        "collected_at": now,
                        "updated_at": now,
                    }
                )
            if missing_rows:
                self.repo.upsert_instruments(missing_rows)
            self.repo.upsert_daily_market(normalized)
        if issues:
            self.repo.insert_issues(issues)
        return len(normalized)


class BenchmarkCollector:
    def __init__(self, repo: Repository, index_code_map: Dict[str, str] = None):
        self.repo = repo
        self.index_code_map = index_code_map or {"KOSDAQ": "KOSDAQ", "KOSPI": "KOSPI"}

    def collect(self, rows: List[Dict], source_name: str, run_id: str) -> int:
        now = _utc_now_iso()
        persisted_run_id = _resolve_existing_run_id(self.repo, run_id)
        normalized = []
        issues = []
        dates_by_series: Dict[tuple, set] = {}
        for r in rows:
            raw_index_code = str(r.get("index_code", "")).upper()
            if raw_index_code not in self.index_code_map:
                issues.append(
                    _issue(
                        dataset_name="benchmark_index_data",
                        issue_code="UNMAPPED_INDEX_CODE",
                        severity="ERROR",
                        source_name=source_name,
                        detected_at=now,
                        run_id=persisted_run_id,
                        trade_date=_to_iso(r.get("trade_date")),
                        index_code=raw_index_code or None,
                        issue_detail=f"index_code={raw_index_code}",
                    )
                )
                continue

            index_code = self.index_code_map[raw_index_code]
            index_name = str(r.get("index_name") or index_code).strip() or index_code
            try:
                trade_date = _normalize_date(r.get("trade_date"))
                close_raw = r.get("close")
                if close_raw is None:
                    raise ValueError("close is required")

                open_raw = r.get("open")
                high_raw = r.get("high")
                low_raw = r.get("low")

                open_price = float(open_raw) if open_raw is not None else None
                high_price = float(high_raw) if high_raw is not None else None
                low_price = float(low_raw) if low_raw is not None else None
                close_price = float(close_raw)

                status = str(r.get("record_status") or "VALID").upper()
                if status not in {"VALID", "PARTIAL", "INVALID"}:
                    status = "VALID"
                if status == "VALID" and None in (open_price, high_price, low_price):
                    status = "PARTIAL"

                if status == "VALID":
                    if high_price < max(open_price, close_price, low_price):
                        raise ValueError("high is inconsistent")
                    if low_price > min(open_price, close_price, high_price):
                        raise ValueError("low is inconsistent")
            except (KeyError, TypeError, ValueError) as exc:
                issues.append(
                    _issue(
                        dataset_name="benchmark_index_data",
                        issue_code="INVALID_BENCHMARK_ROW",
                        severity="ERROR",
                        source_name=source_name,
                        detected_at=now,
                        run_id=persisted_run_id,
                        trade_date=_to_iso(r.get("trade_date")),
                        index_code=index_code,
                        issue_detail=f"{index_name}: {exc}",
                    )
                )
                continue

            volume = r.get("volume")
            turnover_value = r.get("turnover_value")
            market_cap = r.get("market_cap")
            price_change = r.get("price_change")
            change_rate = r.get("change_rate")

            if status == "PARTIAL":
                issues.append(
                    _issue(
                        dataset_name="benchmark_index_data",
                        issue_code="BENCHMARK_PARTIAL_OHLC",
                        severity="WARN",
                        source_name=source_name,
                        detected_at=now,
                        run_id=persisted_run_id,
                        trade_date=trade_date,
                        index_code=index_code,
                        issue_detail=f"index_name={index_name}",
                    )
                )

            normalized.append(
                {
                    "index_code": index_code,
                    "index_name": index_name,
                    "trade_date": trade_date,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "raw_open": r.get("raw_open"),
                    "raw_high": r.get("raw_high"),
                    "raw_low": r.get("raw_low"),
                    "raw_close": r.get("raw_close"),
                    "volume": int(volume) if volume is not None else None,
                    "turnover_value": float(turnover_value) if turnover_value is not None else None,
                    "market_cap": float(market_cap) if market_cap is not None else None,
                    "price_change": float(price_change) if price_change is not None else None,
                    "change_rate": float(change_rate) if change_rate is not None else None,
                    "record_status": status,
                    "source_name": source_name,
                    "collected_at": now,
                    "run_id": persisted_run_id,
                }
            )
            dates_by_series.setdefault((index_code, index_name), set()).add(date.fromisoformat(trade_date))

        for (index_code, index_name), trade_dates in dates_by_series.items():
            if len(trade_dates) <= 1:
                continue
            start = min(trade_dates)
            end = max(trade_dates)
            current = start
            while current <= end:
                if current not in trade_dates:
                    issues.append(
                        _issue(
                            dataset_name="benchmark_index_data",
                            issue_code="BENCHMARK_DAY_MISSING",
                            severity="WARN",
                            source_name=source_name,
                            detected_at=now,
                            run_id=persisted_run_id,
                            trade_date=current.isoformat(),
                            index_code=index_code,
                            issue_detail=f"missing benchmark day for {index_code}/{index_name}",
                        )
                    )
                current += timedelta(days=1)

        if normalized:
            self.repo.upsert_benchmark(normalized)
        if issues:
            self.repo.insert_issues(issues)
        return len(normalized)
