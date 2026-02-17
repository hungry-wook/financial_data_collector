from datetime import datetime
from typing import Dict, List

from .repository import Repository


def _to_iso(value) -> str:
    if value is None:
        return None
    return str(value)


class InstrumentCollector:
    def __init__(self, repo: Repository):
        self.repo = repo

    def collect(self, rows: List[Dict], source_name: str) -> int:
        now = datetime.utcnow().isoformat()
        normalized = []
        for r in rows:
            if not r.get("instrument_id") or not r.get("external_code") or not r.get("listing_date"):
                continue
            normalized.append(
                {
                    "instrument_id": r["instrument_id"],
                    "external_code": r["external_code"],
                    "market_code": r["market_code"].upper(),
                    "instrument_name": r.get("instrument_name", r["external_code"]),
                    "listing_date": _to_iso(r["listing_date"]),
                    "delisting_date": _to_iso(r.get("delisting_date")),
                    "source_name": source_name,
                    "collected_at": now,
                    "updated_at": now,
                }
            )
        if normalized:
            self.repo.upsert_instruments(normalized)
        return len(normalized)


class DailyMarketCollector:
    def __init__(self, repo: Repository):
        self.repo = repo

    def collect(self, rows: List[Dict], source_name: str, run_id: str) -> int:
        now = datetime.utcnow().isoformat()
        normalized = []
        for r in rows:
            normalized.append(
                {
                    "instrument_id": r["instrument_id"],
                    "trade_date": _to_iso(r["trade_date"]),
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                    "volume": int(r["volume"]),
                    "turnover_value": r.get("turnover_value"),
                    "market_value": r.get("market_value"),
                    "is_trade_halted": bool(r.get("is_trade_halted", False)),
                    "is_under_supervision": bool(r.get("is_under_supervision", False)),
                    "record_status": r.get("record_status", "VALID"),
                    "source_name": source_name,
                    "collected_at": now,
                    "run_id": run_id,
                }
            )
        if normalized:
            self.repo.upsert_daily_market(normalized)
        return len(normalized)


class BenchmarkCollector:
    def __init__(self, repo: Repository):
        self.repo = repo

    def collect(self, rows: List[Dict], source_name: str, run_id: str) -> int:
        now = datetime.utcnow().isoformat()
        normalized = []
        for r in rows:
            normalized.append(
                {
                    "index_code": r["index_code"],
                    "trade_date": _to_iso(r["trade_date"]),
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                    "source_name": source_name,
                    "collected_at": now,
                    "run_id": run_id,
                }
            )
        if normalized:
            self.repo.upsert_benchmark(normalized)
        return len(normalized)

