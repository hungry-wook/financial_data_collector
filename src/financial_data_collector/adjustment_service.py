from datetime import date, datetime, timedelta, timezone
from math import isfinite
from typing import Dict, Optional

from .repository import Repository


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class AdjustmentService:
    def __init__(self, repo: Repository):
        self.repo = repo

    @staticmethod
    def compute_impacted_window(date_from: str, latest_trade_date: Optional[str], overlap_days: int = 7) -> Optional[Dict[str, str]]:
        if not latest_trade_date:
            return None
        start = date.fromisoformat(str(date_from)) - timedelta(days=max(overlap_days, 0))
        end = date.fromisoformat(str(latest_trade_date))
        if start > end:
            start = end
        return {"date_from": start.isoformat(), "date_to": end.isoformat()}

    @staticmethod
    def _resolve_factor(row: Dict) -> float:
        base_price = row.get("base_price")
        prev_close = row.get("prev_close")
        if base_price in (None, "") or prev_close in (None, "", 0):
            return 1.0
        try:
            base_value = float(base_price)
            prev_value = float(prev_close)
        except (TypeError, ValueError):
            return 1.0
        if base_value <= 0 or prev_value <= 0:
            return 1.0
        factor = base_value / prev_value
        if not isfinite(factor) or factor <= 0:
            return 1.0
        return float(factor)

    def rebuild_factors(self, date_from: str, date_to: str, as_of_timestamp: Optional[str] = None, run_id: Optional[str] = None) -> Dict[str, int]:
        as_of_date = "9999-12-31"
        if as_of_timestamp:
            as_of_date = str(as_of_timestamp).strip().split("T", 1)[0]

        trade_rows = self.repo.get_market_adjustment_inputs(date_from, date_to)
        rows_by_instrument: Dict[str, list[Dict]] = {}
        for row in trade_rows:
            rows_by_instrument.setdefault(row["instrument_id"], []).append(row)

        now = _utc_now_iso()
        rows = []
        for instrument_id, instrument_rows in rows_by_instrument.items():
            rows_sorted = sorted(instrument_rows, key=lambda row: row["trade_date"])
            cumulative = 1.0
            factors_by_date = {row["trade_date"]: self._resolve_factor(row) for row in rows_sorted}
            for row in reversed(rows_sorted):
                trade_date = row["trade_date"]
                factor = float(factors_by_date.get(trade_date, 1.0))
                rows.append(
                    {
                        "instrument_id": instrument_id,
                        "trade_date": trade_date,
                        "as_of_date": as_of_date,
                        "factor": factor,
                        "cumulative_factor": float(cumulative),
                        "created_at": now,
                        "run_id": run_id,
                    }
                )
                cumulative *= factor

        self.repo.clear_price_adjustment_factors(date_from=date_from, date_to=date_to, as_of_date=as_of_date)
        upserted = self.repo.upsert_price_adjustment_factors(rows)
        return {
            "trade_dates": len(trade_rows),
            "factors": upserted,
            "instrument_count": len(rows_by_instrument),
        }