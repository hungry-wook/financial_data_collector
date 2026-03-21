from collections import defaultdict
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
    def _resolve_event_factor(event: Dict) -> Optional[float]:
        raw_factor = event.get("raw_factor")
        if raw_factor is not None:
            try:
                factor = float(raw_factor)
            except (TypeError, ValueError):
                return None
            return factor if factor > 0 else None

        payload = event.get("payload") or {}
        if not isinstance(payload, dict):
            return None

        ratio = payload.get("ratio")
        if ratio is None:
            return None
        try:
            ratio_f = float(ratio)
        except (TypeError, ValueError):
            return None
        if ratio_f <= 0:
            return None
        return ratio_f

    @staticmethod
    def _resolve_market_factor(row: Dict) -> Optional[float]:
        prev_listed_shares = row.get("prev_listed_shares")
        listed_shares = row.get("listed_shares")
        if prev_listed_shares in (None, "", 0) or listed_shares in (None, "", 0):
            return None
        try:
            prev_value = float(prev_listed_shares)
            curr_value = float(listed_shares)
        except (TypeError, ValueError):
            return None
        if prev_value <= 0 or curr_value <= 0:
            return None
        factor = prev_value / curr_value
        if not isfinite(factor) or factor <= 0:
            return None
        if abs(factor - 1.0) < 1e-12:
            return None
        return factor

    def rebuild_factors(
        self,
        date_from: str,
        date_to: str,
        as_of_timestamp: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> Dict[str, int]:
        as_of_date = "9999-12-31"
        if as_of_timestamp:
            as_of_date = str(as_of_timestamp).strip().split("T", 1)[0]

        events = self.repo.get_corporate_events_for_period(
            date_from=date_from,
            date_to=date_to,
            as_of_date=None if as_of_date == "9999-12-31" else as_of_date,
            statuses=["ACTIVE"],
        )
        trade_rows = self.repo.get_market_adjustment_inputs(date_from, date_to)

        event_factor_by_instrument_date: Dict[str, Dict[str, float]] = defaultdict(dict)
        for event in events:
            instrument_id = event.get("instrument_id")
            effective_date = event.get("effective_date")
            if not instrument_id or not effective_date:
                continue
            factor = self._resolve_event_factor(event)
            if factor is None:
                continue
            event_factor_by_instrument_date[instrument_id][effective_date] = float(factor)

        trade_rows_by_instrument: Dict[str, list[Dict]] = defaultdict(list)
        factor_by_instrument_date: Dict[str, Dict[str, float]] = defaultdict(dict)
        factor_source_by_instrument_date: Dict[str, Dict[str, str]] = defaultdict(dict)

        for row in trade_rows:
            instrument_id = row["instrument_id"]
            trade_date = row["trade_date"]
            trade_rows_by_instrument[instrument_id].append(row)

            market_factor = self._resolve_market_factor(row)
            if market_factor is not None:
                factor_by_instrument_date[instrument_id][trade_date] = float(market_factor)
                factor_source_by_instrument_date[instrument_id][trade_date] = "market_observed"
                continue

            event_factor = event_factor_by_instrument_date.get(instrument_id, {}).get(trade_date)
            if event_factor is not None:
                factor_by_instrument_date[instrument_id][trade_date] = float(event_factor)
                factor_source_by_instrument_date[instrument_id][trade_date] = "corporate_event_fallback"

        now = _utc_now_iso()
        rows = []
        for instrument_id, instrument_rows in trade_rows_by_instrument.items():
            rows_sorted = sorted(instrument_rows, key=lambda row: row["trade_date"])
            if not rows_sorted:
                continue
            factor_by_date = factor_by_instrument_date.get(instrument_id, {})
            factor_source_by_date = factor_source_by_instrument_date.get(instrument_id, {})
            cumulative = 1.0
            for row in reversed(rows_sorted):
                trade_date = row["trade_date"]
                factor = float(factor_by_date.get(trade_date, 1.0))
                rows.append(
                    {
                        "instrument_id": instrument_id,
                        "trade_date": trade_date,
                        "as_of_date": as_of_date,
                        "factor": factor,
                        "cumulative_factor": float(cumulative),
                        "factor_source": factor_source_by_date.get(trade_date, "market_observed"),
                        "confidence": "HIGH" if trade_date in factor_by_date else "MEDIUM",
                        "created_at": now,
                        "run_id": run_id,
                    }
                )
                if trade_date in factor_by_date:
                    cumulative *= factor

        self.repo.clear_price_adjustment_factors(date_from=date_from, date_to=date_to, as_of_date=as_of_date)
        upserted = self.repo.upsert_price_adjustment_factors(rows)
        instrument_count = len(trade_rows_by_instrument)
        event_date_count = sum(len(by_date) for by_date in factor_by_instrument_date.values())
        return {
            "events": len(events),
            "trade_dates": len(trade_rows),
            "factors": upserted,
            "instrument_count": instrument_count,
            "event_date_count": event_date_count,
        }
