from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

from .repository import Repository


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class AdjustmentService:
    def __init__(self, repo: Repository):
        self.repo = repo

    @staticmethod
    def _resolve_event_factor(event: Dict) -> Optional[float]:
        raw_factor = event.get("raw_factor")
        if raw_factor is not None:
            try:
                f = float(raw_factor)
            except (TypeError, ValueError):
                return None
            return f if f > 0 else None

        payload = event.get("payload") or {}
        if not isinstance(payload, dict):
            return None

        # Generic fallback for normalized payload contracts.
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
        trade_dates = self.repo.get_market_trade_dates(date_from, date_to)

        events_by_instrument_date: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        for ev in events:
            instrument_id = ev.get("instrument_id")
            effective_date = ev.get("effective_date")
            if not instrument_id or not effective_date:
                continue
            factor = self._resolve_event_factor(ev)
            if factor is None:
                continue
            if events_by_instrument_date[instrument_id][effective_date] == 0:
                events_by_instrument_date[instrument_id][effective_date] = 1.0
            events_by_instrument_date[instrument_id][effective_date] *= factor

        trade_dates_by_instrument: Dict[str, List[str]] = defaultdict(list)
        for row in trade_dates:
            trade_dates_by_instrument[row["instrument_id"]].append(row["trade_date"])

        now = _utc_now_iso()
        rows = []
        for instrument_id, dates in trade_dates_by_instrument.items():
            dates_sorted = sorted(dates)
            if not dates_sorted:
                continue
            event_factor_by_date = events_by_instrument_date.get(instrument_id, {})
            cumulative = 1.0
            for td in reversed(dates_sorted):
                rows.append(
                    {
                        "instrument_id": instrument_id,
                        "trade_date": td,
                        "as_of_date": as_of_date,
                        "factor": float(event_factor_by_date.get(td, 1.0)),
                        "cumulative_factor": float(cumulative),
                        "factor_source": "corporate_event",
                        "confidence": "MEDIUM",
                        "created_at": now,
                        "run_id": run_id,
                    }
                )
                if td in event_factor_by_date:
                    cumulative *= float(event_factor_by_date[td])

        self.repo.clear_price_adjustment_factors(date_from=date_from, date_to=date_to, as_of_date=as_of_date)
        upserted = self.repo.upsert_price_adjustment_factors(rows)
        return {"events": len(events), "trade_dates": len(trade_dates), "factors": upserted}
