from datetime import datetime, timedelta, timezone
from typing import Dict, List
from uuid import UUID

from .repository import Repository


class TradingCalendarBuilder:
    def __init__(self, repo: Repository):
        self.repo = repo

    def build_from_index_days(
        self,
        market_code: str,
        date_from,
        date_to,
        index_trade_dates: List,
        source_name: str,
        run_id: str,
    ) -> int:
        open_days = {str(d) for d in index_trade_dates}
        persisted_run_id = None
        if run_id:
            try:
                normalized = str(UUID(str(run_id)))
                if self.repo.query("SELECT run_id FROM collection_runs WHERE run_id = %s", (normalized,)):
                    persisted_run_id = normalized
            except ValueError:
                persisted_run_id = None
        current = date_from
        rows: List[Dict] = []
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        while current <= date_to:
            day = str(current)
            rows.append(
                {
                    "market_code": market_code,
                    "trade_date": day,
                    "is_open": day in open_days,
                    "holiday_name": None if day in open_days else "CLOSED",
                    "source_name": source_name,
                    "collected_at": now,
                    "run_id": persisted_run_id,
                }
            )
            current = current + timedelta(days=1)

        self.repo.upsert_trading_calendar(rows)
        return len(rows)
