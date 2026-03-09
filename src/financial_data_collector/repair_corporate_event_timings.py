import argparse
import json
import os
from datetime import date, timedelta

from .adjustment_service import AdjustmentService
from .collect_dart_corporate_events import repair_corporate_event_timings
from .repository import Repository


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair stored corporate-event timing/status using saved payloads")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--date-from", help="begin date YYYY-MM-DD (default: today-30d)")
    parser.add_argument("--date-to", help="end date YYYY-MM-DD (default: today)")
    parser.add_argument("--rebuild-adjustments", action="store_true")
    parser.add_argument("--overlap-days", type=int, default=7)
    parser.add_argument("--as-of-timestamp")
    parser.add_argument("--run-id")
    args = parser.parse_args()

    if not args.database_url:
        raise ValueError("--database-url or DATABASE_URL is required")

    end_date = _parse_date(args.date_to) if args.date_to else date.today()
    start_date = _parse_date(args.date_from) if args.date_from else (end_date - timedelta(days=30))

    repo = Repository(args.database_url)
    result = repair_corporate_event_timings(repo, start_date.isoformat(), end_date.isoformat())

    if args.rebuild_adjustments:
        latest_trade_date = repo.get_latest_trade_date()
        impacted_window = AdjustmentService.compute_impacted_window(
            date_from=start_date.isoformat(),
            latest_trade_date=latest_trade_date,
            overlap_days=args.overlap_days,
        )
        if impacted_window is not None:
            result["rebuild"] = AdjustmentService(repo).rebuild_factors(
                date_from=impacted_window["date_from"],
                date_to=impacted_window["date_to"],
                as_of_timestamp=args.as_of_timestamp,
                run_id=args.run_id,
            )
            result["impacted_window"] = impacted_window
        else:
            result["rebuild"] = None
            result["impacted_window"] = None

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
