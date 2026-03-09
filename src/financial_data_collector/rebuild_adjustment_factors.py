import argparse
import json
import os

from .adjustment_service import AdjustmentService
from .repository import Repository


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild materialized price adjustment factors")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--date-from", required=True)
    parser.add_argument("--date-to", required=True)
    parser.add_argument("--as-of-timestamp")
    parser.add_argument("--run-id")
    args = parser.parse_args()

    if not args.database_url:
        raise ValueError("--database-url or DATABASE_URL is required")

    repo = Repository(args.database_url)
    repo.init_schema()
    result = AdjustmentService(repo).rebuild_factors(
        date_from=args.date_from,
        date_to=args.date_to,
        as_of_timestamp=args.as_of_timestamp,
        run_id=args.run_id,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
