import argparse
import json
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from .kind_client import KINDClient
from .repository import Repository
from .runs import RunManager


MARKET_TYPES = ("1", "2", "6")


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _deduplicate_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in rows:
        key = (row["market_code"], row["external_code"])
        existing = by_key.get(key)
        if not existing or row["delisting_date"] >= existing["delisting_date"]:
            by_key[key] = row
    return list(by_key.values())


def run_kind_delisting_collection(
    database_url: str,
    date_from: date,
    date_to: date,
    source_name: str = "kind",
    client: Optional[KINDClient] = None,
    schema: Optional[str] = None,
) -> Dict[str, Any]:
    if date_from > date_to:
        raise ValueError("date-from must be <= date-to")

    repo = Repository(database_url, schema=schema)
    repo.init_schema()
    kind_client = client or KINDClient()
    run_manager = RunManager(repo)
    run_id = run_manager.start(
        "phase1-collect-delisting-kind",
        source_name,
        date_from.isoformat(),
        date_to.isoformat(),
    )

    collected_rows: List[Dict[str, Any]] = []
    try:
        for market_type in MARKET_TYPES:
            collected_rows.extend(
                kind_client.fetch_delistings(
                    market_type=market_type,
                    date_from=date_from.isoformat(),
                    date_to=date_to.isoformat(),
                    source_name=source_name,
                )
            )
        deduped = _deduplicate_rows(collected_rows)
        snapshot_result = repo.upsert_delisting_snapshot(deduped, source_name=source_name, run_id=run_id)
        result = repo.bulk_update_delisting_dates(deduped, source_name=source_name, run_id=run_id)
        run_manager.finish(
            run_id=run_id,
            success_count=result["updated"] + result["unchanged"],
            failure_count=result["invalid"] + snapshot_result["invalid"],
            warning_count=result["unmatched"],
        )
        return {
            "run_id": run_id,
            "collected": len(collected_rows),
            "deduped": len(deduped),
            "snapshot": snapshot_result,
            "result": result,
        }
    except Exception:
        run_manager.fail(run_id)
        raise


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect KIND delisting dates and update instrument master.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--date-from", default="1900-01-01", help="YYYY-MM-DD")
    parser.add_argument("--date-to", default=date.today().isoformat(), help="YYYY-MM-DD")
    parser.add_argument("--source-name", default="kind")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    result = run_kind_delisting_collection(
        database_url=args.database_url,
        date_from=_parse_date(args.date_from),
        date_to=_parse_date(args.date_to),
        source_name=args.source_name,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
