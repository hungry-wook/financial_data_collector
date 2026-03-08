import argparse
import json
import os

from .repository import Repository


def main() -> None:
    parser = argparse.ArgumentParser(description="Update corporate event review status")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--source-event-id", required=True)
    parser.add_argument("--status", required=True, choices=["ACTIVE", "NEEDS_REVIEW", "REJECTED"])
    parser.add_argument("--raw-factor", type=float, default=None)
    parser.add_argument("--confidence", default="MEDIUM", choices=["HIGH", "MEDIUM", "LOW"])
    args = parser.parse_args()

    if not args.database_url:
        raise ValueError("--database-url or DATABASE_URL is required")

    repo = Repository(args.database_url)
    updates = {"status": args.status, "confidence": args.confidence}
    if args.raw_factor is not None:
        updates["raw_factor"] = args.raw_factor

    set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
    params = [*updates.values(), args.source_event_id]
    with repo.connect() as conn:
        cur = conn.execute(
            f"""
            UPDATE corporate_events
            SET {set_clause}
            WHERE source_event_id = %s
            """,
            tuple(params),
        )
        changed = int(cur.rowcount or 0)

    print(json.dumps({"updated": changed, "source_event_id": args.source_event_id, "status": args.status}, ensure_ascii=False))


if __name__ == "__main__":
    main()
