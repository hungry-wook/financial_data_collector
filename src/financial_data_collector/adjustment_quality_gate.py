import argparse
import json
import os

from .repository import Repository


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Adjusted pipeline quality gate report")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--parse-success-threshold", type=float, default=70.0)
    parser.add_argument("--mismatch-threshold", type=float, default=1.0)
    parser.add_argument("--active-coverage-threshold", type=float, default=30.0)
    parser.add_argument("--mapping-success-threshold", type=float, default=70.0)
    parser.add_argument("--doc-download-success-threshold", type=float, default=80.0)
    args = parser.parse_args()

    if not args.database_url:
        raise ValueError("--database-url or DATABASE_URL is required")

    repo = Repository(args.database_url)

    total_events = _to_float(repo.query("SELECT COUNT(*) AS c FROM corporate_events")[0]["c"])
    active_events = _to_float(repo.query("SELECT COUNT(*) AS c FROM corporate_events WHERE status = 'ACTIVE'")[0]["c"])

    total_parse = _to_float(
        repo.query(
            """
            SELECT COUNT(DISTINCT source_event_id) AS c
            FROM event_validation_results
            WHERE check_name = 'DOCUMENT_FACTOR_PARSE'
            """
        )[0]["c"]
    )
    parse_fail = _to_float(
        repo.query(
            """
            SELECT COUNT(DISTINCT source_event_id) AS c
            FROM event_validation_results
            WHERE check_name = 'DOCUMENT_FACTOR_PARSE' AND result = 'PARSE_FAIL'
            """
        )[0]["c"]
    )
    mismatch = _to_float(repo.query("SELECT COUNT(*) AS c FROM event_validation_results WHERE result = 'MISMATCH'")[0]["c"])

    mapping_fail = _to_float(
        repo.query(
            """
            SELECT COUNT(DISTINCT source_event_id) AS c
            FROM event_validation_results
            WHERE check_name = 'INSTRUMENT_MAPPING' AND result = 'PARSE_FAIL'
            """
        )[0]["c"]
    )

    doc_download_fail = _to_float(
        repo.query(
            """
            SELECT COUNT(DISTINCT source_event_id) AS c
            FROM event_validation_results
            WHERE check_name = 'DOCUMENT_DOWNLOAD' AND result = 'PARSE_FAIL'
            """
        )[0]["c"]
    )

    parse_success_rate = 100.0 if total_parse == 0 else 100.0 * (total_parse - parse_fail) / total_parse
    mismatch_rate = 0.0 if total_parse == 0 else 100.0 * mismatch / total_parse
    active_coverage = 0.0 if total_events == 0 else 100.0 * active_events / total_events
    mapping_success = 100.0 if total_events == 0 else 100.0 * (total_events - mapping_fail) / total_events

    doc_attempted = max(total_events - mapping_fail, 0.0)
    doc_download_success = 100.0 if doc_attempted == 0 else 100.0 * (doc_attempted - doc_download_fail) / doc_attempted

    checks = {
        "parse_success": parse_success_rate >= args.parse_success_threshold,
        "mismatch_rate": mismatch_rate <= args.mismatch_threshold,
        "active_coverage": active_coverage >= args.active_coverage_threshold,
        "mapping_success": mapping_success >= args.mapping_success_threshold,
        "doc_download_success": doc_download_success >= args.doc_download_success_threshold,
    }

    report = {
        "metrics": {
            "total_events": int(total_events),
            "active_events": int(active_events),
            "total_parse_events": int(total_parse),
            "parse_fail_events": int(parse_fail),
            "mapping_fail_events": int(mapping_fail),
            "doc_download_fail_events": int(doc_download_fail),
            "parse_success_rate": round(parse_success_rate, 4),
            "mismatch_rate": round(mismatch_rate, 4),
            "active_coverage": round(active_coverage, 4),
            "mapping_success": round(mapping_success, 4),
            "doc_download_success": round(doc_download_success, 4),
        },
        "thresholds": {
            "parse_success": args.parse_success_threshold,
            "mismatch_rate": args.mismatch_threshold,
            "active_coverage": args.active_coverage_threshold,
            "mapping_success": args.mapping_success_threshold,
            "doc_download_success": args.doc_download_success_threshold,
        },
        "checks": checks,
        "passed": all(checks.values()),
    }

    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not report["passed"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
