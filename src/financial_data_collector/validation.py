from datetime import datetime, timezone
from typing import Dict, List
from uuid import UUID

from .repository import Repository


class ValidationJob:
    def __init__(self, repo: Repository):
        self.repo = repo

    def validate_range(self, market_code: str, date_from: str, date_to: str, run_id: str) -> Dict[str, int]:
        issues: List[Dict] = []
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        persisted_run_id = None
        if run_id:
            try:
                normalized = str(UUID(str(run_id)))
                if self.repo.query("SELECT run_id FROM collection_runs WHERE run_id = %s", (normalized,)):
                    persisted_run_id = normalized
            except ValueError:
                persisted_run_id = None

        rows = self.repo.query(
            """
            SELECT d.instrument_id, d.trade_date, d.open, d.high, d.low, d.close, d.volume, d.turnover_value, d.market_value, d.is_trade_halted
            FROM daily_market_data d
            JOIN instruments i ON i.instrument_id = d.instrument_id
            WHERE i.market_code = %s
              AND d.trade_date BETWEEN %s AND %s
            """,
            (market_code, date_from, date_to),
        )
        for r in rows:
            if not bool(r.get("is_trade_halted")):
                if r["high"] < max(r["open"], r["close"], r["low"]):
                    issues.append(
                        self._issue(
                            "daily_market_data",
                            r["trade_date"],
                            r["instrument_id"],
                            None,
                            "OHLC_HIGH_INCONSISTENT",
                            "ERROR",
                            persisted_run_id,
                            now,
                        )
                    )
                if r["low"] > min(r["open"], r["close"], r["high"]):
                    issues.append(
                        self._issue(
                            "daily_market_data",
                            r["trade_date"],
                            r["instrument_id"],
                            None,
                            "OHLC_LOW_INCONSISTENT",
                            "ERROR",
                            persisted_run_id,
                            now,
                        )
                    )
            if r["volume"] < 0:
                issues.append(
                    self._issue("daily_market_data", r["trade_date"], r["instrument_id"], None, "NEGATIVE_VOLUME", "ERROR", persisted_run_id, now)
                )
            if r["turnover_value"] is not None and r["turnover_value"] < 0:
                issues.append(
                    self._issue("daily_market_data", r["trade_date"], r["instrument_id"], None, "NEGATIVE_TURNOVER", "ERROR", persisted_run_id, now)
                )
            if r["market_value"] is not None and r["market_value"] < 0:
                issues.append(
                    self._issue("daily_market_data", r["trade_date"], r["instrument_id"], None, "NEGATIVE_MARKET_VALUE", "ERROR", persisted_run_id, now)
                )

        open_days = self.repo.query(
            """
            SELECT trade_date
            FROM trading_calendar
            WHERE market_code = %s
              AND trade_date BETWEEN %s AND %s
              AND is_open = TRUE
            """,
            (market_code, date_from, date_to),
        )
        for d in open_days:
            count = self.repo.query(
                """
                SELECT COUNT(1) AS c
                FROM daily_market_data d
                JOIN instruments i ON i.instrument_id = d.instrument_id
                WHERE i.market_code = %s
                  AND d.trade_date = %s
                """,
                (market_code, d["trade_date"]),
            )[0]["c"]
            if count == 0:
                issues.append(
                    self._issue(
                        "daily_market_data",
                        d["trade_date"],
                        None,
                        None,
                        "OPEN_DAY_TOTAL_MISSING",
                        "WARN",
                        persisted_run_id,
                        now,
                    )
                )

        if issues:
            self.repo.insert_issues(issues)

        errors = sum(1 for issue in issues if issue["severity"] == "ERROR")
        warnings = sum(1 for issue in issues if issue["severity"] == "WARN")
        infos = sum(1 for issue in issues if issue["severity"] == "INFO")
        return {
            "issues_total": len(issues),
            "errors": errors,
            "warnings": warnings,
            "infos": infos,
            "rows_checked": len(rows),
        }

    @staticmethod
    def _issue(dataset_name, trade_date, instrument_id, index_code, issue_code, severity, run_id, detected_at):
        return {
            "dataset_name": dataset_name,
            "trade_date": trade_date,
            "instrument_id": instrument_id,
            "index_code": index_code,
            "issue_code": issue_code,
            "severity": severity,
            "issue_detail": issue_code,
            "source_name": "validation",
            "detected_at": detected_at,
            "run_id": run_id,
            "resolved_at": None,
        }
