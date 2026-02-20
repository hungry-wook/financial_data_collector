from datetime import datetime, timezone
from typing import Dict, List

from .repository import Repository


class ValidationJob:
    def __init__(self, repo: Repository):
        self.repo = repo

    def validate_range(self, market_code: str, date_from: str, date_to: str, run_id: str) -> Dict[str, int]:
        issues: List[Dict] = []
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        rows = self.repo.query(
            """
            SELECT d.instrument_id, d.trade_date, d.open, d.high, d.low, d.close, d.volume, d.turnover_value, d.market_value, d.is_trade_halted
            FROM daily_market_data d
            JOIN instruments i ON i.instrument_id = d.instrument_id
            WHERE i.market_code = ?
              AND d.trade_date BETWEEN ? AND ?
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
                            run_id,
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
                            run_id,
                            now,
                        )
                    )
            if r["volume"] < 0:
                issues.append(
                    self._issue("daily_market_data", r["trade_date"], r["instrument_id"], None, "NEGATIVE_VOLUME", "ERROR", run_id, now)
                )
            if r["turnover_value"] is not None and r["turnover_value"] < 0:
                issues.append(
                    self._issue("daily_market_data", r["trade_date"], r["instrument_id"], None, "NEGATIVE_TURNOVER", "ERROR", run_id, now)
                )
            if r["market_value"] is not None and r["market_value"] < 0:
                issues.append(
                    self._issue("daily_market_data", r["trade_date"], r["instrument_id"], None, "NEGATIVE_MARKET_VALUE", "ERROR", run_id, now)
                )

        open_days = self.repo.query(
            """
            SELECT trade_date
            FROM trading_calendar
            WHERE market_code = ?
              AND trade_date BETWEEN ? AND ?
              AND is_open = 1
            """,
            (market_code, date_from, date_to),
        )
        for d in open_days:
            count = self.repo.query(
                """
                SELECT COUNT(1) AS c
                FROM daily_market_data d
                JOIN instruments i ON i.instrument_id = d.instrument_id
                WHERE i.market_code = ?
                  AND d.trade_date = ?
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
                        run_id,
                        now,
                    )
                )

        if issues:
            self.repo.insert_issues(issues)

        return {"issues": len(issues), "rows_checked": len(rows)}

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
