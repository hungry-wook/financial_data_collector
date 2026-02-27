import os
import time
from datetime import date, timedelta

import pytest

from financial_data_collector.collectors import BenchmarkCollector, InstrumentCollector
from financial_data_collector.export_service import ExportRequest, ExportService


pytestmark = pytest.mark.performance


class NullWriter:
    def write(self, path, rows):
        return len(rows)

    def sha256(self, path):
        return "na"

    def write_manifest(self, path, manifest):
        path.write_text("{}", encoding="utf-8")


def _business_days(start: date, days: int):
    out = []
    current = start
    while len(out) < days:
        if current.weekday() < 5:
            out.append(current)
        current += timedelta(days=1)
    return out


def _perf_instrument_uuid(idx: int) -> str:
    return f"00000000-0000-0000-0000-{idx:012d}"


@pytest.mark.skipif(os.getenv("RUN_PERF_TESTS") != "1", reason="Set RUN_PERF_TESTS=1 to run performance tests")
def test_export_performance_1y_kosdaq(repo, tmp_path):
    start = date(2025, 1, 2)
    trade_days = _business_days(start, 252)

    instruments = [
        {
            "instrument_id": _perf_instrument_uuid(idx),
            "external_code": f"{idx:04d}",
            "market_code": "KOSDAQ",
            "instrument_name": f"inst-{idx}",
            "listing_date": "2020-01-01",
            "delisting_date": None,
            "source_name": "perf",
            "collected_at": "2026-01-01T00:00:00",
            "updated_at": "2026-01-01T00:00:00",
        }
        for idx in range(200)
    ]
    repo.upsert_instruments(instruments)

    daily_rows = []
    for td in trade_days:
        td_str = td.isoformat()
        for idx in range(200):
            base = 100 + idx
            daily_rows.append(
                {
                    "instrument_id": _perf_instrument_uuid(idx),
                    "trade_date": td_str,
                    "open": base,
                    "high": base + 2,
                    "low": base - 1,
                    "close": base + 1,
                    "volume": 1000 + idx,
                    "turnover_value": float((1000 + idx) * (base + 1)),
                    "market_value": float((base + 1) * 100000),
                    "is_trade_halted": False,
                    "is_under_supervision": False,
                    "record_status": "VALID",
                    "source_name": "perf",
                    "collected_at": "2026-01-01T00:00:00",
                    "run_id": None,
                }
            )
    repo.upsert_daily_market(daily_rows)

    BenchmarkCollector(repo).collect(
        [
            {"index_code": "KOSDAQ", "trade_date": td, "open": 1000, "high": 1005, "low": 995, "close": 1002}
            for td in trade_days
        ],
        "perf",
        "perf_run",
    )
    repo.upsert_trading_calendar(
        [
            {
                "market_code": "KOSDAQ",
                "trade_date": td.isoformat(),
                "is_open": True,
                "holiday_name": None,
                "source_name": "perf",
                "collected_at": "2026-01-01T00:00:00",
                "run_id": None,
            }
            for td in trade_days
        ]
    )

    InstrumentCollector(repo).collect(
        [
            {
                "instrument_id": "i_perf_unused",
                "external_code": "9999",
                "market_code": "KOSDAQ",
                "instrument_name": "unused",
                "listing_date": date(2020, 1, 1),
            }
        ],
        "perf",
    )

    svc = ExportService(repo, writer=NullWriter())
    created = svc.create_job(
        ExportRequest(
            market_codes=["KOSDAQ"],
            index_codes=["KOSDAQ"],
            date_from=trade_days[0].isoformat(),
            date_to=trade_days[-1].isoformat(),
            include_issues=False,
            output_format="parquet",
            output_path=(tmp_path / "perf_out").as_posix(),
        )
    )

    started = time.perf_counter()
    result = svc.run_job(created["job_id"])
    elapsed_sec = time.perf_counter() - started

    assert result["status"] == "SUCCEEDED"
    assert result["row_counts"]["instrument_daily"] == len(trade_days) * 200
    assert elapsed_sec < float(os.getenv("PERF_MAX_EXPORT_SECONDS", "10"))
