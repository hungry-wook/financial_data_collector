import json
from datetime import date
from pathlib import Path

from financial_data_collector.api import BacktestExportAPI
from financial_data_collector.collectors import BenchmarkCollector, DailyMarketCollector, InstrumentCollector
from financial_data_collector.export_service import ExportRequest, ExportService
from financial_data_collector.runs import RunManager


class FakeParquetWriter:
    def write(self, path: Path, rows):
        path.write_text(json.dumps(rows), encoding="utf-8")
        return len(rows)

    def sha256(self, path: Path):
        return "fakehash"

    def write_manifest(self, path: Path, manifest):
        path.write_text(json.dumps(manifest), encoding="utf-8")


def _seed_data(repo):
    InstrumentCollector(repo).collect(
        [
            {
                "instrument_id": "i1",
                "external_code": "0001",
                "market_code": "KOSDAQ",
                "instrument_name": "A",
                "listing_date": date(2020, 1, 1),
            }
        ],
        "krx",
    )
    DailyMarketCollector(repo).collect(
        [
            {
                "instrument_id": "i1",
                "trade_date": date(2026, 1, 2),
                "open": 10,
                "high": 12,
                "low": 9,
                "close": 11,
                "volume": 100,
                "turnover_value": 1000,
                "market_value": 5000,
            }
        ],
        "krx",
        "r1",
    )
    BenchmarkCollector(repo).collect(
        [{"index_code": "KOSDAQ", "trade_date": date(2026, 1, 2), "open": 100, "high": 101, "low": 99, "close": 100.5}],
        "krx",
        "r1",
    )
    repo.upsert_trading_calendar(
        [
            {
                "market_code": "KOSDAQ",
                "trade_date": "2026-01-02",
                "is_open": True,
                "holiday_name": None,
                "source_name": "krx",
                "collected_at": "2026-01-02T00:00:00",
                "run_id": "r1",
            }
        ]
    )


def test_run_manager_status(repo):
    runs = RunManager(repo)
    run_id = runs.start("phase1", "krx", "2026-01-01", "2026-01-02")
    runs.finish(run_id, success_count=10, failure_count=0, warning_count=1)
    row = repo.query("SELECT status, warning_count FROM collection_runs WHERE run_id=?", (run_id,))[0]
    assert row["status"] == "PARTIAL"
    assert row["warning_count"] == 1


def test_export_service_happy_path(repo, tmp_path):
    _seed_data(repo)
    svc = ExportService(repo, writer=FakeParquetWriter())
    req = ExportRequest(
        market_code="KOSDAQ",
        index_codes=["KOSDAQ"],
        date_from="2026-01-01",
        date_to="2026-01-03",
        include_issues=True,
        output_format="parquet",
        output_path=(tmp_path / "out").as_posix(),
    )
    created = svc.create_job(req)
    done = svc.run_job(created["job_id"])
    assert done["status"] == "SUCCEEDED"
    assert (tmp_path / "out" / "instrument_daily.parquet").exists()
    assert (tmp_path / "out" / "manifest.json").exists()


def test_export_service_invalid_date(repo, tmp_path):
    svc = ExportService(repo, writer=FakeParquetWriter())
    try:
        svc.create_job(
            ExportRequest(
                market_code="KOSDAQ",
                index_codes=["KOSDAQ"],
                date_from="2026-01-10",
                date_to="2026-01-03",
                include_issues=True,
                output_format="parquet",
                output_path=(tmp_path / "out").as_posix(),
            )
        )
        assert False, "expected ValueError"
    except ValueError:
        assert True


def test_api_contract(repo, tmp_path):
    api = BacktestExportAPI(ExportService(repo, writer=FakeParquetWriter()))
    status, payload = api.post_exports(
        {
            "market_code": "KOSDAQ",
            "index_codes": ["KOSDAQ"],
            "date_from": "2026-01-01",
            "date_to": "2026-01-03",
            "output_path": (tmp_path / "o").as_posix(),
        }
    )
    assert status == 202
    get_status, detail = api.get_export(payload["job_id"])
    assert get_status == 200
    bad_status, _ = api.post_exports({"market_code": "KOSDAQ"})
    assert bad_status == 400

