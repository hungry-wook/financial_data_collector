import json
from datetime import date
from pathlib import Path

import pytest

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


class CapturingParquetWriter(FakeParquetWriter):
    def __init__(self):
        self.schemas = {}

    def write(self, path: Path, rows):
        keys = list(rows[0].keys()) if rows else []
        self.schemas[path.name] = keys
        return super().write(path, rows)


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
                "run_id": None,
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
        market_codes=["KOSDAQ"],
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


def test_export_service_persists_job_state_in_db(repo, tmp_path):
    _seed_data(repo)
    req = ExportRequest(
        market_codes=["KOSDAQ"],
        index_codes=["KOSDAQ"],
        date_from="2026-01-01",
        date_to="2026-01-03",
        include_issues=False,
        output_format="parquet",
        output_path=(tmp_path / "persisted").as_posix(),
    )

    svc1 = ExportService(repo, writer=FakeParquetWriter())
    created = svc1.create_job(req)

    svc2 = ExportService(repo, writer=FakeParquetWriter())
    pending = svc2.get_job(created["job_id"])
    assert pending["status"] == "PENDING"
    assert pending["progress"] == 0

    done = svc2.run_job(created["job_id"])
    assert done["status"] == "SUCCEEDED"

    svc3 = ExportService(repo, writer=FakeParquetWriter())
    persisted = svc3.get_job(created["job_id"])
    assert persisted["status"] == "SUCCEEDED"
    assert persisted["progress"] == 100
    assert persisted["output_path"] == (tmp_path / "persisted").as_posix()


def test_export_service_invalid_date(repo, tmp_path):
    svc = ExportService(repo, writer=FakeParquetWriter())
    try:
        svc.create_job(
            ExportRequest(
                market_codes=["KOSDAQ"],
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


def test_export_service_rejects_invalid_date_format(repo, tmp_path):
    svc = ExportService(repo, writer=FakeParquetWriter())
    with pytest.raises(ValueError):
        svc.create_job(
            ExportRequest(
                market_codes=["KOSDAQ"],
                index_codes=["KOSDAQ"],
                date_from="2026-1-2",
                date_to="2026-01-03",
                include_issues=False,
                output_format="parquet",
                output_path=(tmp_path / "out").as_posix(),
            )
        )


def test_api_contract(repo, tmp_path):
    api = BacktestExportAPI(ExportService(repo, writer=FakeParquetWriter()))
    status, payload = api.post_exports(
        {
            "market_codes": ["KOSDAQ"],
            "index_codes": ["KOSDAQ"],
            "date_from": "2026-01-01",
            "date_to": "2026-01-03",
            "output_path": (tmp_path / "o").as_posix(),
        }
    )
    assert status == 202
    get_status, detail = api.get_export(payload["job_id"])
    assert get_status == 200
    bad_status, _ = api.post_exports({"market_codes": ["KOSDAQ"]})
    assert bad_status == 400


def test_parquet_schema_snapshot_instrument_daily(repo, tmp_path):
    _seed_data(repo)
    writer = CapturingParquetWriter()
    svc = ExportService(repo, writer=writer)
    job = svc.create_job(
        ExportRequest(
            market_codes=["KOSDAQ"],
            index_codes=["KOSDAQ"],
            date_from="2026-01-01",
            date_to="2026-01-03",
            include_issues=False,
            output_format="parquet",
            output_path=(tmp_path / "out1").as_posix(),
        )
    )
    svc.run_job(job["job_id"])
    assert writer.schemas["instrument_daily.parquet"] == [
        "instrument_id",
        "external_code",
        "market_code",
        "instrument_name",
        "listing_date",
        "delisting_date",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover_value",
        "market_value",
        "is_trade_halted",
        "is_under_supervision",
        "record_status",
        "source_name",
        "collected_at",
    ]


def test_parquet_schema_snapshot_benchmark_daily(repo, tmp_path):
    _seed_data(repo)
    writer = CapturingParquetWriter()
    svc = ExportService(repo, writer=writer)
    job = svc.create_job(
        ExportRequest(
            market_codes=["KOSDAQ"],
            index_codes=["KOSDAQ"],
            date_from="2026-01-01",
            date_to="2026-01-03",
            include_issues=False,
            output_format="parquet",
            output_path=(tmp_path / "out2").as_posix(),
        )
    )
    svc.run_job(job["job_id"])
    assert writer.schemas["benchmark_daily.parquet"] == [
        "index_code",
        "index_name",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "record_status",
    ]


def test_parquet_schema_snapshot_trading_calendar(repo, tmp_path):
    _seed_data(repo)
    writer = CapturingParquetWriter()
    svc = ExportService(repo, writer=writer)
    job = svc.create_job(
        ExportRequest(
            market_codes=["KOSDAQ"],
            index_codes=["KOSDAQ"],
            date_from="2026-01-01",
            date_to="2026-01-03",
            include_issues=False,
            output_format="parquet",
            output_path=(tmp_path / "out3").as_posix(),
        )
    )
    svc.run_job(job["job_id"])
    assert writer.schemas["trading_calendar.parquet"] == [
        "market_code",
        "trade_date",
        "is_open",
        "holiday_name",
    ]


def test_export_service_excludes_delisting_date_from_universe(repo, tmp_path):
    InstrumentCollector(repo).collect(
        [
            {
                "instrument_id": "i2",
                "external_code": "0002",
                "market_code": "KOSDAQ",
                "instrument_name": "B",
                "listing_date": date(2020, 1, 1),
                "delisting_date": date(2026, 1, 3),
            }
        ],
        "krx",
    )
    DailyMarketCollector(repo).collect(
        [
            {
                "instrument_id": "i2",
                "trade_date": date(2026, 1, 2),
                "open": 10,
                "high": 12,
                "low": 9,
                "close": 11,
                "volume": 100,
                "turnover_value": 1000,
                "market_value": 5000,
            },
            {
                "instrument_id": "i2",
                "trade_date": date(2026, 1, 3),
                "open": 11,
                "high": 13,
                "low": 10,
                "close": 12,
                "volume": 120,
                "turnover_value": 1200,
                "market_value": 5200,
            },
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
                "run_id": None,
            },
            {
                "market_code": "KOSDAQ",
                "trade_date": "2026-01-03",
                "is_open": True,
                "holiday_name": None,
                "source_name": "krx",
                "collected_at": "2026-01-03T00:00:00",
                "run_id": None,
            },
        ]
    )

    svc = ExportService(repo, writer=FakeParquetWriter())
    req = ExportRequest(
        market_codes=["KOSDAQ"],
        index_codes=["KOSDAQ"],
        date_from="2026-01-01",
        date_to="2026-01-03",
        include_issues=False,
        output_format="parquet",
        output_path=(tmp_path / "out_delist").as_posix(),
    )
    created = svc.create_job(req)
    done = svc.run_job(created["job_id"])
    assert done["status"] == "SUCCEEDED"

    instrument_daily = json.loads((tmp_path / "out_delist" / "instrument_daily.parquet").read_text(encoding="utf-8"))
    trade_dates = [row["trade_date"] for row in instrument_daily]
    assert "2026-01-02" in trade_dates
    assert "2026-01-03" not in trade_dates


def test_export_service_returns_kosdaq_and_kospi_when_requested(repo, tmp_path):
    InstrumentCollector(repo).collect(
        [
            {
                "instrument_id": "im1",
                "external_code": "111111",
                "market_code": "KOSDAQ",
                "instrument_name": "KD",
                "listing_date": date(2020, 1, 1),
            },
            {
                "instrument_id": "im2",
                "external_code": "222222",
                "market_code": "KOSPI",
                "instrument_name": "KP",
                "listing_date": date(2020, 1, 1),
            },
        ],
        "krx",
    )
    DailyMarketCollector(repo).collect(
        [
            {
                "instrument_id": "im1",
                "trade_date": date(2026, 1, 2),
                "open": 10,
                "high": 12,
                "low": 9,
                "close": 11,
                "volume": 100,
                "turnover_value": 1000,
                "market_value": 5000,
            },
            {
                "instrument_id": "im2",
                "trade_date": date(2026, 1, 2),
                "open": 20,
                "high": 22,
                "low": 19,
                "close": 21,
                "volume": 200,
                "turnover_value": 2000,
                "market_value": 9000,
            },
        ],
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
                "run_id": None,
            },
            {
                "market_code": "KOSPI",
                "trade_date": "2026-01-02",
                "is_open": True,
                "holiday_name": None,
                "source_name": "krx",
                "collected_at": "2026-01-02T00:00:00",
                "run_id": None,
            },
        ]
    )
    BenchmarkCollector(repo).collect(
        [
            {"index_code": "KOSDAQ", "trade_date": date(2026, 1, 2), "open": 100, "high": 101, "low": 99, "close": 100.5},
            {"index_code": "KOSPI", "trade_date": date(2026, 1, 2), "open": 200, "high": 201, "low": 199, "close": 200.5},
        ],
        "krx",
        "r1",
    )

    svc = ExportService(repo, writer=FakeParquetWriter())
    created = svc.create_job(
        ExportRequest(
            market_codes=["KOSDAQ", "KOSPI"],
            index_codes=["KOSDAQ", "KOSPI"],
            date_from="2026-01-01",
            date_to="2026-01-03",
            include_issues=False,
            output_format="parquet",
            output_path=(tmp_path / "out_both").as_posix(),
        )
    )
    done = svc.run_job(created["job_id"])
    assert done["status"] == "SUCCEEDED"
    rows = json.loads((tmp_path / "out_both" / "instrument_daily.parquet").read_text(encoding="utf-8"))
    markets = sorted({r["market_code"] for r in rows})
    assert markets == ["KOSDAQ", "KOSPI"]


def test_export_service_prevents_benchmark_series_mixing_by_default(repo, tmp_path):
    InstrumentCollector(repo).collect(
        [
            {
                "instrument_id": "imx1",
                "external_code": "333333",
                "market_code": "KOSDAQ",
                "instrument_name": "KD",
                "listing_date": date(2020, 1, 1),
            }
        ],
        "krx",
    )
    DailyMarketCollector(repo).collect(
        [
            {
                "instrument_id": "imx1",
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
    repo.upsert_trading_calendar(
        [
            {
                "market_code": "KOSDAQ",
                "trade_date": "2026-01-02",
                "is_open": True,
                "holiday_name": None,
                "source_name": "krx",
                "collected_at": "2026-01-02T00:00:00",
                "run_id": None,
            }
        ]
    )
    BenchmarkCollector(repo).collect(
        [
            {
                "index_code": "KOSDAQ",
                "index_name": "KOSDAQ",
                "trade_date": date(2026, 1, 2),
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100.5,
            },
            {
                "index_code": "KOSDAQ",
                "index_name": "KOSDAQ_LEGACY",
                "trade_date": date(2026, 1, 2),
                "open": 200,
                "high": 201,
                "low": 199,
                "close": 200.5,
            },
        ],
        "krx",
        "r1",
    )

    svc = ExportService(repo, writer=FakeParquetWriter())
    created_default = svc.create_job(
        ExportRequest(
            market_codes=["KOSDAQ"],
            index_codes=["KOSDAQ"],
            date_from="2026-01-01",
            date_to="2026-01-03",
            include_issues=False,
            output_format="parquet",
            output_path=(tmp_path / "out_default").as_posix(),
        )
    )
    svc.run_job(created_default["job_id"])
    default_rows = json.loads((tmp_path / "out_default" / "benchmark_daily.parquet").read_text(encoding="utf-8"))
    assert [r["index_name"] for r in default_rows] == ["KOSDAQ"]

    created_series = svc.create_job(
        ExportRequest(
            market_codes=["KOSDAQ"],
            index_codes=["KOSDAQ"],
            series_names=["KOSDAQ_LEGACY"],
            date_from="2026-01-01",
            date_to="2026-01-03",
            include_issues=False,
            output_format="parquet",
            output_path=(tmp_path / "out_series").as_posix(),
        )
    )
    svc.run_job(created_series["job_id"])
    series_rows = json.loads((tmp_path / "out_series" / "benchmark_daily.parquet").read_text(encoding="utf-8"))
    assert [r["index_name"] for r in series_rows] == ["KOSDAQ_LEGACY"]
