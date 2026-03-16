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
        row_list = list(rows)
        path.write_text(json.dumps(row_list), encoding="utf-8")
        return len(row_list)

    def sha256(self, path: Path):
        return "fakehash"

    def write_manifest(self, path: Path, manifest):
        path.write_text(json.dumps(manifest), encoding="utf-8")


class CapturingParquetWriter(FakeParquetWriter):
    def __init__(self):
        self.schemas = {}

    def write(self, path: Path, rows):
        row_list = list(rows)
        keys = list(row_list[0].keys()) if row_list else []
        self.schemas[path.name] = keys
        return super().write(path, row_list)


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


def test_export_service_uses_streaming_repository_methods(repo, tmp_path, monkeypatch):
    _seed_data(repo)
    svc = ExportService(repo, writer=FakeParquetWriter())

    monkeypatch.setattr(repo, "get_core_market", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("legacy market query used")))
    monkeypatch.setattr(repo, "get_benchmark", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("legacy benchmark query used")))
    monkeypatch.setattr(repo, "get_calendar", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("legacy calendar query used")))
    monkeypatch.setattr(repo, "get_issues", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("legacy issues query used")))

    job = svc.create_job(
        ExportRequest(
            market_codes=["KOSDAQ"],
            index_codes=["KOSDAQ"],
            date_from="2026-01-01",
            date_to="2026-01-03",
            include_issues=True,
            output_format="parquet",
            output_path=(tmp_path / "stream_out").as_posix(),
        )
    )

    done = svc.run_job(job["job_id"])
    assert done["status"] == "SUCCEEDED"


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
        "has_recent_halt_or_zero_volume",
        "has_unresolved_corporate_action",
        "unresolved_corporate_action_types",
        "unresolved_corporate_action_issues",
        "is_special_trading_regime",
        "is_tradable_for_signal",
        "signal_validity_reason",
    ]


def test_export_service_marks_signal_validity_without_dropping_rows(repo, tmp_path):
    InstrumentCollector(repo).collect(
        [
            {
                "instrument_id": "550e8400-e29b-41d4-a716-446655440099",
                "external_code": "700001",
                "market_code": "KOSDAQ",
                "instrument_name": "Mask Sample",
                "listing_date": date(2020, 1, 1),
            }
        ],
        "krx",
    )
    DailyMarketCollector(repo).collect(
        [
            {
                "instrument_id": "550e8400-e29b-41d4-a716-446655440099",
                "trade_date": date(2026, 1, 2),
                "open": 100,
                "high": 100,
                "low": 100,
                "close": 100,
                "volume": 0,
                "turnover_value": 0,
                "market_value": 1000,
            },
            {
                "instrument_id": "550e8400-e29b-41d4-a716-446655440099",
                "trade_date": date(2026, 1, 3),
                "open": 90,
                "high": 91,
                "low": 89,
                "close": 90,
                "volume": 100,
                "turnover_value": 9000,
                "market_value": 900,
            },
            {
                "instrument_id": "550e8400-e29b-41d4-a716-446655440099",
                "trade_date": date(2026, 1, 4),
                "open": 92,
                "high": 93,
                "low": 91,
                "close": 92,
                "volume": 100,
                "turnover_value": 9200,
                "market_value": 920,
            },
        ],
        "krx",
        "r_mask",
    )
    BenchmarkCollector(repo).collect(
        [{"index_code": "KOSDAQ", "trade_date": date(2026, 1, 2), "open": 100, "high": 101, "low": 99, "close": 100.5}],
        "krx",
        "r_mask",
    )
    repo.upsert_trading_calendar(
        [
            {"market_code": "KOSDAQ", "trade_date": "2026-01-02", "is_open": True, "holiday_name": None, "source_name": "krx", "collected_at": "2026-01-02T00:00:00", "run_id": None},
            {"market_code": "KOSDAQ", "trade_date": "2026-01-03", "is_open": True, "holiday_name": None, "source_name": "krx", "collected_at": "2026-01-03T00:00:00", "run_id": None},
            {"market_code": "KOSDAQ", "trade_date": "2026-01-04", "is_open": True, "holiday_name": None, "source_name": "krx", "collected_at": "2026-01-04T00:00:00", "run_id": None},
        ]
    )
    repo.upsert_corporate_events(
        [
            {
                "event_id": "evt_mask_review",
                "event_version": 1,
                "instrument_id": "550e8400-e29b-41d4-a716-446655440099",
                "event_type": "RIGHTS_ISSUE",
                "announce_date": "2026-01-04",
                "effective_date": "2026-01-04",
                "source_event_id": "mask_review_1",
                "source_name": "opendart",
                "collected_at": "2026-01-04T00:00:00Z",
                "raw_factor": 0.8,
                "confidence": "LOW",
                "status": "NEEDS_REVIEW",
                "payload": {"activation_issue": "missing_pricing_inputs", "factor_rule": "rights_issue_section1_3"},
            }
        ]
    )

    svc = ExportService(repo, writer=FakeParquetWriter())
    created = svc.create_job(
        ExportRequest(
            market_codes=["KOSDAQ"],
            index_codes=["KOSDAQ"],
            date_from="2026-01-02",
            date_to="2026-01-04",
            include_issues=False,
            output_format="parquet",
            output_path=(tmp_path / "out_mask").as_posix(),
        )
    )
    svc.run_job(created["job_id"])

    rows = json.loads((tmp_path / "out_mask" / "instrument_daily.parquet").read_text(encoding="utf-8"))
    assert len(rows) == 3
    by_date = {row["trade_date"]: row for row in rows}
    assert by_date["2026-01-02"]["is_tradable_for_signal"] is False
    assert by_date["2026-01-02"]["signal_validity_reason"] == "current_halt_or_zero_volume,unresolved_corporate_action"
    assert by_date["2026-01-03"]["has_recent_halt_or_zero_volume"] is True
    assert by_date["2026-01-03"]["is_special_trading_regime"] is True
    assert by_date["2026-01-04"]["has_unresolved_corporate_action"] is True
    assert by_date["2026-01-04"]["unresolved_corporate_action_types"] == ["RIGHTS_ISSUE"]
    assert by_date["2026-01-04"]["unresolved_corporate_action_issues"] == ["missing_pricing_inputs"]


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


def test_export_service_defaults_to_first_available_benchmark_series(repo, tmp_path):
    InstrumentCollector(repo).collect(
        [
            {
                "instrument_id": "i_default_series",
                "external_code": "0099",
                "market_code": "KOSDAQ",
                "instrument_name": "Default Series",
                "listing_date": date(2020, 1, 1),
            }
        ],
        "krx",
    )
    DailyMarketCollector(repo).collect(
        [
            {
                "instrument_id": "i_default_series",
                "trade_date": date(2026, 1, 2),
                "open": 10,
                "high": 11,
                "low": 9,
                "close": 10.5,
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
                "index_name": "KOSDAQ_PRIMARY",
                "trade_date": date(2026, 1, 2),
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100.5,
            }
        ],
        "krx",
        "r1",
    )

    svc = ExportService(repo, writer=FakeParquetWriter())
    created = svc.create_job(
        ExportRequest(
            market_codes=["KOSDAQ"],
            index_codes=["KOSDAQ"],
            date_from="2026-01-01",
            date_to="2026-01-03",
            include_issues=False,
            output_format="parquet",
            output_path=(tmp_path / "out_first_series").as_posix(),
        )
    )
    svc.run_job(created["job_id"])

    rows = json.loads((tmp_path / "out_first_series" / "benchmark_daily.parquet").read_text(encoding="utf-8"))
    assert len(rows) == 1
    assert rows[0]["index_name"] == "KOSDAQ_PRIMARY"


def test_api_manifest_returns_200_for_succeeded_job(repo, tmp_path):
    _seed_data(repo)
    service = ExportService(repo, writer=FakeParquetWriter())
    api = BacktestExportAPI(service)
    created = service.create_job(
        ExportRequest(
            market_codes=["KOSDAQ"],
            index_codes=["KOSDAQ"],
            date_from="2026-01-01",
            date_to="2026-01-03",
            include_issues=False,
            output_format="parquet",
            output_path=(tmp_path / "manifest_ok").as_posix(),
        )
    )
    service.run_job(created["job_id"])

    status, payload = api.get_manifest(created["job_id"])

    assert status == 200
    assert payload["job_id"] == created["job_id"]


def test_api_manifest_returns_404_when_manifest_file_is_missing(repo, tmp_path):
    _seed_data(repo)
    service = ExportService(repo, writer=FakeParquetWriter())
    api = BacktestExportAPI(service)
    created = service.create_job(
        ExportRequest(
            market_codes=["KOSDAQ"],
            index_codes=["KOSDAQ"],
            date_from="2026-01-01",
            date_to="2026-01-03",
            include_issues=False,
            output_format="parquet",
            output_path=(tmp_path / "manifest_missing").as_posix(),
        )
    )
    service.run_job(created["job_id"])
    (tmp_path / "manifest_missing" / "manifest.json").unlink()

    status, payload = api.get_manifest(created["job_id"])

    assert status == 404
    assert "Manifest not found" in payload["error"]


def test_api_manifest_returns_409_before_job_success(repo, tmp_path):
    service = ExportService(repo, writer=FakeParquetWriter())
    api = BacktestExportAPI(service)
    created = service.create_job(
        ExportRequest(
            market_codes=["KOSDAQ"],
            index_codes=["KOSDAQ"],
            date_from="2026-01-01",
            date_to="2026-01-03",
            include_issues=False,
            output_format="parquet",
            output_path=(tmp_path / "manifest_pending").as_posix(),
        )
    )

    status, payload = api.get_manifest(created["job_id"])

    assert status == 409
    assert "only after success" in payload["error"]



def test_export_service_prefers_representative_benchmark_series(repo, tmp_path):
    InstrumentCollector(repo).collect(
        [
            {
                "instrument_id": "i_rep_series",
                "external_code": "0100",
                "market_code": "KOSDAQ",
                "instrument_name": "Representative Series",
                "listing_date": date(2020, 1, 1),
            }
        ],
        "krx",
    )
    DailyMarketCollector(repo).collect(
        [
            {
                "instrument_id": "i_rep_series",
                "trade_date": date(2026, 1, 2),
                "open": 10,
                "high": 11,
                "low": 9,
                "close": 10.5,
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
                "index_name": "건설",
                "trade_date": date(2026, 1, 2),
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100.5,
            },
            {
                "index_code": "KOSDAQ",
                "index_name": "코스닥",
                "trade_date": date(2026, 1, 2),
                "open": 1100,
                "high": 1101,
                "low": 1099,
                "close": 1100.5,
            }
        ],
        "krx",
        "r1",
    )

    svc = ExportService(repo, writer=FakeParquetWriter())
    created = svc.create_job(
        ExportRequest(
            market_codes=["KOSDAQ"],
            index_codes=["KOSDAQ"],
            date_from="2026-01-01",
            date_to="2026-01-03",
            include_issues=False,
            output_format="parquet",
            output_path=(tmp_path / "out_representative").as_posix(),
        )
    )
    svc.run_job(created["job_id"])

    rows = json.loads((tmp_path / "out_representative" / "benchmark_daily.parquet").read_text(encoding="utf-8"))
    assert len(rows) == 1
    assert rows[0]["index_name"] == "코스닥"
