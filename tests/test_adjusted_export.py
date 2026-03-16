import json
from datetime import date


from financial_data_collector.collectors import BenchmarkCollector, DailyMarketCollector, InstrumentCollector
from financial_data_collector.adjustment_service import AdjustmentService
from financial_data_collector.export_service import ExportRequest, ExportService


class _Writer:
    def write(self, path, rows, **_kwargs):
        row_list = list(rows)
        path.write_text(json.dumps(row_list), encoding="utf-8")
        return len(row_list)

    def sha256(self, path):
        return "x"

    def write_manifest(self, path, manifest):
        path.write_text(json.dumps(manifest), encoding="utf-8")


def test_export_adjusted_series_uses_materialized_factor(repo, tmp_path):
    InstrumentCollector(repo).collect(
        [{"instrument_id": "i_exp_adj", "external_code": "654321", "market_code": "KOSDAQ", "instrument_name": "Adj", "listing_date": date(2020, 1, 1)}],
        "krx",
    )
    instrument_id = repo.get_instrument_id_by_external_code("654321", market_code="KOSDAQ")
    assert instrument_id

    DailyMarketCollector(repo).collect(
        [
            {"instrument_id": instrument_id, "trade_date": date(2026, 1, 2), "open": 100, "high": 110, "low": 90, "close": 100, "volume": 10, "listed_shares": 100},
            {"instrument_id": instrument_id, "trade_date": date(2026, 1, 3), "open": 50, "high": 55, "low": 45, "close": 50, "volume": 10, "listed_shares": 200},
        ],
        "krx",
        "r1",
    )
    BenchmarkCollector(repo).collect(
        [{"index_code": "KOSDAQ", "trade_date": date(2026, 1, 2), "open": 100, "high": 101, "low": 99, "close": 100}],
        "krx",
        "r1",
    )
    repo.upsert_trading_calendar(
        [{"market_code": "KOSDAQ", "trade_date": "2026-01-02", "is_open": True, "holiday_name": None, "source_name": "krx", "collected_at": "2026-01-02T00:00:00", "run_id": None}]
    )
    repo.upsert_corporate_events(
        [
            {
                "event_id": "evt_exp_adj",
                "event_version": 1,
                "instrument_id": instrument_id,
                "event_type": "BONUS_ISSUE",
                "announce_date": "2026-01-03",
                "effective_date": "2026-01-03",
                "source_event_id": "20260103000002",
                "source_name": "opendart",
                "collected_at": "2026-01-03T00:00:00Z",
                "raw_factor": 0.5,
                "confidence": "HIGH",
                "status": "ACTIVE",
                "payload": {"ratio": 0.5},
            }
        ]
    )

    AdjustmentService(repo).rebuild_factors("2026-01-01", "2026-01-04")

    svc = ExportService(repo, writer=_Writer())
    job = svc.create_job(
        ExportRequest(
            market_codes=["KOSDAQ"],
            index_codes=["KOSDAQ"],
            date_from="2026-01-01",
            date_to="2026-01-04",
            include_issues=False,
            output_format="parquet",
            output_path=(tmp_path / "out_adj").as_posix(),
            series_type="adjusted",
        )
    )
    done = svc.run_job(job["job_id"])
    assert done["status"] == "SUCCEEDED"

    rows = json.loads((tmp_path / "out_adj" / "instrument_daily.parquet").read_text(encoding="utf-8"))
    first_day = [r for r in rows if r["trade_date"] == "2026-01-02"][0]
    assert first_day["close"] == 50
    assert first_day["volume"] == 20



def test_export_adjusted_series_fails_without_materialized_factor(repo, tmp_path):
    InstrumentCollector(repo).collect(
        [{"instrument_id": "i_exp_raw", "external_code": "777777", "market_code": "KOSDAQ", "instrument_name": "Adj Missing", "listing_date": date(2020, 1, 1)}],
        "krx",
    )
    instrument_id = repo.get_instrument_id_by_external_code("777777", market_code="KOSDAQ")
    assert instrument_id

    DailyMarketCollector(repo).collect(
        [
            {"instrument_id": instrument_id, "trade_date": date(2026, 1, 2), "open": 100, "high": 110, "low": 90, "close": 100, "volume": 10},
            {"instrument_id": instrument_id, "trade_date": date(2026, 1, 3), "open": 50, "high": 55, "low": 45, "close": 50, "volume": 10},
        ],
        "krx",
        "r1",
    )
    BenchmarkCollector(repo).collect(
        [{"index_code": "KOSDAQ", "trade_date": date(2026, 1, 2), "open": 100, "high": 101, "low": 99, "close": 100}],
        "krx",
        "r1",
    )
    repo.upsert_trading_calendar(
        [{"market_code": "KOSDAQ", "trade_date": "2026-01-02", "is_open": True, "holiday_name": None, "source_name": "krx", "collected_at": "2026-01-02T00:00:00", "run_id": None}]
    )
    repo.upsert_corporate_events(
        [
            {
                "event_id": "evt_exp_raw",
                "event_version": 1,
                "instrument_id": instrument_id,
                "event_type": "BONUS_ISSUE",
                "announce_date": "2026-01-03",
                "effective_date": "2026-01-03",
                "source_event_id": "20260103000003",
                "source_name": "opendart",
                "collected_at": "2026-01-03T00:00:00Z",
                "raw_factor": 0.5,
                "confidence": "HIGH",
                "status": "ACTIVE",
                "payload": {"ratio": 0.5},
            }
        ]
    )

    svc = ExportService(repo, writer=_Writer())
    job = svc.create_job(
        ExportRequest(
            market_codes=["KOSDAQ"],
            index_codes=["KOSDAQ"],
            date_from="2026-01-01",
            date_to="2026-01-04",
            include_issues=False,
            output_format="parquet",
            output_path=(tmp_path / "out_adj_missing").as_posix(),
            series_type="adjusted",
        )
    )
    done = svc.run_job(job["job_id"])
    assert done["status"] == "FAILED"
    assert "missing coverage" in done["error_message"]
    assert not (tmp_path / "out_adj_missing" / "instrument_daily.parquet").exists()


def test_export_adjusted_series_fails_when_prior_event_factor_window_is_missing(repo, tmp_path):
    InstrumentCollector(repo).collect(
        [{"instrument_id": "i_exp_gap", "external_code": "888888", "market_code": "KOSDAQ", "instrument_name": "Adj Prior Gap", "listing_date": date(2020, 1, 1)}],
        "krx",
    )
    instrument_id = repo.get_instrument_id_by_external_code("888888", market_code="KOSDAQ")
    assert instrument_id

    DailyMarketCollector(repo).collect(
        [
            {"instrument_id": instrument_id, "trade_date": date(2026, 1, 4), "open": 55, "high": 60, "low": 50, "close": 55, "volume": 10},
            {"instrument_id": instrument_id, "trade_date": date(2026, 1, 5), "open": 56, "high": 61, "low": 51, "close": 56, "volume": 10},
        ],
        "krx",
        "r1",
    )
    BenchmarkCollector(repo).collect(
        [{"index_code": "KOSDAQ", "trade_date": date(2026, 1, 4), "open": 100, "high": 101, "low": 99, "close": 100}],
        "krx",
        "r1",
    )
    repo.upsert_trading_calendar(
        [{"market_code": "KOSDAQ", "trade_date": "2026-01-04", "is_open": True, "holiday_name": None, "source_name": "krx", "collected_at": "2026-01-04T00:00:00", "run_id": None}]
    )
    repo.upsert_corporate_events(
        [
            {
                "event_id": "evt_exp_gap",
                "event_version": 1,
                "instrument_id": instrument_id,
                "event_type": "BONUS_ISSUE",
                "announce_date": "2026-01-03",
                "effective_date": "2026-01-03",
                "source_event_id": "20260103000009",
                "source_name": "opendart",
                "collected_at": "2026-01-03T00:00:00Z",
                "raw_factor": 0.5,
                "confidence": "HIGH",
                "status": "ACTIVE",
                "payload": {"ratio": 0.5},
            }
        ]
    )

    svc = ExportService(repo, writer=_Writer())
    job = svc.create_job(
        ExportRequest(
            market_codes=["KOSDAQ"],
            index_codes=["KOSDAQ"],
            date_from="2026-01-04",
            date_to="2026-01-05",
            include_issues=False,
            output_format="parquet",
            output_path=(tmp_path / "out_adj_gap").as_posix(),
            series_type="adjusted",
        )
    )

    done = svc.run_job(job["job_id"])
    assert done["status"] == "FAILED"
    assert "888888" in done["error_message"]
