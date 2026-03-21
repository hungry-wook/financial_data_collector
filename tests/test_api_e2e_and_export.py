import importlib
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from financial_data_collector.adjustment_service import AdjustmentService
from financial_data_collector.collectors import BenchmarkCollector, DailyMarketCollector, InstrumentCollector
from financial_data_collector.export_backtest_dataset import export_backtest_dataset


def _seed_instrument(repo, instrument_id, external_code, market_code, delisting_date=None):
    InstrumentCollector(repo).collect(
        [{
            "instrument_id": instrument_id,
            "external_code": external_code,
            "market_code": market_code,
            "instrument_name": f"Name-{external_code}",
            "listing_date": "2020-01-01",
            "delisting_date": delisting_date,
            "listed_shares": 1000,
        }],
        "krx",
    )


def _seed_market_rows(repo, rebuild_factors=True):
    _seed_instrument(repo, "11111111-1111-1111-1111-111111111111", "111111", "KOSDAQ")
    _seed_instrument(repo, "22222222-2222-2222-2222-222222222222", "222222", "KOSPI", delisting_date="2026-01-03")
    first = repo.get_instrument_id_by_external_code("111111", market_code="KOSDAQ")
    second = repo.get_instrument_id_by_external_code("222222", market_code="KOSPI")
    DailyMarketCollector(repo).collect(
        [
            {"instrument_id": first, "external_code": "111111", "market_code": "KOSDAQ", "trade_date": date(2026, 1, 1), "open": 10, "high": 11, "low": 9, "close": 10, "volume": 1000, "turnover_value": 10000, "market_value": 50000, "listed_shares": 1000, "base_price": 10},
            {"instrument_id": first, "external_code": "111111", "market_code": "KOSDAQ", "trade_date": date(2026, 1, 2), "open": 5, "high": 6, "low": 4, "close": 5, "volume": 2000, "turnover_value": 11000, "market_value": 55000, "listed_shares": 2000, "base_price": 5},
            {"instrument_id": second, "external_code": "222222", "market_code": "KOSPI", "trade_date": date(2026, 1, 2), "open": 20, "high": 22, "low": 19, "close": 21, "volume": 1500, "turnover_value": 21000, "market_value": 70000, "listed_shares": 1000, "base_price": 21},
            {"instrument_id": second, "external_code": "222222", "market_code": "KOSPI", "trade_date": date(2026, 1, 3), "open": 21, "high": 21, "low": 18, "close": 18, "volume": 1700, "turnover_value": 18000, "market_value": 68000, "listed_shares": 1000, "base_price": 18},
        ],
        "krx",
        "run-1",
    )
    BenchmarkCollector(repo).collect(
        [
            {"index_code": "KOSDAQ", "index_name": "KOSDAQ", "trade_date": date(2026, 1, 2), "open": 100, "high": 103, "low": 99, "close": 101, "volume": 100000, "turnover_value": 1000000, "market_cap": 5000000},
            {"index_code": "KOSPI", "index_name": "KOSPI", "trade_date": date(2026, 1, 2), "open": 200, "high": 205, "low": 198, "close": 204, "volume": 200000, "turnover_value": 2000000, "market_cap": 9000000},
        ],
        "krx",
        "run-1",
    )
    repo.upsert_trading_calendar([
        {"market_code": "KOSDAQ", "trade_date": "2026-01-02", "is_open": True, "holiday_name": None, "source_name": "krx", "collected_at": "2026-01-02T00:00:00Z", "run_id": None},
        {"market_code": "KOSPI", "trade_date": "2026-01-02", "is_open": True, "holiday_name": None, "source_name": "krx", "collected_at": "2026-01-02T00:00:00Z", "run_id": None},
        {"market_code": "KOSPI", "trade_date": "2026-01-03", "is_open": True, "holiday_name": None, "source_name": "krx", "collected_at": "2026-01-03T00:00:00Z", "run_id": None},
    ])
    if rebuild_factors:
        AdjustmentService(repo).rebuild_factors("2026-01-01", "2026-01-03")


def _make_api_client(repo, monkeypatch):
    server = importlib.import_module("financial_data_collector.server")
    monkeypatch.setattr(server, "DATABASE_URL", "postgresql://test")
    monkeypatch.setattr(server, "Repository", lambda *args, **kwargs: repo)
    return TestClient(server.app)


def test_api_e2e_serves_benchmark_catalog(repo, monkeypatch):
    _seed_market_rows(repo)
    with _make_api_client(repo, monkeypatch) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["status"] == "healthy"

        instruments = client.get("/api/v1/instruments", params={"limit": 10, "offset": 0})
        assert instruments.status_code == 200
        assert instruments.json()["total"] == 2

        benchmarks = client.get("/api/v1/benchmarks")
        assert benchmarks.status_code == 200
        payload = benchmarks.json()
        assert len(payload) == 2
        assert payload[0]["index_code"] in {"KOSDAQ", "KOSPI"}

        coverage = client.get("/api/v1/adjustments/coverage", params={"date_from": "2026-01-01", "date_to": "2026-01-03"})
        assert coverage.status_code == 200
        assert coverage.json()["is_complete"] is True


def test_export_backtest_dataset_requires_rebuilt_factors(repo, monkeypatch, tmp_path):
    _seed_market_rows(repo, rebuild_factors=False)
    with _make_api_client(repo, monkeypatch) as client:
        def api_get(path, params):
            response = client.get(path, params=params)
            response.raise_for_status()
            return response.json()

        with pytest.raises(ValueError, match="adjustment factors are incomplete"):
            export_backtest_dataset(
                base_url="http://testserver",
                output_dir=str(tmp_path),
                date_from="2026-01-01",
                date_to="2026-01-03",
                api_get=api_get,
            )


def test_export_backtest_dataset_writes_backtest_ready_parquet(repo, monkeypatch, tmp_path):
    _seed_market_rows(repo)
    with _make_api_client(repo, monkeypatch) as client:
        def api_get(path, params):
            response = client.get(path, params=params)
            response.raise_for_status()
            return response.json()

        manifest = export_backtest_dataset(
            base_url="http://testserver",
            output_dir=str(tmp_path),
            date_from="2026-01-01",
            date_to="2026-01-03",
            series_page_size=1,
            api_get=api_get,
        )

    assert manifest["counts"]["instruments"] == 2
    assert manifest["counts"]["instrument_daily"] == 4
    assert manifest["counts"]["benchmarks"] == 2
    assert manifest["counts"]["benchmark_daily"] == 2
    assert manifest["counts"]["trading_calendar"] == 3
    assert manifest["adjustment_coverage"]["is_complete"] is True

    instrument_daily_rows = pq.read_table(Path(tmp_path) / "instrument_daily.parquet").to_pylist()
    assert len(instrument_daily_rows) == 4
    assert any(row["external_code"] == "111111" and row["adj_close"] == 5.0 for row in instrument_daily_rows)
    assert any(row["external_code"] == "222222" and row["turnover_value"] == 21000.0 for row in instrument_daily_rows)

    benchmark_rows = pq.read_table(Path(tmp_path) / "benchmark_daily.parquet").to_pylist()
    assert len(benchmark_rows) == 2
    assert {row["index_code"] for row in benchmark_rows} == {"KOSDAQ", "KOSPI"}

    calendar_rows = pq.read_table(Path(tmp_path) / "trading_calendar.parquet").to_pylist()
    assert len(calendar_rows) == 3
