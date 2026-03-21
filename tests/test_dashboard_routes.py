import asyncio
from datetime import date

from financial_data_collector.collectors import BenchmarkCollector, DailyMarketCollector, InstrumentCollector
from financial_data_collector.dashboard_routes import get_benchmark_series, get_instrument_profile, get_instruments, get_prices


class _DummyState:
    def __init__(self, repo):
        self.repo = repo


class _DummyApp:
    def __init__(self, repo):
        self.state = _DummyState(repo)


class _DummyRequest:
    def __init__(self, repo):
        self.app = _DummyApp(repo)


def _seed_instrument(repo, instrument_id="seed-1", external_code="111111", market_code="KOSDAQ", delisting_date=None):
    InstrumentCollector(repo).collect(
        [{
            "instrument_id": instrument_id,
            "external_code": external_code,
            "market_code": market_code,
            "instrument_name": "Test Name",
            "listing_date": "2020-01-01",
            "delisting_date": delisting_date,
            "listed_shares": 100,
        }],
        "krx",
    )


def test_dashboard_instruments_includes_delisting_snapshot_fields(repo):
    _seed_instrument(repo, instrument_id="seed-del", external_code="123456", delisting_date="2026-01-15")
    repo.upsert_delisting_snapshot([
        {
            "market_code": "KOSDAQ",
            "external_code": "123456",
            "delisting_date": "2026-01-15",
            "delisting_reason": "delisting reason",
            "note": "delisting note",
        }
    ], source_name="kind", run_id=None)
    payload = asyncio.run(get_instruments(_DummyRequest(repo), search="", listed_status="delisted", limit=20, offset=0))
    assert payload["total"] == 1
    assert payload["items"][0]["delisting_reason"] == "delisting reason"


def test_dashboard_instrument_profile_returns_latest_record(repo):
    _seed_instrument(repo, instrument_id="seed-old", external_code="333333", market_code="KOSDAQ", delisting_date="2022-12-31")
    _seed_instrument(repo, instrument_id="seed-new", external_code="333333", market_code="KOSPI", delisting_date=None)
    payload = asyncio.run(get_instrument_profile("333333", _DummyRequest(repo)))
    assert payload["market_code"] == "KOSPI"
    assert payload["listed_status"] == "listed"


def test_dashboard_prices_return_adjusted_columns(repo):
    _seed_instrument(repo, instrument_id="p1", external_code="444444")
    instrument_id = repo.get_instrument_id_by_external_code("444444", market_code="KOSDAQ")
    DailyMarketCollector(repo).collect(
        [
            {"instrument_id": instrument_id, "trade_date": date(2026, 1, 1), "open": 10, "high": 11, "low": 9, "close": 10, "volume": 1000, "turnover_value": 10000, "market_value": 50000, "base_price": 10},
            {"instrument_id": instrument_id, "trade_date": date(2026, 1, 2), "open": 5, "high": 6, "low": 4, "close": 5, "volume": 2000, "turnover_value": 11000, "market_value": 55000, "base_price": 5},
        ],
        "krx",
        "r1",
    )
    from financial_data_collector.adjustment_service import AdjustmentService
    AdjustmentService(repo).rebuild_factors("2026-01-01", "2026-01-02")
    payload = asyncio.run(get_prices("444444", _DummyRequest(repo), date_from="", date_to="", limit=10, offset=0))
    assert payload["total"] == 2
    assert "adj_close" in payload["items"][0]
    assert "base_price" in payload["items"][0]


def test_dashboard_benchmark_defaults_to_available_series(repo):
    BenchmarkCollector(repo).collect([
        {"index_code": "KOSDAQ", "index_name": "KOSDAQ_PRIMARY", "trade_date": date(2026, 1, 2), "open": 100, "high": 101, "low": 99, "close": 100.5}
    ], "krx", "r1")
    payload = asyncio.run(get_benchmark_series("KOSDAQ", _DummyRequest(repo), series_name="", date_from="", date_to="", limit=5, offset=0))
    assert payload["total"] == 1
    assert payload["items"][0]["close"] == 100.5