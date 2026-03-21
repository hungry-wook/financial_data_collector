from datetime import date

from financial_data_collector.calendar_builder import TradingCalendarBuilder
from financial_data_collector.collectors import BenchmarkCollector, DailyMarketCollector, InstrumentCollector
from financial_data_collector.validation import ValidationJob


def _seed_instrument(repo):
    InstrumentCollector(repo).collect(
        [{
            "instrument_id": "i1",
            "external_code": "0001",
            "market_code": "kosdaq",
            "instrument_name": "A",
            "listing_date": date(2020, 1, 1),
        }],
        "krx",
    )


def test_instrument_collect_upsert(repo):
    count = InstrumentCollector(repo).collect(
        [{
            "instrument_id": "i1",
            "external_code": "0001",
            "market_code": "kosdaq",
            "instrument_name": "A",
            "listing_date": date(2020, 1, 1),
        }],
        "krx",
    )
    assert count == 1
    rows = repo.query("SELECT market_code FROM instruments WHERE external_code='0001'")
    assert rows[0]["market_code"] == "KOSDAQ"


def test_daily_and_benchmark_collect(repo):
    _seed_instrument(repo)
    d = DailyMarketCollector(repo)
    b = BenchmarkCollector(repo)
    d.collect([
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
            "base_price": 10,
        }
    ], "krx", "r1")
    b.collect([
        {"index_code": "KOSDAQ", "trade_date": date(2026, 1, 2), "open": 100, "high": 101, "low": 99, "close": 100.5, "turnover_value": 10000, "market_cap": 50000}
    ], "krx", "r1")
    saved = repo.query("SELECT base_price, turnover_value, market_value FROM daily_market_data")[0]
    assert saved["base_price"] == 10.0
    assert saved["turnover_value"] == 1000.0
    assert saved["market_value"] == 5000.0
    assert len(repo.query("SELECT * FROM benchmark_index_data")) == 1


def test_calendar_builder(repo):
    count = TradingCalendarBuilder(repo).build_from_index_days(
        market_code="KOSDAQ",
        date_from=date(2026, 1, 1),
        date_to=date(2026, 1, 3),
        index_trade_dates=[date(2026, 1, 2)],
        source_name="krx",
        run_id="r1",
    )
    assert count == 3
    open_days = repo.query("SELECT COUNT(1) AS c FROM trading_calendar WHERE is_open=TRUE")[0]["c"]
    assert open_days == 1


def test_validation_open_day_missing_issue(repo):
    _seed_instrument(repo)
    TradingCalendarBuilder(repo).build_from_index_days(
        market_code="KOSDAQ",
        date_from=date(2026, 1, 2),
        date_to=date(2026, 1, 2),
        index_trade_dates=[date(2026, 1, 2)],
        source_name="krx",
        run_id="r1",
    )
    v = ValidationJob(repo).validate_range("KOSDAQ", "2026-01-02", "2026-01-02", "r1")
    assert v["warnings"] == 1
    issues = repo.query("SELECT issue_code FROM data_quality_issues")
    assert issues[0]["issue_code"] == "OPEN_DAY_TOTAL_MISSING"