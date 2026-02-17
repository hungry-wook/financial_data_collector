from datetime import date

from financial_data_collector.calendar_builder import TradingCalendarBuilder
from financial_data_collector.collectors import BenchmarkCollector, DailyMarketCollector, InstrumentCollector
from financial_data_collector.validation import ValidationJob


def _seed_instrument(repo):
    InstrumentCollector(repo).collect(
        [
            {
                "instrument_id": "i1",
                "external_code": "0001",
                "market_code": "kosdaq",
                "instrument_name": "A",
                "listing_date": date(2020, 1, 1),
            }
        ],
        "krx",
    )


def test_instrument_collect_upsert(repo):
    c = InstrumentCollector(repo)
    count = c.collect(
        [
            {
                "instrument_id": "i1",
                "external_code": "0001",
                "market_code": "kosdaq",
                "instrument_name": "A",
                "listing_date": date(2020, 1, 1),
            }
        ],
        "krx",
    )
    assert count == 1
    rows = repo.query("SELECT market_code FROM instruments WHERE instrument_id='i1'")
    assert rows[0]["market_code"] == "KOSDAQ"


def test_daily_and_benchmark_collect(repo):
    _seed_instrument(repo)
    d = DailyMarketCollector(repo)
    b = BenchmarkCollector(repo)
    d.collect(
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
    b.collect(
        [{"index_code": "KOSDAQ", "trade_date": date(2026, 1, 2), "open": 100, "high": 101, "low": 99, "close": 100.5}],
        "krx",
        "r1",
    )
    assert len(repo.query("SELECT * FROM daily_market_data")) == 1
    assert len(repo.query("SELECT * FROM benchmark_index_data")) == 1


def test_calendar_builder(repo):
    builder = TradingCalendarBuilder(repo)
    count = builder.build_from_index_days(
        market_code="KOSDAQ",
        date_from=date(2026, 1, 1),
        date_to=date(2026, 1, 3),
        index_trade_dates=[date(2026, 1, 2)],
        source_name="krx",
        run_id="r1",
    )
    assert count == 3
    open_days = repo.query("SELECT COUNT(1) AS c FROM trading_calendar WHERE is_open=1")[0]["c"]
    assert open_days == 1


def test_validation_open_day_missing_issue(repo):
    _seed_instrument(repo)
    builder = TradingCalendarBuilder(repo)
    builder.build_from_index_days(
        market_code="KOSDAQ",
        date_from=date(2026, 1, 2),
        date_to=date(2026, 1, 2),
        index_trade_dates=[date(2026, 1, 2)],
        source_name="krx",
        run_id="r1",
    )
    v = ValidationJob(repo).validate_range("KOSDAQ", "2026-01-02", "2026-01-02", "r1")
    assert v["issues"] == 1
    issues = repo.query("SELECT issue_code FROM data_quality_issues")
    assert issues[0]["issue_code"] == "OPEN_DAY_TOTAL_MISSING"

