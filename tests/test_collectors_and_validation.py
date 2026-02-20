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


def test_instrument_date_normalization_failure_records_issue(repo):
    c = InstrumentCollector(repo)
    count = c.collect(
        [
            {
                "instrument_id": "i_bad",
                "external_code": "0099",
                "market_code": "KOSDAQ",
                "instrument_name": "Bad Date",
                "listing_date": "not-a-date",
            }
        ],
        "krx",
    )
    assert count == 0
    issue = repo.query("SELECT issue_code FROM data_quality_issues WHERE instrument_id='i_bad'")[0]
    assert issue["issue_code"] == "DATE_NORMALIZATION_FAILED"


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


def test_daily_invalid_row_records_issue(repo):
    _seed_instrument(repo)
    d = DailyMarketCollector(repo)
    count = d.collect(
        [
            {
                "instrument_id": "i1",
                "trade_date": date(2026, 1, 2),
                "open": 10,
                "high": 9,
                "low": 8,
                "close": 11,
                "volume": -100,
            }
        ],
        "krx",
        "r1",
    )
    assert count == 0
    issue = repo.query("SELECT issue_code FROM data_quality_issues WHERE run_id='r1'")[0]
    assert issue["issue_code"] == "INVALID_DAILY_MARKET_ROW"


def test_benchmark_unmapped_index_code_records_issue(repo):
    b = BenchmarkCollector(repo)
    count = b.collect(
        [{"index_code": "UNKNOWN", "trade_date": date(2026, 1, 2), "open": 100, "high": 101, "low": 99, "close": 100.5}],
        "krx",
        "r1",
    )
    assert count == 0
    issue = repo.query("SELECT issue_code FROM data_quality_issues WHERE run_id='r1'")[0]
    assert issue["issue_code"] == "UNMAPPED_INDEX_CODE"


def test_benchmark_missing_day_records_issue(repo):
    b = BenchmarkCollector(repo)
    count = b.collect(
        [
            {"index_code": "KOSDAQ", "trade_date": date(2026, 1, 2), "open": 100, "high": 101, "low": 99, "close": 100.5},
            {"index_code": "KOSDAQ", "trade_date": date(2026, 1, 4), "open": 101, "high": 102, "low": 100, "close": 101.5},
        ],
        "krx",
        "r1",
    )
    assert count == 2
    issue = repo.query("SELECT issue_code, trade_date FROM data_quality_issues WHERE issue_code='BENCHMARK_DAY_MISSING'")[0]
    assert issue["trade_date"] == "2026-01-03"


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


def test_validation_no_issue_for_fully_valid_range(repo):
    _seed_instrument(repo)
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
    TradingCalendarBuilder(repo).build_from_index_days(
        market_code="KOSDAQ",
        date_from=date(2026, 1, 2),
        date_to=date(2026, 1, 2),
        index_trade_dates=[date(2026, 1, 2)],
        source_name="krx",
        run_id="r1",
    )
    v = ValidationJob(repo).validate_range("KOSDAQ", "2026-01-02", "2026-01-02", "r1")
    assert v["issues"] == 0
    assert len(repo.query("SELECT * FROM data_quality_issues")) == 0
