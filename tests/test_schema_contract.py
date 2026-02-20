import sqlite3


def test_views_exist(repo):
    rows = repo.query(
        "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"
    )
    names = {r["name"] for r in rows}
    assert "core_market_dataset_v1" in names
    assert "benchmark_dataset_v1" in names
    assert "trading_calendar_v1" in names


def test_daily_market_constraint_volume(repo):
    repo.upsert_instruments(
        [
            {
                "instrument_id": "i1",
                "external_code": "0001",
                "market_code": "KOSDAQ",
                "instrument_name": "A",
                "listing_date": "2020-01-01",
                "delisting_date": None,
                "source_name": "t",
                "collected_at": "2026-01-01T00:00:00",
                "updated_at": None,
            }
        ]
    )
    try:
        repo.upsert_daily_market(
            [
                {
                    "instrument_id": "i1",
                    "trade_date": "2026-01-02",
                    "open": 10,
                    "high": 11,
                    "low": 9,
                    "close": 10,
                    "volume": -1,
                    "turnover_value": 10,
                    "market_value": 10,
                    "is_trade_halted": False,
                    "is_under_supervision": False,
                    "record_status": "VALID",
                    "source_name": "t",
                    "collected_at": "2026-01-01T00:00:00",
                    "run_id": None,
                }
            ]
        )
        assert False, "Expected IntegrityError"
    except sqlite3.IntegrityError:
        assert True


def test_run_status_constraint(repo):
    try:
        repo.insert_run(
            {
                "run_id": "r1",
                "pipeline_name": "p1",
                "source_name": "s1",
                "window_start": "2026-01-01",
                "window_end": "2026-01-02",
                "status": "WRONG",
                "started_at": "2026-01-01T00:00:00",
            }
        )
        assert False, "Expected IntegrityError"
    except sqlite3.IntegrityError:
        assert True


def test_benchmark_constraint_ohlc(repo):
    try:
        repo.upsert_benchmark(
            [
                {
                    "index_code": "KOSDAQ",
                    "trade_date": "2026-01-02",
                    "open": 100,
                    "high": 99,
                    "low": 98,
                    "close": 99.5,
                    "source_name": "t",
                    "collected_at": "2026-01-01T00:00:00",
                    "run_id": None,
                }
            ]
        )
        assert False, "Expected IntegrityError"
    except sqlite3.IntegrityError:
        assert True


def test_data_quality_issues_severity_constraint(repo):
    try:
        repo.insert_issues(
            [
                {
                    "dataset_name": "daily_market_data",
                    "trade_date": "2026-01-02",
                    "instrument_id": None,
                    "index_code": None,
                    "issue_code": "X",
                    "severity": "FATAL",
                    "issue_detail": "bad",
                    "source_name": "t",
                    "detected_at": "2026-01-01T00:00:00",
                    "run_id": None,
                    "resolved_at": None,
                }
            ]
        )
        assert False, "Expected IntegrityError"
    except sqlite3.IntegrityError:
        assert True


def test_v1_view_columns_are_backward_compatible(repo):
    core_cols = [r["name"] for r in repo.query("PRAGMA table_info(core_market_dataset_v1)")]
    benchmark_cols = [r["name"] for r in repo.query("PRAGMA table_info(benchmark_dataset_v1)")]
    calendar_cols = [r["name"] for r in repo.query("PRAGMA table_info(trading_calendar_v1)")]

    assert core_cols == [
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
    assert benchmark_cols == [
        "index_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "source_name",
        "collected_at",
    ]
    assert calendar_cols == [
        "market_code",
        "trade_date",
        "is_open",
        "holiday_name",
        "source_name",
        "collected_at",
    ]
