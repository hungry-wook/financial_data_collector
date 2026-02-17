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

