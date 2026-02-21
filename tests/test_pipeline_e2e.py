from datetime import date

import pytest

from financial_data_collector.pipeline import Phase1Pipeline


def test_phase1_pipeline_end_to_end(repo):
    p = Phase1Pipeline(repo)
    result = p.run(
        market_code="KOSDAQ",
        date_from=date(2026, 1, 2),
        date_to=date(2026, 1, 3),
        instruments=[
            {
                "instrument_id": "i1",
                "external_code": "0001",
                "market_code": "KOSDAQ",
                "instrument_name": "A",
                "listing_date": date(2020, 1, 1),
            }
        ],
        daily_market=[
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
                "is_trade_halted": False,
                "is_under_supervision": False,
            }
        ],
        benchmark=[
            {"index_code": "KOSDAQ", "trade_date": date(2026, 1, 2), "open": 100, "high": 101, "low": 99, "close": 100.5}
        ],
    )
    assert result["counts"]["instruments"] == 1
    assert result["counts"]["daily"] == 1
    assert result["counts"]["benchmark"] == 1
    assert len(repo.query("SELECT * FROM core_market_dataset_v1")) == 1


def test_phase1_pipeline_marks_failed_on_mid_run_error(repo, monkeypatch):
    p = Phase1Pipeline(repo)

    def _boom(*args, **kwargs):
        raise RuntimeError("benchmark collector failed")

    monkeypatch.setattr(p.benchmark_collector, "collect", _boom)

    with pytest.raises(RuntimeError):
        p.run(
            market_code="KOSDAQ",
            date_from=date(2026, 1, 2),
            date_to=date(2026, 1, 3),
            instruments=[
                {
                    "instrument_id": "i1",
                    "external_code": "0001",
                    "market_code": "KOSDAQ",
                    "instrument_name": "A",
                    "listing_date": date(2020, 1, 1),
                }
            ],
            daily_market=[
                {
                    "instrument_id": "i1",
                    "trade_date": date(2026, 1, 2),
                    "open": 10,
                    "high": 12,
                    "low": 9,
                    "close": 11,
                    "volume": 100,
                }
            ],
            benchmark=[
                {"index_code": "KOSDAQ", "trade_date": date(2026, 1, 2), "open": 100, "high": 101, "low": 99, "close": 100.5}
            ],
        )

    run = repo.query("SELECT status FROM collection_runs ORDER BY started_at DESC LIMIT 1")[0]
    assert run["status"] == "FAILED"
    assert len(repo.query("SELECT * FROM instruments")) == 1
    assert len(repo.query("SELECT * FROM daily_market_data")) == 1
    assert len(repo.query("SELECT * FROM benchmark_index_data")) == 0


def test_phase1_pipeline_marks_partial_when_only_warnings(repo, monkeypatch):
    p = Phase1Pipeline(repo)

    monkeypatch.setattr(
        p.validation,
        "validate_range",
        lambda *args, **kwargs: {
            "issues_total": 2,
            "errors": 0,
            "warnings": 2,
            "infos": 0,
            "rows_checked": 1,
        },
    )

    p.run(
        market_code="KOSDAQ",
        date_from=date(2026, 1, 2),
        date_to=date(2026, 1, 2),
        instruments=[
            {
                "instrument_id": "i1",
                "external_code": "0001",
                "market_code": "KOSDAQ",
                "instrument_name": "A",
                "listing_date": date(2020, 1, 1),
            }
        ],
        daily_market=[
            {
                "instrument_id": "i1",
                "trade_date": date(2026, 1, 2),
                "open": 10,
                "high": 12,
                "low": 9,
                "close": 11,
                "volume": 100,
            }
        ],
        benchmark=[
            {"index_code": "KOSDAQ", "trade_date": date(2026, 1, 2), "open": 100, "high": 101, "low": 99, "close": 100.5}
        ],
    )

    run = repo.query("SELECT status, failure_count, warning_count FROM collection_runs ORDER BY started_at DESC LIMIT 1")[0]
    assert run["status"] == "PARTIAL"
    assert run["failure_count"] == 0
    assert run["warning_count"] == 2
