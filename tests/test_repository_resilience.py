from datetime import date

import pytest

from financial_data_collector.collectors import DailyMarketCollector
from financial_data_collector.repository import Repository


class _DummyConn:
    def __init__(self):
        self.committed = False
        self.closed = False

    def execute(self, *_args, **_kwargs):
        return None

    def commit(self):
        self.committed = True

    def rollback(self):
        pass

    def close(self):
        self.closed = True


def test_repository_connect_retries_operational_error(monkeypatch):
    attempts = {"count": 0}

    def fake_connect(*_args, **_kwargs):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise pytest.importorskip("psycopg").OperationalError("Address already in use")
        return _DummyConn()

    import financial_data_collector.repository as repository_module

    monkeypatch.setattr(repository_module.psycopg, "connect", fake_connect)

    repo = Repository("postgresql://ignored")
    with repo.connect() as conn:
        assert isinstance(conn, _DummyConn)
    assert attempts["count"] == 3


def test_daily_market_collector_batches_missing_instrument_lookup(repo, monkeypatch):
    collector = DailyMarketCollector(repo)
    rows = [
        {
            "instrument_id": "i-missing-1",
            "external_code": "000001",
            "market_code": "KOSDAQ",
            "trade_date": date(2026, 1, 2),
            "open": 10,
            "high": 12,
            "low": 9,
            "close": 11,
            "volume": 100,
        },
        {
            "instrument_id": "i-missing-2",
            "external_code": "000002",
            "market_code": "KOSDAQ",
            "trade_date": date(2026, 1, 2),
            "open": 20,
            "high": 22,
            "low": 19,
            "close": 21,
            "volume": 200,
        },
    ]

    batch_calls = {"count": 0}
    original_get_existing = repo.get_existing_instrument_ids
    original_query = repo.query

    def wrapped_get_existing(instrument_ids):
        batch_calls["count"] += 1
        return original_get_existing(instrument_ids)

    def wrapped_query(query_text, params=()):
        if "SELECT 1 AS ok FROM instruments" in query_text:
            pytest.fail("legacy per-instrument existence lookup should not run")
        return original_query(query_text, params)

    monkeypatch.setattr(repo, "get_existing_instrument_ids", wrapped_get_existing)
    monkeypatch.setattr(repo, "query", wrapped_query)

    count = collector.collect(rows, "krx", "r1")
    assert count == 2
    assert batch_calls["count"] == 1

    saved = repo.query(
        "SELECT COUNT(*) AS c FROM daily_market_data WHERE trade_date = %s",
        ("2026-01-02",),
    )
    assert saved[0]["c"] == 2
