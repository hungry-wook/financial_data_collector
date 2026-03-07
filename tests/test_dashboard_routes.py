import asyncio
from datetime import date

from financial_data_collector.collectors import BenchmarkCollector
from financial_data_collector.dashboard_routes import (
    get_benchmark_series,
    get_instrument_options,
    get_instrument_profile,
    get_instruments,
    get_prices,
)


class _DummyState:
    def __init__(self, repo):
        self.repo = repo


class _DummyApp:
    def __init__(self, repo):
        self.state = _DummyState(repo)


class _DummyRequest:
    def __init__(self, repo):
        self.app = _DummyApp(repo)


def test_dashboard_instruments_includes_delisting_snapshot_fields(repo):
    repo.upsert_instruments(
        [
            {
                "instrument_id": "550e8400-e29b-41d4-a716-446655449999",
                "external_code": "123456",
                "market_code": "KOSDAQ",
                "instrument_name": "Test Name",
                "instrument_name_abbr": None,
                "instrument_name_eng": None,
                "listing_date": "2020-01-01",
                "delisting_date": "2026-01-15",
                "listed_shares": None,
                "security_group": None,
                "sector_name": None,
                "stock_type": None,
                "par_value": None,
                "source_name": "krx",
                "collected_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            }
        ]
    )
    repo.upsert_delisting_snapshot(
        [
            {
                "market_code": "KOSDAQ",
                "external_code": "123456",
                "delisting_date": "2026-01-15",
                "delisting_reason": "delisting reason",
                "note": "delisting note",
            }
        ],
        source_name="kind",
        run_id=None,
    )

    payload = asyncio.run(
        get_instruments(
            _DummyRequest(repo),
            search="",
            external_code="",
            instrument_name="",
            market_code="",
            security_group="",
            sector_name="",
            listed_status="delisted",
            sort_by="market_code",
            sort_order="asc",
            page=1,
            size=20,
        )
    )

    assert payload["total"] == 1
    assert payload["items"][0]["external_code"] == "123456"
    assert payload["items"][0]["delisting_reason"] == "delisting reason"
    assert payload["items"][0]["delisting_note"] == "delisting note"


def test_dashboard_instrument_options_search(repo):
    repo.upsert_instruments(
        [
            {
                "instrument_id": "b8ecf870-abff-4dbf-9023-9b1fd6be4ddb",
                "external_code": "111111",
                "market_code": "KOSPI",
                "instrument_name": "Alpha Inc",
                "instrument_name_abbr": None,
                "instrument_name_eng": None,
                "listing_date": "2021-01-01",
                "delisting_date": None,
                "listed_shares": None,
                "security_group": None,
                "sector_name": None,
                "stock_type": None,
                "par_value": None,
                "source_name": "krx",
                "collected_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            },
            {
                "instrument_id": "973c819f-661d-4f1b-b2dd-47605da5f30e",
                "external_code": "222222",
                "market_code": "KOSDAQ",
                "instrument_name": "Beta Corp",
                "instrument_name_abbr": None,
                "instrument_name_eng": None,
                "listing_date": "2020-01-01",
                "delisting_date": None,
                "listed_shares": None,
                "security_group": None,
                "sector_name": None,
                "stock_type": None,
                "par_value": None,
                "source_name": "krx",
                "collected_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            },
        ]
    )

    payload = asyncio.run(get_instrument_options(_DummyRequest(repo), q="Alpha", limit=20, offset=0))

    assert payload["total"] == 1
    assert payload["has_more"] is False
    assert len(payload["items"]) == 1
    assert payload["items"][0]["external_code"] == "111111"
    assert payload["items"][0]["instrument_name"] == "Alpha Inc"
    assert payload["items"][0]["listed_status"] == "listed"


def test_dashboard_instrument_options_supports_offset(repo):
    repo.upsert_instruments(
        [
            {
                "instrument_id": "2e44fb8b-7341-4d2f-a2c9-c7e1946d72cf",
                "external_code": "100001",
                "market_code": "KOSDAQ",
                "instrument_name": "Offset A",
                "instrument_name_abbr": None,
                "instrument_name_eng": None,
                "listing_date": "2021-01-01",
                "delisting_date": None,
                "listed_shares": None,
                "security_group": None,
                "sector_name": None,
                "stock_type": None,
                "par_value": None,
                "source_name": "krx",
                "collected_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            },
            {
                "instrument_id": "cfd609a4-1840-4ae2-a6e2-c2125c4d3345",
                "external_code": "100002",
                "market_code": "KOSDAQ",
                "instrument_name": "Offset B",
                "instrument_name_abbr": None,
                "instrument_name_eng": None,
                "listing_date": "2021-01-01",
                "delisting_date": None,
                "listed_shares": None,
                "security_group": None,
                "sector_name": None,
                "stock_type": None,
                "par_value": None,
                "source_name": "krx",
                "collected_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            },
        ]
    )

    first = asyncio.run(get_instrument_options(_DummyRequest(repo), q="Offset", limit=1, offset=0))
    second = asyncio.run(get_instrument_options(_DummyRequest(repo), q="Offset", limit=1, offset=1))

    assert first["total"] == 2
    assert first["has_more"] is True
    assert len(first["items"]) == 1
    assert second["total"] == 2
    assert second["has_more"] is False
    assert len(second["items"]) == 1
    assert first["items"][0]["external_code"] != second["items"][0]["external_code"]


def test_dashboard_instrument_profile_prioritizes_listed_record(repo):
    repo.upsert_instruments(
        [
            {
                "instrument_id": "d734365f-0f29-40f0-abf2-05b1dd03f7d3",
                "external_code": "333333",
                "market_code": "KOSDAQ",
                "instrument_name": "Gamma Corp Old",
                "instrument_name_abbr": None,
                "instrument_name_eng": None,
                "listing_date": "2010-01-01",
                "delisting_date": "2022-12-31",
                "listed_shares": None,
                "security_group": "OLD",
                "sector_name": "Legacy",
                "stock_type": None,
                "par_value": None,
                "source_name": "krx",
                "collected_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            },
            {
                "instrument_id": "7e5426fb-0d4f-4261-b2a1-cfa73f8ecbcc",
                "external_code": "333333",
                "market_code": "KOSPI",
                "instrument_name": "Gamma Corp",
                "instrument_name_abbr": None,
                "instrument_name_eng": None,
                "listing_date": "2022-01-01",
                "delisting_date": None,
                "listed_shares": None,
                "security_group": "COMMON",
                "sector_name": "Tech",
                "stock_type": None,
                "par_value": None,
                "source_name": "krx",
                "collected_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            },
        ]
    )

    payload = asyncio.run(get_instrument_profile("333333", _DummyRequest(repo)))

    assert payload["external_code"] == "333333"
    assert payload["instrument_name"] == "Gamma Corp"
    assert payload["market_code"] == "KOSPI"
    assert payload["listed_status"] == "listed"


def test_dashboard_prices_supports_pagination(repo):
    instrument_id = "8d4ff4fb-11a3-492e-92b7-b9cf4076e50f"
    repo.upsert_instruments(
        [
            {
                "instrument_id": instrument_id,
                "external_code": "444444",
                "market_code": "KOSDAQ",
                "instrument_name": "Paged Price",
                "instrument_name_abbr": None,
                "instrument_name_eng": None,
                "listing_date": "2020-01-01",
                "delisting_date": None,
                "listed_shares": None,
                "security_group": None,
                "sector_name": None,
                "stock_type": None,
                "par_value": None,
                "source_name": "krx",
                "collected_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            }
        ]
    )
    repo.upsert_daily_market(
        [
            {
                "instrument_id": instrument_id,
                "trade_date": "2026-01-01",
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "volume": 1000,
                "turnover_value": 10000,
                "change_rate": 1.0,
                "record_status": "VALID",
                "source_name": "krx",
                "collected_at": "2026-01-01T00:00:00Z",
            },
            {
                "instrument_id": instrument_id,
                "trade_date": "2026-01-02",
                "open": 11.0,
                "high": 12.0,
                "low": 10.5,
                "close": 11.5,
                "volume": 1100,
                "turnover_value": 11000,
                "change_rate": 2.0,
                "record_status": "VALID",
                "source_name": "krx",
                "collected_at": "2026-01-02T00:00:00Z",
            },
            {
                "instrument_id": instrument_id,
                "trade_date": "2026-01-03",
                "open": 12.0,
                "high": 13.0,
                "low": 11.5,
                "close": 12.5,
                "volume": 1200,
                "turnover_value": 12000,
                "change_rate": 3.0,
                "record_status": "VALID",
                "source_name": "krx",
                "collected_at": "2026-01-03T00:00:00Z",
            },
        ]
    )

    first = asyncio.run(
        get_prices(
            _DummyRequest(repo),
            external_code="444444",
            date_from="",
            date_to="",
            limit=2,
            offset=0,
        )
    )
    second = asyncio.run(
        get_prices(
            _DummyRequest(repo),
            external_code="444444",
            date_from="",
            date_to="",
            limit=2,
            offset=2,
        )
    )

    assert first["total"] == 3
    assert first["has_more"] is True
    assert len(first["items"]) == 2
    assert first["items"][0]["trade_date"] == "2026-01-03"
    assert second["total"] == 3
    assert second["has_more"] is False
    assert len(second["items"]) == 1
    assert second["items"][0]["trade_date"] == "2026-01-01"



def test_dashboard_benchmark_detail_defaults_to_available_series(repo):
    BenchmarkCollector(repo).collect(
        [
            {
                "index_code": "KOSDAQ",
                "index_name": "KOSDAQ_PRIMARY",
                "trade_date": date(2026, 1, 2),
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100.5,
            }
        ],
        "krx",
        "r1",
    )

    payload = asyncio.run(
        get_benchmark_series(
            "KOSDAQ",
            _DummyRequest(repo),
            series_name="",
            date_from="",
            date_to="",
            limit=5,
            offset=0,
        )
    )

    assert payload["total"] == 1
    assert len(payload["items"]) == 1
    assert float(payload["items"][0]["close"]) == 100.5



def test_dashboard_prices_returns_all_rows_without_date_filter(repo):
    instrument_id = "c1c3e6ec-0ddb-44db-ae11-62f0842fb95d"
    repo.upsert_instruments(
        [
            {
                "instrument_id": instrument_id,
                "external_code": "555555",
                "market_code": "KOSPI",
                "instrument_name": "Full History",
                "instrument_name_abbr": None,
                "instrument_name_eng": None,
                "listing_date": "2020-01-01",
                "delisting_date": None,
                "listed_shares": None,
                "security_group": None,
                "sector_name": None,
                "stock_type": None,
                "par_value": None,
                "source_name": "krx",
                "collected_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
            }
        ]
    )
    repo.upsert_daily_market(
        [
            {
                "instrument_id": instrument_id,
                "trade_date": "2024-12-31",
                "open": 9.0,
                "high": 10.0,
                "low": 8.5,
                "close": 9.5,
                "volume": 900,
                "turnover_value": 9000,
                "change_rate": -1.0,
                "record_status": "VALID",
                "source_name": "krx",
                "collected_at": "2024-12-31T00:00:00Z",
            },
            {
                "instrument_id": instrument_id,
                "trade_date": "2025-12-31",
                "open": 19.0,
                "high": 20.0,
                "low": 18.5,
                "close": 19.5,
                "volume": 1900,
                "turnover_value": 19000,
                "change_rate": 1.0,
                "record_status": "VALID",
                "source_name": "krx",
                "collected_at": "2025-12-31T00:00:00Z",
            },
        ]
    )

    payload = asyncio.run(
        get_prices(
            _DummyRequest(repo),
            external_code="555555",
            date_from="",
            date_to="",
            limit=10,
            offset=0,
        )
    )

    assert payload["total"] == 2
    assert [row["trade_date"] for row in payload["items"]] == ["2025-12-31", "2024-12-31"]

def test_dashboard_benchmark_detail_prefers_representative_series(repo):
    BenchmarkCollector(repo).collect(
        [
            {
                "index_code": "KOSDAQ",
                "index_name": "건설",
                "trade_date": date(2026, 1, 2),
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100.5,
            },
            {
                "index_code": "KOSDAQ",
                "index_name": "코스닥",
                "trade_date": date(2026, 1, 2),
                "open": 1100,
                "high": 1101,
                "low": 1099,
                "close": 1100.5,
            }
        ],
        "krx",
        "r1",
    )

    payload = asyncio.run(
        get_benchmark_series(
            "KOSDAQ",
            _DummyRequest(repo),
            series_name="",
            date_from="",
            date_to="",
            limit=5,
            offset=0,
        )
    )

    assert payload["total"] == 1
    assert payload["items"][0]["close"] == 1100.5
