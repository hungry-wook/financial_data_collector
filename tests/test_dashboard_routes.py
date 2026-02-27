import asyncio

from financial_data_collector.dashboard_routes import (
    get_instrument_options,
    get_instrument_profile,
    get_instruments,
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

    payload = asyncio.run(get_instrument_options(_DummyRequest(repo), q="Alpha", limit=20))

    assert len(payload) == 1
    assert payload[0]["external_code"] == "111111"
    assert payload[0]["instrument_name"] == "Alpha Inc"
    assert payload[0]["listed_status"] == "listed"


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
