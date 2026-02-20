from datetime import date

import pytest

from financial_data_collector.krx_client import KRXClient, KRXClientConfig, KRXClientError


class FakeOpenAPI:
    def __init__(self):
        self.calls = []
        self.api_key = None

    def get_kosdaq_stock_base_info(self, bas_dd):
        self.calls.append(("get_kosdaq_stock_base_info", bas_dd))
        return {"OutBlock_1": []}

    def get_kosdaq_stock_daily_trade(self, bas_dd):
        self.calls.append(("get_kosdaq_stock_daily_trade", bas_dd))
        return {"OutBlock_1": []}

    def get_kosdaq_daily_trade(self, bas_dd):
        self.calls.append(("get_kosdaq_daily_trade", bas_dd))
        return {"OutBlock_1": []}


def test_auth_header_is_present():
    openapi = FakeOpenAPI()
    client = KRXClient(
        KRXClientConfig(auth_key="k"),
        openapi_client=openapi,
    )
    assert client.config.auth_key == "k"


def test_daily_market_includes_basdd_param():
    openapi = FakeOpenAPI()
    client = KRXClient(
        KRXClientConfig(auth_key="k"),
        openapi_client=openapi,
    )
    client.get_daily_market("KOSDAQ", date(2026, 1, 1))
    assert openapi.calls[0] == ("get_kosdaq_stock_daily_trade", "20260101")


def test_retry_then_success():
    openapi = FakeOpenAPI()
    client = KRXClient(
        KRXClientConfig(auth_key="k"),
        openapi_client=openapi,
    )
    payload = client.get_daily_market("KOSDAQ", date(2026, 1, 1))
    assert "OutBlock_1" in payload


def test_daily_limit_guard():
    openapi = FakeOpenAPI()
    client = KRXClient(
        KRXClientConfig(auth_key="k", daily_limit=0),
        openapi_client=openapi,
    )
    with pytest.raises(KRXClientError):
        client.get_index_daily("KOSDAQ", date(2026, 1, 1))


def test_unsupported_index_code_raises_error():
    client = KRXClient(
        KRXClientConfig(auth_key="k"),
        openapi_client=FakeOpenAPI(),
    )
    with pytest.raises(KRXClientError):
        client.get_index_daily("UNKNOWN", date(2026, 1, 1))


def test_uses_pykrx_openapi_when_available():
    openapi = FakeOpenAPI()
    client = KRXClient(
        KRXClientConfig(auth_key="k"),
        openapi_client=openapi,
    )
    payload = client.get_daily_market("KOSDAQ", date(2026, 1, 1))
    assert openapi.calls[0] == ("get_kosdaq_stock_daily_trade", "20260101")
    assert "OutBlock_1" in payload
