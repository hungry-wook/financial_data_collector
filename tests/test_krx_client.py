from datetime import date

import pytest

from financial_data_collector.krx_client import KRXClient, KRXClientConfig, KRXClientError


class FakeOpenAPI:
    def __init__(self):
        self.calls = []

    def get_kosdaq_stock_base_info(self, bas_dd):
        self.calls.append(("get_kosdaq_stock_base_info", bas_dd))
        return {"OutBlock_1": []}

    def get_kosdaq_stock_daily_trade(self, bas_dd):
        self.calls.append(("get_kosdaq_stock_daily_trade", bas_dd))
        return {"OutBlock_1": []}

    def get_kosdaq_stock_daily_base_price(self, bas_dd):
        self.calls.append(("get_kosdaq_stock_daily_base_price", bas_dd))
        return {"OutBlock_1": []}

    def get_kosdaq_daily_trade(self, bas_dd):
        self.calls.append(("get_kosdaq_daily_trade", bas_dd))
        return {"OutBlock_1": []}


def test_daily_market_includes_basdd_param():
    client = KRXClient(KRXClientConfig(auth_key="k"), openapi_client=FakeOpenAPI())
    client.get_daily_market("KOSDAQ", date(2026, 1, 1))
    assert client.openapi_client.calls[0] == ("get_kosdaq_stock_daily_trade", "20260101")


def test_daily_base_price_includes_basdd_param():
    client = KRXClient(KRXClientConfig(auth_key="k"), openapi_client=FakeOpenAPI())
    client.get_daily_base_price("KOSDAQ", date(2026, 1, 1))
    assert client.openapi_client.calls[0] == ("get_kosdaq_stock_daily_base_price", "20260101")


def test_daily_limit_guard():
    client = KRXClient(KRXClientConfig(auth_key="k", daily_limit=0), openapi_client=FakeOpenAPI())
    with pytest.raises(KRXClientError):
        client.get_index_daily("KOSDAQ", date(2026, 1, 1))