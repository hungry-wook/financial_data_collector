from datetime import date

import pytest

from financial_data_collector.krx_client import KRXClient, KRXClientConfig, KRXClientError


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, params, headers, timeout):
        self.calls.append((url, params, headers, timeout))
        return self.responses.pop(0)


def test_auth_header_is_present():
    session = FakeSession([FakeResponse(200, {"items": []})])
    client = KRXClient(KRXClientConfig(base_url="https://example.com", auth_key="k"), session=session)
    client.get_instruments("KOSDAQ", date(2026, 1, 1))
    assert session.calls[0][2]["AUTH_KEY"] == "k"


def test_retry_then_success():
    session = FakeSession([FakeResponse(500, {}), FakeResponse(200, {"items": [1]})])
    client = KRXClient(
        KRXClientConfig(base_url="https://example.com", auth_key="k", max_retries=2),
        session=session,
    )
    payload = client.get_daily_market("KOSDAQ", date(2026, 1, 1))
    assert payload["items"] == [1]


def test_daily_limit_guard():
    session = FakeSession([FakeResponse(200, {"items": []})])
    client = KRXClient(
        KRXClientConfig(base_url="https://example.com", auth_key="k", daily_limit=0),
        session=session,
    )
    with pytest.raises(KRXClientError):
        client.get_index_daily("KOSDAQ", date(2026, 1, 1))

