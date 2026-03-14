from datetime import date

import pytest
import requests

from financial_data_collector.dart_client import DARTClient, DARTClientConfig, DARTClientError


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, content=b"", raise_for_status_exc=None):
        self.status_code = status_code
        self._payload = payload
        self.content = content
        self._raise_for_status_exc = raise_for_status_exc

    def raise_for_status(self):
        if self._raise_for_status_exc:
            raise self._raise_for_status_exc

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return self.responses.pop(0)


def test_list_filings_calls_ds001_list_with_expected_params():
    session = _FakeSession([_FakeResponse(payload={"status": "000", "list": []})])
    client = DARTClient(DARTClientConfig(api_key="k"), session=session)

    payload = client.list_filings(
        bgn_de=date(2026, 1, 1),
        end_de=date(2026, 1, 31),
        pblntf_ty="B",
        page_no=2,
        page_count=50,
    )

    assert payload["status"] == "000"
    call = session.calls[0]
    assert call["url"].endswith("/list.json")
    assert call["params"]["crtfc_key"] == "k"
    assert call["params"]["bgn_de"] == "20260101"
    assert call["params"]["end_de"] == "20260131"
    assert call["params"]["pblntf_ty"] == "B"
    assert call["params"]["last_reprt_at"] == "Y"


def test_list_filings_allows_no_data_status_013():
    session = _FakeSession([_FakeResponse(payload={"status": "013", "message": "no data", "list": []})])
    client = DARTClient(DARTClientConfig(api_key="k"), session=session)

    payload = client.list_filings(bgn_de=date(2026, 1, 1), end_de=date(2026, 1, 1))
    assert payload["status"] == "013"


def test_list_filings_raises_for_dart_error_status():
    session = _FakeSession([_FakeResponse(payload={"status": "020", "message": "rate limit"})])
    client = DARTClient(DARTClientConfig(api_key="k"), session=session)

    with pytest.raises(DARTClientError):
        client.list_filings(bgn_de=date(2026, 1, 1), end_de=date(2026, 1, 1))


def test_document_zip_returns_bytes():
    session = _FakeSession([_FakeResponse(content=b"PK\x03\x04zip-data")])
    client = DARTClient(DARTClientConfig(api_key="k"), session=session)

    body = client.get_document_zip("20260101000001")
    assert body.startswith(b"PK")


def test_document_zip_raises_when_xml_error_body_returned():
    xml_error = b"<result><status>014</status><message>file does not exist</message></result>"
    session = _FakeSession([_FakeResponse(content=xml_error)])
    client = DARTClient(DARTClientConfig(api_key="k"), session=session)

    with pytest.raises(DARTClientError):
        client.get_document_zip("20260101000001")


def test_daily_limit_guard():
    session = _FakeSession([_FakeResponse(payload={"status": "000", "list": []})])
    client = DARTClient(DARTClientConfig(api_key="k", daily_limit=0), session=session)

    with pytest.raises(DARTClientError):
        client.list_filings(bgn_de=date(2026, 1, 1), end_de=date(2026, 1, 1))


def test_http_error_is_wrapped():
    session = _FakeSession(
        [
            _FakeResponse(
                payload={"status": "000"},
                raise_for_status_exc=requests.HTTPError("500 Server Error"),
            )
        ]
    )
    client = DARTClient(DARTClientConfig(api_key="k"), session=session)

    with pytest.raises(DARTClientError):
        client.list_filings(bgn_de=date(2026, 1, 1), end_de=date(2026, 1, 1))



def test_list_filings_uses_cached_payload_when_available(tmp_path):
    first = DARTClient(
        DARTClientConfig(api_key="k", cache_dir=str(tmp_path)),
        session=_FakeSession([_FakeResponse(payload={"status": "013", "message": "no data", "list": []})]),
    )
    payload = first.list_filings(bgn_de=date(2026, 1, 1), end_de=date(2026, 1, 1), pblntf_ty="B")
    assert payload["status"] == "013"

    second_session = _FakeSession([])
    second = DARTClient(DARTClientConfig(api_key="k", cache_dir=str(tmp_path)), session=second_session)
    cached = second.list_filings(bgn_de=date(2026, 1, 1), end_de=date(2026, 1, 1), pblntf_ty="B")
    assert cached["status"] == "013"
    assert second_session.calls == []


def test_document_zip_uses_cached_body_when_available(tmp_path):
    cache_root = tmp_path / "docs"
    first = DARTClient(
        DARTClientConfig(api_key="k", cache_dir=str(cache_root)),
        session=_FakeSession([_FakeResponse(content=b"PK\x03\x04cached-zip")]),
    )
    first.get_document_zip("20260101000001")

    second_session = _FakeSession([])
    second = DARTClient(DARTClientConfig(api_key="k", cache_dir=str(cache_root)), session=second_session)
    body = second.get_document_zip("20260101000001")
    assert body == b"PK\x03\x04cached-zip"
    assert second_session.calls == []



def test_list_filings_offline_only_raises_on_cache_miss(tmp_path):
    session = _FakeSession([_FakeResponse(payload={"status": "000", "list": []})])
    client = DARTClient(DARTClientConfig(api_key="k", cache_dir=str(tmp_path), offline_only=True), session=session)

    with pytest.raises(DARTClientError, match="cache miss"):
        client.list_filings(bgn_de=date(2026, 1, 1), end_de=date(2026, 1, 1))

    assert session.calls == []


def test_document_zip_offline_only_raises_on_cache_miss(tmp_path):
    session = _FakeSession([_FakeResponse(content=b"PK\x03\x04zip-data")])
    client = DARTClient(DARTClientConfig(api_key="k", cache_dir=str(tmp_path), offline_only=True), session=session)

    with pytest.raises(DARTClientError, match="cache miss"):
        client.get_document_zip("20260101000001")

    assert session.calls == []
