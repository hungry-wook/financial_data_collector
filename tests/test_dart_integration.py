from datetime import date, timedelta

import pytest

from financial_data_collector.dart_client import DARTClient, DARTClientConfig, DARTClientError
from financial_data_collector.settings import OpenDARTSettings, load_dotenv


def _load_and_validate_or_skip() -> OpenDARTSettings:
    load_dotenv(".env")
    s = OpenDARTSettings.from_env()
    try:
        s.validate()
    except ValueError as exc:
        pytest.skip(f"Integration env is not ready: {exc}")
    return s


@pytest.mark.integration
def test_real_dart_list_call_smoke():
    s = _load_and_validate_or_skip()
    client = DARTClient(DARTClientConfig.from_settings(s))

    end_de = date.today()
    bgn_de = end_de - timedelta(days=3)

    try:
        payload = client.list_filings(
            bgn_de=bgn_de,
            end_de=end_de,
            pblntf_ty="B",
            page_no=1,
            page_count=20,
            last_reprt_at="Y",
        )
    except DARTClientError as exc:
        if "Connection error" in str(exc) or "WinError 10013" in str(exc):
            pytest.skip(f"Network is blocked in this environment: {exc}")
        raise

    assert payload.get("status") in {"000", "013"}
    assert "message" in payload
    assert "list" in payload
