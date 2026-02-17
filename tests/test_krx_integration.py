from datetime import date

import pytest

from financial_data_collector.krx_client import KRXClient, KRXClientConfig
from financial_data_collector.settings import KRXSettings, load_dotenv


def _load_and_validate_or_skip() -> KRXSettings:
    load_dotenv(".env")
    s = KRXSettings.from_env()
    try:
        s.validate()
    except ValueError as exc:
        pytest.skip(f"Integration env is not ready: {exc}")
    return s


@pytest.mark.integration
def test_real_krx_instruments_call_smoke():
    s = _load_and_validate_or_skip()
    client = KRXClient(KRXClientConfig.from_settings(s))
    payload = client.get_instruments("KOSDAQ", date.today())
    assert isinstance(payload, dict)
    assert len(payload.keys()) > 0


@pytest.mark.integration
def test_real_krx_index_call_smoke():
    s = _load_and_validate_or_skip()
    client = KRXClient(KRXClientConfig.from_settings(s))
    payload = client.get_index_daily("KOSDAQ", date.today())
    assert isinstance(payload, dict)
    assert len(payload.keys()) > 0

