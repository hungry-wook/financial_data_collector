from datetime import date

import pytest

from financial_data_collector.collectors import BenchmarkCollector, DailyMarketCollector, InstrumentCollector
from financial_data_collector.krx_client import KRXClient, KRXClientConfig, KRXClientError
from financial_data_collector.runs import RunManager
from financial_data_collector.settings import KRXSettings, load_dotenv
from financial_data_collector.validation import ValidationJob


def _load_and_validate_or_skip() -> KRXSettings:
    load_dotenv(".env")
    s = KRXSettings.from_env()
    try:
        import pykrx_openapi  # noqa: F401
    except Exception:
        pytest.skip("pykrx_openapi is not installed in current environment")
    try:
        s.validate()
    except ValueError as exc:
        pytest.skip(f"Integration env is not ready: {exc}")
    return s


def _build_client_or_skip() -> KRXClient:
    s = _load_and_validate_or_skip()
    return KRXClient(KRXClientConfig.from_settings(s))


def _call_or_skip_network(fn):
    try:
        return fn()
    except KRXClientError as exc:
        if "Connection error" in str(exc) or "WinError 10013" in str(exc):
            pytest.skip(f"Network is blocked in this environment: {exc}")
        raise


@pytest.mark.integration
def test_real_krx_instruments_call_smoke():
    client = _build_client_or_skip()
    payload = _call_or_skip_network(lambda: client.get_instruments("KOSDAQ", date.today()))
    assert isinstance(payload, dict)
    assert len(payload.keys()) > 0


@pytest.mark.integration
def test_real_krx_daily_market_call_smoke():
    client = _build_client_or_skip()
    payload = _call_or_skip_network(lambda: client.get_daily_market("KOSDAQ", date.today()))
    assert isinstance(payload, dict)
    assert len(payload.keys()) > 0


@pytest.mark.integration
def test_real_krx_index_call_smoke():
    client = _build_client_or_skip()
    payload = _call_or_skip_network(lambda: client.get_index_daily("KOSDAQ", date.today()))
    assert isinstance(payload, dict)
    assert len(payload.keys()) > 0


@pytest.mark.integration
def test_real_krx_smoke_plus_validation_issue_logging(repo):
    client = _build_client_or_skip()
    base_day = date.today()

    _call_or_skip_network(lambda: client.get_instruments("KOSDAQ", base_day))
    _call_or_skip_network(lambda: client.get_daily_market("KOSDAQ", base_day))
    _call_or_skip_network(lambda: client.get_index_daily("KOSDAQ", base_day))

    run_id = RunManager(repo).start("phase1-integration-smoke", "krx", str(base_day), str(base_day))
    InstrumentCollector(repo).collect(
        [
            {
                "instrument_id": "i_integration",
                "external_code": "0001",
                "market_code": "KOSDAQ",
                "instrument_name": "Integration Sample",
                "listing_date": date(2020, 1, 1),
            }
        ],
        "krx",
    )

    DailyMarketCollector(repo).collect(
        [
            {
                "instrument_id": "i_integration",
                "trade_date": base_day,
                "open": 100,
                "high": 99,
                "low": 95,
                "close": 98,
                "volume": -1,
            }
        ],
        "krx",
        run_id,
    )
    BenchmarkCollector(repo).collect(
        [{"index_code": "UNKNOWN", "trade_date": base_day, "open": 100, "high": 101, "low": 99, "close": 100}],
        "krx",
        run_id,
    )
    validation_result = ValidationJob(repo).validate_range("KOSDAQ", str(base_day), str(base_day), run_id)

    issue_rows = repo.query("SELECT issue_code FROM data_quality_issues WHERE run_id = ?", (run_id,))
    issue_codes = {r["issue_code"] for r in issue_rows}
    assert "INVALID_DAILY_MARKET_ROW" in issue_codes
    assert "UNMAPPED_INDEX_CODE" in issue_codes
    assert isinstance(validation_result["issues"], int)
