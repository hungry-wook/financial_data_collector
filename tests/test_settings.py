import os

import pytest

from financial_data_collector.settings import KRXSettings, load_dotenv


def test_load_dotenv(tmp_path):
    f = tmp_path / ".env"
    f.write_text(
        "\n".join(
            [
                "KRX_AUTH_KEY=abc",
                "KRX_BASE_URL=https://example.com",
                "KRX_API_PATH_INSTRUMENTS=/a",
                "KRX_API_PATH_DAILY_MARKET=/b",
                "KRX_API_PATH_INDEX_DAILY=/c",
            ]
        ),
        encoding="utf-8",
    )
    loaded = load_dotenv(f.as_posix())
    assert loaded["KRX_AUTH_KEY"] == "abc"
    assert os.environ.get("KRX_BASE_URL") == "https://example.com"


def test_settings_validate_missing():
    s = KRXSettings(
        auth_key="",
        base_url="",
        api_path_instruments="",
        api_path_daily_market="",
        api_path_index_daily="",
        timeout_sec=0,
        max_retries=0,
        daily_limit=0,
    )
    with pytest.raises(ValueError):
        s.validate()

