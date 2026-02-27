import os

import pytest

from financial_data_collector.settings import KRXSettings, load_dotenv


def test_load_dotenv(tmp_path):
    f = tmp_path / ".env"
    f.write_text(
        "\n".join(
            [
                "KRX_AUTH_KEY=abc",
                "KRX_DAILY_LIMIT=123",
            ]
        ),
        encoding="utf-8",
    )
    loaded = load_dotenv(f.as_posix())
    assert loaded["KRX_AUTH_KEY"] == "abc"
    assert os.environ.get("KRX_DAILY_LIMIT") == "123"


def test_settings_validate_missing():
    s = KRXSettings(
        auth_key="",
        daily_limit=0,
    )
    with pytest.raises(ValueError):
        s.validate()


def test_from_env_requires_krx_auth_key(monkeypatch):
    monkeypatch.delenv("KRX_AUTH_KEY", raising=False)
    monkeypatch.setenv("KRX_OPENAPI_KEY", "legacy-key")
    monkeypatch.setenv("KRX_DAILY_LIMIT", "100")

    s = KRXSettings.from_env()

    assert s.auth_key == ""
    with pytest.raises(ValueError, match="KRX_AUTH_KEY"):
        s.validate()
