from financial_data_collector.preflight import run_preflight


def test_preflight_fails_with_blank_env(tmp_path):
    p = tmp_path / ".env"
    p.write_text("KRX_AUTH_KEY=\n", encoding="utf-8")
    result = run_preflight(p.as_posix())
    assert result.ok is False
    assert "Missing/invalid env" in result.errors[0]

