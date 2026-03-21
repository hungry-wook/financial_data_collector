from datetime import date

from financial_data_collector.collect_krx_data import (
    _extract_rows,
    _instrument_uuid,
    _normalize_daily_market,
    _normalize_instrument_code,
    _normalize_instruments,
)


def test_extract_rows_with_outblock1():
    payload = {"OutBlock_1": [{"ISU_CD": "A001", "NAME": "Test"}]}
    rows = _extract_rows(payload)
    assert len(rows) == 1
    assert rows[0]["ISU_CD"] == "A001"


def test_normalize_instrument_code_preserves_string_format():
    assert _normalize_instrument_code("005930") == "005930"
    assert _normalize_instrument_code("A005930") == "005930"
    assert _normalize_instrument_code(5930.0) == "005930"


def test_normalize_instruments_uses_6_digit_string_code():
    rows = [{"ISU_SRT_CD": 5930.0, "ISU_NM": "삼성전자", "LIST_DD": "20100101"}]
    normalized = _normalize_instruments(rows, "KOSPI")
    assert normalized[0]["instrument_id"] == _instrument_uuid("KOSPI", "005930")
    assert normalized[0]["external_code"] == "005930"


def test_normalize_daily_market_uses_base_price_payload():
    rows = [{
        "ISU_SRT_CD": "000001",
        "TDD_OPNPRC": "100",
        "TDD_HGPRC": "110",
        "TDD_LWPRC": "95",
        "TDD_CLSPRC": "105",
        "ACC_TRDVOL": "1000",
    }]
    base_rows = [{"ISU_SRT_CD": "000001", "TDD_STPRC": "98"}]
    normalized = _normalize_daily_market(rows, market_code="KOSDAQ", trade_date=date(2026, 1, 2), base_price_rows=base_rows)
    assert len(normalized) == 1
    assert normalized[0]["base_price"] == 98.0
    assert normalized[0]["instrument_id"] == _instrument_uuid("KOSDAQ", "000001")


def test_normalize_daily_market_trade_halted_ohlc_corrected():
    rows = [{
        "ISU_SRT_CD": "000001",
        "TDD_OPNPRC": "0",
        "TDD_HGPRC": "0",
        "TDD_LWPRC": "0",
        "TDD_CLSPRC": "12345",
        "ACC_TRDVOL": "0",
    }]
    normalized = _normalize_daily_market(rows, market_code="KOSDAQ", trade_date=date(2026, 1, 2))
    assert normalized[0]["is_trade_halted"] is True
    assert normalized[0]["open"] == 12345.0
    assert normalized[0]["high"] == 12345.0
    assert normalized[0]["low"] == 12345.0