from datetime import date

import pytest
from financial_data_collector.collect_krx_data import (
    _extract_rows,
    _normalize_daily_market,
    _normalize_instrument_code,
    _normalize_instruments,
)
from financial_data_collector.collectors import DailyMarketCollector, InstrumentCollector


def test_extract_rows_with_outblock1():
    """Test extraction with OutBlock_1 structure (KRX API format)"""
    payload = {"OutBlock_1": [{"ISU_CD": "A001", "NAME": "Test"}]}
    rows = _extract_rows(payload)
    assert len(rows) == 1
    assert rows[0]["ISU_CD"] == "A001"


def test_extract_rows_with_outblock2():
    """Test extraction with OutBlock_2 structure"""
    payload = {"OutBlock_2": [{"ISU_CD": "A001"}]}
    rows = _extract_rows(payload)
    assert len(rows) == 1


def test_extract_rows_with_standard_keys():
    """Test extraction with standard keys (output, result, data)"""
    assert len(_extract_rows({"output": [{"key": "val"}]})) == 1
    assert len(_extract_rows({"result": [{"key": "val"}]})) == 1
    assert len(_extract_rows({"data": [{"key": "val"}]})) == 1


def test_extract_rows_with_nested_structure():
    """Test extraction with nested structures"""
    payload = {"result": {"items": [{"key": "val"}]}}
    rows = _extract_rows(payload)
    assert len(rows) == 1


def test_extract_rows_with_unknown_key_fallback():
    """Test fallback mechanism for unknown keys"""
    payload = {"CustomBlock": [{"key": "val"}]}
    rows = _extract_rows(payload)
    assert len(rows) == 1


def test_extract_rows_with_none():
    """Test with None payload"""
    assert _extract_rows(None) == []


def test_extract_rows_with_empty_list():
    """Test with empty list"""
    assert _extract_rows([]) == []


def test_extract_rows_with_empty_dict():
    """Test with empty dict"""
    assert _extract_rows({}) == []


def test_extract_rows_with_multiple_rows():
    """Test extraction with multiple rows"""
    payload = {"OutBlock_1": [{"id": 1}, {"id": 2}, {"id": 3}]}
    rows = _extract_rows(payload)
    assert len(rows) == 3


def test_extract_rows_filters_non_dict_items():
    """Test that non-dict items are filtered out"""
    payload = {"OutBlock_1": [{"id": 1}, "string", 123, {"id": 2}]}
    rows = _extract_rows(payload)
    assert len(rows) == 2
    assert all(isinstance(r, dict) for r in rows)


def test_normalize_daily_market_trade_halted_ohlc_corrected():
    rows = [
        {
            "ISU_SRT_CD": "000001",
            "TDD_OPNPRC": "0",
            "TDD_HGPRC": "0",
            "TDD_LWPRC": "0",
            "TDD_CLSPRC": "12345",
            "ACC_TRDVOL": "0",
        }
    ]

    normalized = _normalize_daily_market(rows, trade_date=date(2026, 1, 2))

    assert len(normalized) == 1
    r = normalized[0]
    assert r["is_trade_halted"] is True
    assert r["open"] == 12345.0
    assert r["high"] == 12345.0
    assert r["low"] == 12345.0
    assert r["close"] == 12345.0
    assert r["volume"] == 0


def test_normalize_daily_market_non_halted_not_misclassified():
    rows = [
        {
            "ISU_SRT_CD": "000002",
            "TDD_OPNPRC": "100",
            "TDD_HGPRC": "110",
            "TDD_LWPRC": "95",
            "TDD_CLSPRC": "105",
            "ACC_TRDVOL": "1000",
        }
    ]

    normalized = _normalize_daily_market(rows, trade_date=date(2026, 1, 2))

    assert len(normalized) == 1
    r = normalized[0]
    assert r["is_trade_halted"] is False
    assert r["open"] == 100.0
    assert r["high"] == 110.0
    assert r["low"] == 95.0
    assert r["close"] == 105.0


def test_normalize_daily_market_all_zero_edge_case_not_halted():
    rows = [
        {
            "ISU_SRT_CD": "000003",
            "TDD_OPNPRC": "0",
            "TDD_HGPRC": "0",
            "TDD_LWPRC": "0",
            "TDD_CLSPRC": "0",
            "ACC_TRDVOL": "0",
        }
    ]

    normalized = _normalize_daily_market(rows, trade_date=date(2026, 1, 2))

    assert len(normalized) == 1
    r = normalized[0]
    assert r["is_trade_halted"] is False
    assert r["open"] == 0.0
    assert r["high"] == 0.0
    assert r["low"] == 0.0
    assert r["close"] == 0.0


def test_daily_market_collector_accepts_corrected_trade_halted_row(repo):
    InstrumentCollector(repo).collect(
        [
            {
                "instrument_id": "000001",
                "external_code": "000001",
                "market_code": "kosdaq",
                "instrument_name": "halted",
                "listing_date": "2020-01-01",
            }
        ],
        "krx",
    )

    rows = _normalize_daily_market(
        [
            {
                "ISU_SRT_CD": "000001",
                "TDD_OPNPRC": "0",
                "TDD_HGPRC": "0",
                "TDD_LWPRC": "0",
                "TDD_CLSPRC": "12345",
                "ACC_TRDVOL": "0",
            }
        ],
        trade_date=date(2026, 1, 2),
    )

    count = DailyMarketCollector(repo).collect(rows, "krx", "r1")
    assert count == 1

    saved = repo.query("SELECT open, high, low, close, volume, is_trade_halted FROM daily_market_data WHERE instrument_id='000001'")[0]
    assert saved["open"] == 12345.0
    assert saved["high"] == 12345.0
    assert saved["low"] == 12345.0
    assert saved["close"] == 12345.0
    assert saved["volume"] == 0
    assert saved["is_trade_halted"] == 1

    issues = repo.query(
        "SELECT issue_code FROM data_quality_issues WHERE instrument_id='000001' AND issue_code='TRADE_HALTED_OHLC_CORRECTED'"
    )
    assert len(issues) == 0


@pytest.mark.parametrize(
    ("raw_code", "expected"),
    [
        ("005930", "005930"),
        ("A005930", "005930"),
        ("5930", "005930"),
        (5930, "005930"),
        (5930.0, "005930"),
        ("0099X0", "0099X0"),
    ],
)
def test_normalize_instrument_code_preserves_string_format(raw_code, expected):
    assert _normalize_instrument_code(raw_code) == expected


def test_normalize_instruments_uses_6_digit_string_code():
    rows = [
        {
            "ISU_SRT_CD": 5930.0,
            "ISU_CD": "KR7005930003",
            "ISU_NM": "삼성전자",
            "LIST_DD": "20100101",
        }
    ]

    normalized = _normalize_instruments(rows, "KOSPI")
    assert len(normalized) == 1
    assert normalized[0]["instrument_id"] == "005930"
    assert normalized[0]["external_code"] == "005930"


def test_normalize_daily_market_accepts_isu_cd_prefixed_code():
    rows = [
        {
            "ISU_CD": "A005930",
            "TDD_OPNPRC": "100",
            "TDD_HGPRC": "110",
            "TDD_LWPRC": "95",
            "TDD_CLSPRC": "105",
            "ACC_TRDVOL": "1000",
        }
    ]
    normalized = _normalize_daily_market(rows, trade_date=date(2026, 1, 2))
    assert len(normalized) == 1
    assert normalized[0]["instrument_id"] == "005930"
