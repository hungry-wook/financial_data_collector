from datetime import date

from financial_data_collector.adjustment_service import AdjustmentService
from financial_data_collector.collectors import DailyMarketCollector, InstrumentCollector


def test_adjustment_service_builds_cumulative_factor_from_base_price(repo):
    InstrumentCollector(repo).collect(
        [{
            "instrument_id": "i_adj_1",
            "external_code": "123456",
            "market_code": "KOSDAQ",
            "instrument_name": "Adj Sample",
            "listing_date": date(2020, 1, 1),
        }],
        "krx",
    )
    instrument_id = repo.get_instrument_id_by_external_code("123456", market_code="KOSDAQ")
    DailyMarketCollector(repo).collect(
        [
            {"instrument_id": instrument_id, "trade_date": date(2026, 1, 2), "open": 100, "high": 110, "low": 90, "close": 100, "volume": 10, "listed_shares": 100, "base_price": 100},
            {"instrument_id": instrument_id, "trade_date": date(2026, 1, 3), "open": 50, "high": 55, "low": 45, "close": 50, "volume": 10, "listed_shares": 200, "base_price": 50},
            {"instrument_id": instrument_id, "trade_date": date(2026, 1, 6), "open": 55, "high": 60, "low": 50, "close": 55, "volume": 10, "listed_shares": 200, "base_price": 55},
        ],
        "krx",
        "r1",
    )
    out = AdjustmentService(repo).rebuild_factors("2026-01-01", "2026-01-10")
    assert out["factors"] == 3
    rows = repo.query("SELECT trade_date, factor, cumulative_factor FROM price_adjustment_factors WHERE instrument_id = %s ORDER BY trade_date", (instrument_id,))
    assert rows[0]["trade_date"] == "2026-01-02"
    assert rows[0]["cumulative_factor"] == 0.55
    assert rows[1]["factor"] == 0.5
    assert rows[1]["cumulative_factor"] == 1.1
    assert rows[2]["factor"] == 1.1
    assert rows[2]["cumulative_factor"] == 1.0


def test_adjustment_service_defaults_to_one_without_base_price(repo):
    InstrumentCollector(repo).collect(
        [{
            "instrument_id": "i_adj_2",
            "external_code": "654321",
            "market_code": "KOSDAQ",
            "instrument_name": "Adj Missing",
            "listing_date": date(2020, 1, 1),
        }],
        "krx",
    )
    instrument_id = repo.get_instrument_id_by_external_code("654321", market_code="KOSDAQ")
    DailyMarketCollector(repo).collect(
        [
            {"instrument_id": instrument_id, "trade_date": date(2026, 1, 2), "open": 100, "high": 110, "low": 90, "close": 100, "volume": 10},
            {"instrument_id": instrument_id, "trade_date": date(2026, 1, 3), "open": 101, "high": 111, "low": 91, "close": 101, "volume": 10},
        ],
        "krx",
        "r1",
    )
    AdjustmentService(repo).rebuild_factors("2026-01-01", "2026-01-10")
    rows = repo.query("SELECT trade_date, factor, cumulative_factor FROM price_adjustment_factors WHERE instrument_id = %s ORDER BY trade_date", (instrument_id,))
    assert rows[0]["factor"] == 1.0
    assert rows[0]["cumulative_factor"] == 1.0
    assert rows[1]["factor"] == 1.0


def test_adjustment_service_compute_impacted_window():
    out = AdjustmentService.compute_impacted_window("2026-01-10", "2026-01-20", overlap_days=7)
    assert out == {"date_from": "2026-01-03", "date_to": "2026-01-20"}