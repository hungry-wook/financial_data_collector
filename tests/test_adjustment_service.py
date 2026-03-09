from datetime import date

from financial_data_collector.adjustment_service import AdjustmentService
from financial_data_collector.collectors import DailyMarketCollector, InstrumentCollector


def test_adjustment_service_builds_cumulative_factor(repo):
    InstrumentCollector(repo).collect(
        [
            {
                "instrument_id": "i_adj_1",
                "external_code": "123456",
                "market_code": "KOSDAQ",
                "instrument_name": "Adj Sample",
                "listing_date": date(2020, 1, 1),
            }
        ],
        "krx",
    )
    instrument_id = repo.get_instrument_id_by_external_code("123456", market_code="KOSDAQ")
    assert instrument_id

    DailyMarketCollector(repo).collect(
        [
            {"instrument_id": instrument_id, "trade_date": date(2026, 1, 2), "open": 100, "high": 110, "low": 90, "close": 100, "volume": 10},
            {"instrument_id": instrument_id, "trade_date": date(2026, 1, 3), "open": 50, "high": 55, "low": 45, "close": 50, "volume": 10},
            {"instrument_id": instrument_id, "trade_date": date(2026, 1, 6), "open": 55, "high": 60, "low": 50, "close": 55, "volume": 10},
        ],
        "krx",
        "r1",
    )

    repo.upsert_corporate_events(
        [
            {
                "event_id": "evt1",
                "event_version": 1,
                "instrument_id": instrument_id,
                "event_type": "BONUS_ISSUE",
                "announce_date": "2026-01-03",
                "effective_date": "2026-01-03",
                "source_event_id": "20260103000001",
                "source_name": "opendart",
                "collected_at": "2026-01-03T00:00:00Z",
                "raw_factor": 0.5,
                "confidence": "HIGH",
                "status": "ACTIVE",
                "payload": {"ratio": 0.5},
            }
        ]
    )

    out = AdjustmentService(repo).rebuild_factors("2026-01-01", "2026-01-10")
    assert out["factors"] == 3
    assert out["instrument_count"] == 1
    assert out["event_date_count"] == 1

    rows = repo.query(
        """
        SELECT trade_date, cumulative_factor
        FROM price_adjustment_factors
        WHERE instrument_id = %s AND as_of_date = DATE '9999-12-31'
        ORDER BY trade_date
        """,
        (instrument_id,),
    )
    assert [r["trade_date"] for r in rows] == ["2026-01-02", "2026-01-03", "2026-01-06"]
    # pre-event day should carry adjustment factor
    assert rows[0]["cumulative_factor"] == 0.5
    # event day and after stay at latest basis
    assert rows[1]["cumulative_factor"] == 1.0
    assert rows[2]["cumulative_factor"] == 1.0



def test_adjustment_service_compute_impacted_window():
    out = AdjustmentService.compute_impacted_window("2026-01-10", "2026-01-20", overlap_days=7)
    assert out == {"date_from": "2026-01-03", "date_to": "2026-01-20"}
