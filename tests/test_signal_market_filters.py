from datetime import date, datetime, timezone


def _now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _seed_signal_filter_data(repo):
    now = _now_iso()
    repo.upsert_instruments(
        [
            {
                "instrument_id": "00000000-0000-0000-0000-000000000001",
                "external_code": "A001",
                "market_code": "KOSDAQ",
                "instrument_name": "VALID_ROW",
                "listing_date": date(2020, 1, 1),
                "source_name": "test",
                "collected_at": now,
            },
            {
                "instrument_id": "00000000-0000-0000-0000-000000000002",
                "external_code": "A002",
                "market_code": "KOSDAQ",
                "instrument_name": "INVALID_STATUS",
                "listing_date": date(2020, 1, 1),
                "source_name": "test",
                "collected_at": now,
            },
            {
                "instrument_id": "00000000-0000-0000-0000-000000000003",
                "external_code": "A003",
                "market_code": "KOSDAQ",
                "instrument_name": "HALTED",
                "listing_date": date(2020, 1, 1),
                "source_name": "test",
                "collected_at": now,
            },
            {
                "instrument_id": "00000000-0000-0000-0000-000000000004",
                "external_code": "A004",
                "market_code": "KOSDAQ",
                "instrument_name": "SUPERVISION",
                "listing_date": date(2020, 1, 1),
                "source_name": "test",
                "collected_at": now,
            },
            {
                "instrument_id": "00000000-0000-0000-0000-000000000005",
                "external_code": "A005",
                "market_code": "KOSDAQ",
                "instrument_name": "ZERO_VOLUME",
                "listing_date": date(2020, 1, 1),
                "source_name": "test",
                "collected_at": now,
            },
        ]
    )
    repo.upsert_daily_market(
        [
            {
                "instrument_id": "00000000-0000-0000-0000-000000000001",
                "trade_date": date(2026, 1, 2),
                "open": 10,
                "high": 11,
                "low": 9,
                "close": 10.5,
                "volume": 100,
                "source_name": "test",
                "collected_at": now,
                "record_status": "VALID",
                "is_trade_halted": False,
                "is_under_supervision": False,
            },
            {
                "instrument_id": "00000000-0000-0000-0000-000000000002",
                "trade_date": date(2026, 1, 2),
                "open": 10,
                "high": 11,
                "low": 9,
                "close": 10.5,
                "volume": 100,
                "source_name": "test",
                "collected_at": now,
                "record_status": "INVALID",
                "is_trade_halted": False,
                "is_under_supervision": False,
            },
            {
                "instrument_id": "00000000-0000-0000-0000-000000000003",
                "trade_date": date(2026, 1, 2),
                "open": 10,
                "high": 11,
                "low": 9,
                "close": 10.5,
                "volume": 100,
                "source_name": "test",
                "collected_at": now,
                "record_status": "VALID",
                "is_trade_halted": True,
                "is_under_supervision": False,
            },
            {
                "instrument_id": "00000000-0000-0000-0000-000000000004",
                "trade_date": date(2026, 1, 2),
                "open": 10,
                "high": 11,
                "low": 9,
                "close": 10.5,
                "volume": 100,
                "source_name": "test",
                "collected_at": now,
                "record_status": "VALID",
                "is_trade_halted": False,
                "is_under_supervision": True,
            },
            {
                "instrument_id": "00000000-0000-0000-0000-000000000005",
                "trade_date": date(2026, 1, 2),
                "open": 10,
                "high": 11,
                "low": 9,
                "close": 10.5,
                "volume": 0,
                "source_name": "test",
                "collected_at": now,
                "record_status": "VALID",
                "is_trade_halted": False,
                "is_under_supervision": False,
            },
        ]
    )


def test_get_signal_market_applies_default_hard_filters(repo):
    _seed_signal_filter_data(repo)

    rows = repo.get_signal_market(["KOSDAQ"], "2026-01-01", "2026-01-03")
    codes = sorted(r["external_code"] for r in rows)

    assert codes == ["A001", "A005"]


def test_get_signal_market_can_require_positive_volume(repo):
    _seed_signal_filter_data(repo)

    rows = repo.get_signal_market(["KOSDAQ"], "2026-01-01", "2026-01-03", require_positive_volume=True)
    codes = sorted(r["external_code"] for r in rows)

    assert codes == ["A001"]
