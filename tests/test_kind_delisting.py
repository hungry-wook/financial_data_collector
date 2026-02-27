from datetime import date, datetime, timezone

from financial_data_collector.collect_kind_delistings import run_kind_delisting_collection
from financial_data_collector.kind_client import map_market_type_to_code, parse_delisting_excel


def _seed_instrument(repo, instrument_id: str, market_code: str, external_code: str, listing_date: str, delisting_date=None):
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    repo.upsert_instruments(
        [
            {
                "instrument_id": instrument_id,
                "external_code": external_code,
                "market_code": market_code,
                "instrument_name": f"{external_code}-name",
                "instrument_name_abbr": None,
                "instrument_name_eng": None,
                "listing_date": listing_date,
                "delisting_date": delisting_date,
                "listed_shares": None,
                "security_group": None,
                "sector_name": None,
                "stock_type": None,
                "par_value": None,
                "source_name": "krx",
                "collected_at": now,
                "updated_at": now,
            }
        ]
    )


def test_kind_market_mapping():
    assert map_market_type_to_code("1") == "KOSPI"
    assert map_market_type_to_code("2") == "KOSDAQ"
    assert map_market_type_to_code("6") == "KONEX"


def test_parse_delisting_excel_extracts_expected_fields():
    html = """
    <html><body><table border='1'>
      <tr>
        <th>번호</th><th>회사명</th><th>종목코드</th><th>폐지일자</th><th>폐지사유</th><th>비고</th>
      </tr>
      <tr>
        <td>1</td><td>테스트</td><td>12345</td><td>2026-01-10</td><td>사유A</td><td>비고A</td>
      </tr>
      <tr>
        <td>2</td><td>무시</td><td>ABC</td><td>bad-date</td><td>사유B</td><td></td>
      </tr>
    </table></body></html>
    """
    rows = parse_delisting_excel(
        content=html.encode("euc-kr"),
        market_code="KOSDAQ",
        source_name="kind",
        collected_at="2026-01-02T00:00:00Z",
    )
    assert len(rows) == 1
    assert rows[0]["market_code"] == "KOSDAQ"
    assert rows[0]["external_code"] == "012345"
    assert rows[0]["delisting_date"] == "2026-01-10"
    assert rows[0]["delisting_reason"] == "사유A"
    assert rows[0]["note"] == "비고A"


def test_bulk_update_delisting_dates_overwrites_existing(repo):
    _seed_instrument(repo, "550e8400-e29b-41d4-a716-446655440001", "KOSDAQ", "000001", "2020-01-01", "2025-01-01")
    result = repo.bulk_update_delisting_dates(
        [
            {
                "market_code": "KOSDAQ",
                "external_code": "000001",
                "delisting_date": "2026-01-15",
            }
        ],
        source_name="kind",
        run_id=None,
    )
    row = repo.query("SELECT delisting_date, source_name FROM instruments WHERE market_code='KOSDAQ' AND external_code='000001'")[0]
    assert row["delisting_date"] == "2026-01-15"
    assert row["source_name"] == "kind"
    assert result["updated"] == 1
    assert result["invalid"] == 0


def test_bulk_update_delisting_dates_unmatched_does_not_record_issue(repo):
    result = repo.bulk_update_delisting_dates(
        [
            {
                "market_code": "KOSDAQ",
                "external_code": "999999",
                "delisting_date": "2026-01-15",
            }
        ],
        source_name="kind",
        run_id=None,
    )
    assert result["unmatched"] == 1
    issues = repo.query(
        """
        SELECT issue_code
        FROM data_quality_issues
        WHERE issue_code='DELISTING_INSTRUMENT_UNMATCHED'
        """
    )
    assert issues == []


def test_bulk_update_delisting_dates_mixed_match_and_unmatched(repo):
    _seed_instrument(repo, "550e8400-e29b-41d4-a716-446655440021", "KOSDAQ", "000021", "2010-01-01")
    result = repo.bulk_update_delisting_dates(
        [
            {
                "market_code": "KOSDAQ",
                "external_code": "000021",
                "delisting_date": "2026-01-15",
            },
            {
                "market_code": "KOSDAQ",
                "external_code": "999999",
                "delisting_date": "2026-01-16",
            },
        ],
        source_name="kind",
        run_id=None,
    )
    assert result["updated"] == 1
    assert result["unmatched"] == 1
    issues = repo.query(
        """
        SELECT issue_code
        FROM data_quality_issues
        WHERE issue_code='DELISTING_INSTRUMENT_UNMATCHED'
        """
    )
    assert issues == []


def test_run_kind_delisting_collection_updates_by_market_type(repo):
    _seed_instrument(repo, "550e8400-e29b-41d4-a716-446655440011", "KOSPI", "005930", "2000-01-01", None)
    _seed_instrument(repo, "550e8400-e29b-41d4-a716-446655440012", "KOSDAQ", "060000", "2005-01-01", None)
    _seed_instrument(repo, "550e8400-e29b-41d4-a716-446655440013", "KONEX", "299999", "2015-01-01", None)

    class FakeClient:
        def fetch_delistings(self, market_type, date_from, date_to, source_name="kind"):
            if market_type == "1":
                return [
                    {
                        "market_code": "KOSPI",
                        "external_code": "005930",
                        "delisting_date": "2026-01-31",
                        "delisting_reason": "사유-코스피",
                        "note": "비고-코스피",
                    }
                ]
            if market_type == "2":
                return [
                    {
                        "market_code": "KOSDAQ",
                        "external_code": "060000",
                        "delisting_date": "2026-01-30",
                        "delisting_reason": "사유-코스닥",
                        "note": "비고-코스닥",
                    }
                ]
            if market_type == "6":
                return [
                    {
                        "market_code": "KONEX",
                        "external_code": "299999",
                        "delisting_date": "2026-01-29",
                        "delisting_reason": "사유-코넥스",
                        "note": "비고-코넥스",
                    }
                ]
            return []

    result = run_kind_delisting_collection(
        database_url=repo.database_url,
        date_from=date(2026, 1, 1),
        date_to=date(2026, 2, 1),
        source_name="kind",
        client=FakeClient(),
        schema=repo.schema,
    )

    assert result["result"]["updated"] == 3
    assert result["snapshot"]["upserted"] == 3
    rows = repo.query(
        """
        SELECT market_code, external_code, delisting_date
        FROM instruments
        WHERE external_code IN ('005930','060000','299999')
        ORDER BY market_code
        """
    )
    assert rows[0]["delisting_date"] == "2026-01-29"
    assert rows[1]["delisting_date"] == "2026-01-30"
    assert rows[2]["delisting_date"] == "2026-01-31"

    snapshots = repo.query(
        """
        SELECT market_code, external_code, delisting_date, delisting_reason, note
        FROM instrument_delisting_snapshot
        ORDER BY market_code
        """
    )
    assert snapshots[0]["delisting_reason"] == "사유-코넥스"
    assert snapshots[1]["delisting_reason"] == "사유-코스닥"
    assert snapshots[2]["delisting_reason"] == "사유-코스피"
