from datetime import date

from financial_data_collector.collect_dart_corporate_events import _derive_adjustment_apply_date, _derive_legal_effective_date, collect_corporate_events, collect_corporate_events_and_rebuild_factors, repair_corporate_event_timings, run_dart_corporate_event_collection
from financial_data_collector.collectors import InstrumentCollector

BONUS_REPORT = "\uC8FC\uC694\uC0AC\uD56D\uBCF4\uACE0\uC11C(\uBB34\uC0C1\uC99D\uC790\uACB0\uC815)"
MERGER_REPORT = "\uC8FC\uC694\uC0AC\uD56D\uBCF4\uACE0\uC11C(\uD68C\uC0AC\uD569\uBCD1\uACB0\uC815)"
RIGHTS_REPORT = "\uC8FC\uC694\uC0AC\uD56D\uBCF4\uACE0\uC11C(\uC720\uC0C1\uC99D\uC790\uACB0\uC815)"
RIGHTS_ATTACH_REPORT = "[\uCCA8\uBD80\uC815\uC815]\uC8FC\uC694\uC0AC\uD56D\uBCF4\uACE0\uC11C(\uC720\uC0C1\uC99D\uC790\uACB0\uC815)"
RIGHTS_BONUS_REPORT = "\uC8FC\uC694\uC0AC\uD56D\uBCF4\uACE0\uC11C(\uC720\uBB34\uC0C1\uC99D\uC790\uACB0\uC815)"
KORP = "\uC0D8\uD50C"


class _FakeClient:
    def __init__(self):
        self.calls = []

    def list_filings(self, **kwargs):
        self.calls.append(("list", kwargs))
        return {
            "status": "000",
            "total_count": 1,
            "list": [
                {
                    "corp_code": "x",
                    "corp_name": KORP,
                    "stock_code": "123456",
                    "corp_cls": "K",
                    "report_nm": BONUS_REPORT,
                    "rcept_no": "20260308000001",
                    "rcept_dt": "20260308",
                }
            ],
        }

    def get_document_zip(self, rcept_no):
        self.calls.append(("doc", {"rcept_no": rcept_no}))
        import io
        import zipfile

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("a.html", "<html><body>\uBB34\uC0C1\uC99D\uC790 1\uC8FC\uB2F9 0.5\uC8FC \uBC30\uC815 \uC2E0\uC8FC \uC0C1\uC7A5 \uC608\uC815\uC77C 2026\uB144 03\uC6D4 08\uC77C</body></html>")
        return buf.getvalue()


class _FakeStructuralClient(_FakeClient):
    def list_filings(self, **kwargs):
        self.calls.append(("list", kwargs))
        return {
            "status": "000",
            "total_count": 1,
            "list": [
                {
                    "corp_code": "x",
                    "corp_name": KORP,
                    "stock_code": "123456",
                    "corp_cls": "K",
                    "report_nm": MERGER_REPORT,
                    "rcept_no": "20260308000002",
                    "rcept_dt": "20260308",
                }
            ],
        }

    def get_document_zip(self, rcept_no):
        self.calls.append(("doc", {"rcept_no": rcept_no}))
        import io
        import zipfile

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("a.html", "<html><body>\uD569\uBCD1 \uAD00\uB828 \uC77C\uBC18 \uACF5\uC2DC \uBB38\uAD6C\uB9CC \uC788\uACE0 \uAD50\uD658\uBE44\uC728\uC740 \uC5C6\uC74C</body></html>")
        return buf.getvalue()


class _FakeWithdrawnClient(_FakeClient):
    def list_filings(self, **kwargs):
        self.calls.append(("list", kwargs))
        return {
            "status": "000",
            "total_count": 1,
            "list": [
                {
                    "corp_code": "x",
                    "corp_name": KORP,
                    "stock_code": "123456",
                    "corp_cls": "K",
                    "report_nm": RIGHTS_REPORT,
                    "rcept_no": "20260308000003",
                    "rcept_dt": "20260308",
                }
            ],
        }

    def get_document_zip(self, rcept_no):
        self.calls.append(("doc", {"rcept_no": rcept_no}))
        import io
        import zipfile

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("a.html", "<html><body>[\uAE30\uC7AC\uC815\uC815] \uC8FC\uC694\uC0AC\uD56D\uBCF4\uACE0\uC11C(\uC720\uC0C1\uC99D\uC790\uACB0\uC815) \uC720\uC0C1\uC99D\uC790 \uACB0\uC815 \uCCA0\uD68C</body></html>")
        return buf.getvalue()


class _FakeRemoteChainClient(_FakeClient):
    def list_filings(self, **kwargs):
        self.calls.append(("list", kwargs))
        corp_code = kwargs.get("corp_code")
        if corp_code == "x":
            return {
                "status": "000",
                "total_count": 2,
                "list": [
                    {
                        "corp_code": "x",
                        "corp_name": KORP,
                        "stock_code": "123456",
                        "corp_cls": "K",
                        "report_nm": RIGHTS_ATTACH_REPORT,
                        "rcept_no": "20260308000004",
                        "rcept_dt": "20260308",
                    },
                    {
                        "corp_code": "x",
                        "corp_name": KORP,
                        "stock_code": "123456",
                        "corp_cls": "K",
                        "report_nm": RIGHTS_REPORT,
                        "rcept_no": "20260201000001",
                        "rcept_dt": "20260201",
                    },
                ],
            }
        return {
            "status": "000",
            "total_count": 1,
            "list": [
                {
                    "corp_code": "x",
                    "corp_name": KORP,
                    "stock_code": "123456",
                    "corp_cls": "K",
                    "report_nm": RIGHTS_ATTACH_REPORT,
                    "rcept_no": "20260308000004",
                    "rcept_dt": "20260308",
                }
            ],
        }

    def get_document_zip(self, rcept_no):
        self.calls.append(("doc", {"rcept_no": rcept_no}))
        if rcept_no == "20260308000004":
            raise RuntimeError("OpenDART document error status=014: \uD30C\uC77C\uC774 \uC874\uC7AC\uD558\uC9C0 \uC54A\uC2B5\uB2C8\uB2E4.")
        import io
        import zipfile

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(
                "a.html",
                "<html><body>\uC720\uC0C1\uC99D\uC790 \uACB0\uC815 1. \uC2E0\uC8FC\uC758 \uC885\uB958\uC640 \uC218 \uBCF4\uD1B5\uC8FC\uC2DD (\uC8FC) 100 2. 1\uC8FC\uB2F9 \uC561\uBA74\uAC00\uC561 (\uC6D0) 500 3. \uC99D\uC790\uC804 \uBC1C\uD589\uC8FC\uC2DD\uCD1D\uC218 (\uC8FC) \uBCF4\uD1B5\uC8FC\uC2DD (\uC8FC) 900 \uC2E0\uC8FC \uC0C1\uC7A5 \uC608\uC815\uC77C 2026\uB144 03\uC6D4 08\uC77C</body></html>",
            )
        return buf.getvalue()


class _FakeThirdPartyRightsClient(_FakeClient):
    def list_filings(self, **kwargs):
        self.calls.append(("list", kwargs))
        return {
            "status": "000",
            "total_count": 1,
            "list": [
                {
                    "corp_code": "x",
                    "corp_name": KORP,
                    "stock_code": "123456",
                    "corp_cls": "K",
                    "report_nm": RIGHTS_REPORT,
                    "rcept_no": "20260308000006",
                    "rcept_dt": "20260308",
                }
            ],
        }

    def get_rights_issue_disclosures(self, **kwargs):
        self.calls.append(("ds005", kwargs))
        return {
            "status": "000",
            "list": [
                {
                    "rcept_no": "20260308000006",
                    "ic_mthn": "\uC81C3\uC790\uBC30\uC815\uC99D\uC790",
                    "bfic_tisstk_ostk": "900",
                    "nstk_ostk_cnt": "100",
                }
            ],
        }


class _FakePublicRightsClient(_FakeClient):
    def list_filings(self, **kwargs):
        self.calls.append(("list", kwargs))
        return {
            "status": "000",
            "total_count": 1,
            "list": [
                {
                    "corp_code": "x",
                    "corp_name": KORP,
                    "stock_code": "123456",
                    "corp_cls": "K",
                    "report_nm": RIGHTS_REPORT,
                    "rcept_no": "20260308000008",
                    "rcept_dt": "20260308",
                }
            ],
        }

    def get_rights_issue_disclosures(self, **kwargs):
        self.calls.append(("ds005", kwargs))
        return {
            "status": "000",
            "list": [
                {
                    "rcept_no": "20260308000008",
                    "ic_mthn": "\uC77C\uBC18\uACF5\uBAA8\uC99D\uC790",
                    "bfic_tisstk_ostk": "900",
                    "nstk_ostk_cnt": "100",
                }
            ],
        }


class _FakeCapitalReductionDs005Client(_FakeClient):
    def list_filings(self, **kwargs):
        self.calls.append(("list", kwargs))
        return {
            "status": "000",
            "total_count": 1,
            "list": [
                {
                    "corp_code": "x",
                    "corp_name": KORP,
                    "stock_code": "123456",
                    "corp_cls": "K",
                    "report_nm": "\uC8FC\uC694\uC0AC\uD56D\uBCF4\uACE0\uC11C(\uAC10\uC790\uACB0\uC815)",
                    "rcept_no": "20260308000007",
                    "rcept_dt": "20260308",
                }
            ],
        }

    def get_capital_reduction_disclosures(self, **kwargs):
        self.calls.append(("ds005", kwargs))
        return {
            "status": "000",
            "list": [
                {
                    "rcept_no": "20260308000007",
                    "bfcr_tisstk_ostk": "1000",
                    "atcr_tisstk_ostk": "200",
                    "crsc_nstklstprd": "2026-04-30",
                }
            ],
        }


class _FakeRevisionWindowClient(_FakeClient):
    def __init__(self, report_nm: str, rcept_no: str, rcept_dt: str):
        super().__init__()
        self.report_nm = report_nm
        self.rcept_no = rcept_no
        self.rcept_dt = rcept_dt

    def list_filings(self, **kwargs):
        self.calls.append(("list", kwargs))
        return {
            "status": "000",
            "total_count": 1,
            "list": [
                {
                    "corp_code": "x",
                    "corp_name": KORP,
                    "stock_code": "123456",
                    "corp_cls": "K",
                    "report_nm": self.report_nm,
                    "rcept_no": self.rcept_no,
                    "rcept_dt": self.rcept_dt,
                }
            ],
        }

    def get_document_zip(self, rcept_no):
        self.calls.append(("doc", {"rcept_no": rcept_no}))
        import io
        import zipfile

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(
                "a.html",
                "<html><body>\uC720\uC0C1\uC99D\uC790 \uACB0\uC815 1. \uC2E0\uC8FC\uC758 \uC885\uB958\uC640 \uC218 \uBCF4\uD1B5\uC8FC\uC2DD (\uC8FC) 100 2. 1\uC8FC\uB2F9 \uC561\uBA74\uAC00\uC561 (\uC6D0) 500 3. \uC99D\uC790\uC804 \uBC1C\uD589\uC8FC\uC2DD\uCD1D\uC218 (\uC8FC) \uBCF4\uD1B5\uC8FC\uC2DD (\uC8FC) 900 \uC2E0\uC8FC \uC0C1\uC7A5 \uC608\uC815\uC77C 2026\uB144 03\uC6D4 08\uC77C</body></html>",
            )
        return buf.getvalue()


class _FakeRightsBonusClient(_FakeClient):
    def list_filings(self, **kwargs):
        self.calls.append(("list", kwargs))
        return {
            "status": "000",
            "total_count": 1,
            "list": [
                {
                    "corp_code": "x",
                    "corp_name": KORP,
                    "stock_code": "123456",
                    "corp_cls": "K",
                    "report_nm": RIGHTS_BONUS_REPORT,
                    "rcept_no": "20260308000005",
                    "rcept_dt": "20260308",
                }
            ],
        }

    def get_document_zip(self, rcept_no):
        self.calls.append(("doc", {"rcept_no": rcept_no}))
        import io
        import zipfile

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(
                "a.html",
                "<html><body>\uC720\uBB34\uC0C1\uC99D\uC790 \uACB0\uC815 1. \uC2E0\uC8FC\uC758 \uC885\uB958\uC640 \uC218 \uBCF4\uD1B5\uC8FC\uC2DD (\uC8FC) 100 2. 1\uC8FC\uB2F9 \uC561\uBA74\uAC00\uC561 (\uC6D0) 500 3. \uC99D\uC790\uC804 \uBC1C\uD589\uC8FC\uC2DD\uCD1D\uC218 (\uC8FC) \uBCF4\uD1B5\uC8FC\uC2DD (\uC8FC) 900</body></html>",
            )
        return buf.getvalue()


def _seed_instrument(repo):
    InstrumentCollector(repo).collect(
        [
            {
                "instrument_id": "id_for_dart",
                "external_code": "123456",
                "market_code": "KOSDAQ",
                "instrument_name": "Sample",
                "listing_date": date(2020, 1, 1),
            }
        ],
        "krx",
    )


def test_collect_corporate_events_sets_active_when_factor_parsed(repo):
    _seed_instrument(repo)

    out = collect_corporate_events(
        repo=repo,
        client=_FakeClient(),
        bgn_de=date(2026, 3, 8),
        end_de=date(2026, 3, 8),
        verify_document=True,
    )

    assert out["events_upserted"] == 1
    assert out["active_events"] == 1

    rows = repo.query("SELECT status, raw_factor FROM corporate_events WHERE source_event_id = %s", ("20260308000001",))
    assert rows[0]["status"] == "ACTIVE"
    assert rows[0]["raw_factor"] > 0


def test_collect_corporate_events_keeps_unresolved_structural_event_in_review(repo):
    _seed_instrument(repo)

    out = collect_corporate_events(
        repo=repo,
        client=_FakeStructuralClient(),
        bgn_de=date(2026, 3, 8),
        end_de=date(2026, 3, 8),
        verify_document=True,
    )

    assert out["events_upserted"] == 1
    assert out["needs_review_events"] == 1

    rows = repo.query("SELECT status, raw_factor FROM corporate_events WHERE source_event_id = %s", ("20260308000002",))
    assert rows[0]["status"] == "NEEDS_REVIEW"
    assert rows[0]["raw_factor"] is None


def test_collect_corporate_events_marks_withdrawn_event_rejected(repo):
    _seed_instrument(repo)

    out = collect_corporate_events(
        repo=repo,
        client=_FakeWithdrawnClient(),
        bgn_de=date(2026, 3, 8),
        end_de=date(2026, 3, 8),
        verify_document=True,
    )

    assert out["events_upserted"] == 1

    rows = repo.query("SELECT status, raw_factor FROM corporate_events WHERE source_event_id = %s", ("20260308000003",))
    assert rows[0]["status"] == "REJECTED"
    assert rows[0]["raw_factor"] is None


def test_collect_corporate_events_recovers_factor_from_remote_revision_chain(repo):
    _seed_instrument(repo)

    out = collect_corporate_events(
        repo=repo,
        client=_FakeRemoteChainClient(),
        bgn_de=date(2026, 3, 8),
        end_de=date(2026, 3, 8),
        verify_document=True,
    )

    assert out["events_upserted"] == 1
    assert out["active_events"] == 1

    rows = repo.query("SELECT status, raw_factor, payload->>'factor_rule' as factor_rule FROM corporate_events WHERE source_event_id = %s", ("20260308000004",))
    assert rows[0]["status"] == "ACTIVE"
    assert rows[0]["raw_factor"] == 0.9
    assert rows[0]["factor_rule"] == "remote_chain_rights_issue_keyword_sections"


def test_collect_corporate_events_deduplicates_revision_chain_across_windows(repo):
    _seed_instrument(repo)

    older = _FakeRevisionWindowClient(
        report_nm=RIGHTS_REPORT,
        rcept_no="20260201000001",
        rcept_dt="20260201",
    )
    newer = _FakeRevisionWindowClient(
        report_nm=RIGHTS_ATTACH_REPORT,
        rcept_no="20260308000004",
        rcept_dt="20260308",
    )

    first = collect_corporate_events(
        repo=repo,
        client=older,
        bgn_de=date(2026, 2, 1),
        end_de=date(2026, 2, 1),
        verify_document=True,
    )
    second = collect_corporate_events(
        repo=repo,
        client=newer,
        bgn_de=date(2026, 3, 8),
        end_de=date(2026, 3, 8),
        verify_document=True,
    )

    assert first["events_upserted"] == 1
    assert second["events_upserted"] == 1
    assert second["revision_chain_deleted"] == 1

    rows = repo.query(
        """
        SELECT source_event_id
        FROM corporate_events
        WHERE source_event_id IN ('20260201000001', '20260308000004')
          AND event_type = 'RIGHTS_ISSUE'
        ORDER BY source_event_id
        """
    )
    assert [row["source_event_id"] for row in rows] == ["20260308000004"]



def test_collect_corporate_events_maps_rights_bonus_before_bonus(repo):
    _seed_instrument(repo)

    out = collect_corporate_events(
        repo=repo,
        client=_FakeRightsBonusClient(),
        bgn_de=date(2026, 3, 8),
        end_de=date(2026, 3, 8),
        verify_document=True,
    )

    assert out["events_upserted"] == 1

    rows = repo.query("SELECT event_type FROM corporate_events WHERE source_event_id = %s", ("20260308000005",))
    assert rows[0]["event_type"] == "RIGHTS_BONUS_ISSUE"



def test_infer_ds005_factor_split_merger_equal_ratio_returns_one():
    from financial_data_collector.collect_dart_corporate_events import _infer_ds005_factor

    row = {
        "dvmg_rt_bs": "??? ?? ??? ????? ???? ???????. ????? 1:1? ???????.",
    }

    assert _infer_ds005_factor("SPLIT_MERGER", row) == 1.0



def test_collect_and_rebuild_adjustments_materializes_factor_window(repo):
    _seed_instrument(repo)
    from financial_data_collector.collectors import DailyMarketCollector

    instrument_id = repo.get_instrument_id_by_external_code("123456", market_code="KOSDAQ")
    DailyMarketCollector(repo).collect(
        [
            {"instrument_id": instrument_id, "trade_date": date(2026, 3, 7), "open": 100, "high": 110, "low": 90, "close": 100, "volume": 10},
            {"instrument_id": instrument_id, "trade_date": date(2026, 3, 8), "open": 50, "high": 55, "low": 45, "close": 50, "volume": 10},
        ],
        "krx",
        "r1",
    )

    out = collect_corporate_events_and_rebuild_factors(
        repo=repo,
        client=_FakeClient(),
        bgn_de=date(2026, 3, 8),
        end_de=date(2026, 3, 8),
        verify_document=True,
        overlap_days=2,
    )

    assert out["collect"]["active_events"] == 1
    assert out["rebuild_status"] == "SUCCEEDED"
    assert out["rebuild_skip_reason"] is None
    assert out["latest_trade_date"] == "2026-03-08"
    assert out["impacted_window"] == {"date_from": "2026-03-06", "date_to": "2026-03-08"}
    rows = repo.query("SELECT trade_date, cumulative_factor FROM price_adjustment_factors WHERE instrument_id = %s ORDER BY trade_date", (instrument_id,))
    assert rows[0]["trade_date"] == "2026-03-07"
    assert rows[0]["cumulative_factor"] > 0


def test_collect_and_rebuild_adjustments_skips_without_trade_dates(repo):
    _seed_instrument(repo)

    out = collect_corporate_events_and_rebuild_factors(
        repo=repo,
        client=_FakeClient(),
        bgn_de=date(2026, 3, 8),
        end_de=date(2026, 3, 8),
        verify_document=True,
        overlap_days=2,
    )

    assert out["collect"]["active_events"] == 1
    assert out["rebuild"] is None
    assert out["impacted_window"] is None
    assert out["rebuild_status"] == "SKIPPED"
    assert out["rebuild_skip_reason"] == "NO_TRADE_DATES"
    assert out["latest_trade_date"] is None



def test_run_dart_corporate_event_collection_persists_run_metadata(repo):
    _seed_instrument(repo)

    out = run_dart_corporate_event_collection(
        database_url=repo.database_url,
        schema=repo.schema,
        client=_FakeClient(),
        bgn_de=date(2026, 3, 8),
        end_de=date(2026, 3, 8),
        verify_document=True,
        rebuild_adjustments=False,
    )

    row = repo.query(
        "SELECT pipeline_name, source_name, status, success_count, warning_count, metadata FROM collection_runs WHERE run_id = %s",
        (out["run_id"],),
    )[0]
    assert row["pipeline_name"] == "collect-dart-corporate-events"
    assert row["source_name"] == "opendart"
    assert row["status"] == "SUCCESS"
    assert row["success_count"] == 1
    assert row["warning_count"] == 0
    assert row["metadata"]["active_events"] == 1


def test_collect_corporate_events_uses_rcept_dt_as_announce_date_only(repo):
    _seed_instrument(repo)

    out = collect_corporate_events(
        repo=repo,
        client=_FakeCapitalReductionDs005Client(),
        bgn_de=date(2026, 3, 8),
        end_de=date(2026, 3, 8),
        verify_document=False,
    )

    assert out["events_upserted"] == 1
    rows = repo.query("SELECT announce_date, effective_date, status, raw_factor FROM corporate_events WHERE source_event_id = %s", ("20260308000007",))
    assert rows[0]["announce_date"] == "2026-03-08"
    assert rows[0]["effective_date"] == "2026-04-30"
    assert rows[0]["status"] == "ACTIVE"
    assert float(rows[0]["raw_factor"]) == 5.0


def test_collect_corporate_events_keeps_third_party_rights_issue_in_review_without_effective_date(repo):
    _seed_instrument(repo)

    out = collect_corporate_events(
        repo=repo,
        client=_FakeThirdPartyRightsClient(),
        bgn_de=date(2026, 3, 8),
        end_de=date(2026, 3, 8),
        verify_document=False,
    )

    assert out["events_upserted"] == 1
    rows = repo.query("SELECT event_type, status, effective_date, announce_date, raw_factor FROM corporate_events WHERE source_event_id = %s", ("20260308000006",))
    assert rows[0]["event_type"] == "RIGHTS_ISSUE_THIRD_PARTY"
    assert rows[0]["announce_date"] == "2026-03-08"
    assert rows[0]["effective_date"] is None
    assert rows[0]["status"] == "NEEDS_REVIEW"
    assert float(rows[0]["raw_factor"]) == 0.9


def test_collect_corporate_events_keeps_public_rights_issue_in_review_without_listing_date(repo):
    _seed_instrument(repo)

    out = collect_corporate_events(
        repo=repo,
        client=_FakePublicRightsClient(),
        bgn_de=date(2026, 3, 8),
        end_de=date(2026, 3, 8),
        verify_document=False,
    )

    assert out["events_upserted"] == 1
    rows = repo.query("SELECT status, effective_date, raw_factor, payload->>'activation_issue' AS activation_issue FROM corporate_events WHERE source_event_id = %s", ("20260308000008",))
    assert rows[0]["status"] == "NEEDS_REVIEW"
    assert rows[0]["effective_date"] is None
    assert float(rows[0]["raw_factor"]) == 0.9
    assert rows[0]["activation_issue"] == "missing_listing_like_date"


def test_repair_corporate_event_timings_moves_capital_reduction_to_listing_date(repo):
    _seed_instrument(repo)

    collect_corporate_events(
        repo=repo,
        client=_FakeCapitalReductionDs005Client(),
        bgn_de=date(2026, 3, 8),
        end_de=date(2026, 3, 8),
        verify_document=False,
    )
    with repo.connect() as conn:
        conn.execute(
            "UPDATE corporate_events SET effective_date = %s, status = %s WHERE source_event_id = %s",
            ("2026-03-08", "ACTIVE", "20260308000007"),
        )

    out = repair_corporate_event_timings(repo, "2026-03-01", "2026-03-31")
    assert out["upserted"] == 1

    rows = repo.query("SELECT effective_date, status, payload->>'repair_effective_date' AS repair_effective_date FROM corporate_events WHERE source_event_id = %s", ("20260308000007",))
    assert rows[0]["effective_date"] == "2026-04-30"
    assert rows[0]["status"] == "ACTIVE"
    assert rows[0]["repair_effective_date"] == "2026-04-30"


def test_repair_corporate_event_timings_blocks_duplicate_bonus_when_rights_bonus_exists(repo):
    _seed_instrument(repo)
    instrument_id = repo.get_instrument_id_by_external_code("123456", market_code="KOSDAQ")
    assert instrument_id
    repo.upsert_corporate_events([
        {
            "event_id": "evt_bonus_dup",
            "event_version": 1,
            "instrument_id": instrument_id,
            "event_type": "BONUS_ISSUE",
            "announce_date": "2026-02-10",
            "effective_date": "2026-02-10",
            "source_event_id": "dup1",
            "source_name": "opendart",
            "collected_at": "2026-02-10T00:00:00Z",
            "raw_factor": 0.5,
            "confidence": "HIGH",
            "status": "ACTIVE",
            "payload": {"report_nm": "???????(???????)"},
        },
        {
            "event_id": "evt_rights_bonus_dup",
            "event_version": 1,
            "instrument_id": instrument_id,
            "event_type": "RIGHTS_BONUS_ISSUE",
            "announce_date": "2026-02-10",
            "effective_date": "2026-02-10",
            "source_event_id": "dup1",
            "source_name": "opendart",
            "collected_at": "2026-02-10T00:00:00Z",
            "raw_factor": 0.5,
            "confidence": "HIGH",
            "status": "ACTIVE",
            "payload": {"report_nm": "???????(???????)"},
        },
    ])

    out = repair_corporate_event_timings(repo, "2026-02-01", "2026-02-28")
    assert out["upserted"] == 2
    rows = repo.query("SELECT event_type, status, payload->>'activation_issue' AS activation_issue FROM corporate_events WHERE source_event_id = %s ORDER BY event_type", ("dup1",))
    assert rows[0]["event_type"] == "BONUS_ISSUE"
    assert rows[0]["status"] == "NEEDS_REVIEW"
    assert rows[0]["activation_issue"] == "duplicate_rights_bonus_event"
    assert rows[1]["event_type"] == "RIGHTS_BONUS_ISSUE"
    assert rows[1]["status"] == "ACTIVE"


def test_repair_corporate_event_timings_blocks_document_only_rights_issue_without_pricing_inputs(repo):
    _seed_instrument(repo)
    instrument_id = repo.get_instrument_id_by_external_code("123456", market_code="KOSDAQ")
    assert instrument_id
    repo.upsert_corporate_events([
        {
            "event_id": "evt_rights_doc_only",
            "event_version": 1,
            "instrument_id": instrument_id,
            "event_type": "RIGHTS_ISSUE",
            "announce_date": "2026-02-10",
            "effective_date": "2026-02-10",
            "source_event_id": "doconly1",
            "source_name": "opendart",
            "collected_at": "2026-02-10T00:00:00Z",
            "raw_factor": 0.9,
            "confidence": "MEDIUM",
            "status": "ACTIVE",
            "payload": {"factor_rule": "rights_issue_keyword_sections", "report_nm": "???????(??????)"},
        },
    ])

    out = repair_corporate_event_timings(repo, "2026-02-01", "2026-02-28")
    assert out["upserted"] >= 1
    row = repo.query("SELECT status, payload->>'activation_issue' AS activation_issue FROM corporate_events WHERE source_event_id = %s", ("doconly1",))[0]
    assert row["status"] == "NEEDS_REVIEW"
    assert row["activation_issue"] == "missing_pricing_inputs"


def test_collect_corporate_events_stores_legal_and_apply_dates(repo):
    _seed_instrument(repo)

    collect_corporate_events(
        repo=repo,
        client=_FakeCapitalReductionDs005Client(),
        bgn_de=date(2026, 3, 8),
        end_de=date(2026, 3, 8),
        verify_document=False,
    )

    row = repo.query(
        "SELECT effective_date, payload->>'legal_effective_date' AS legal_effective_date, payload->>'adjustment_apply_date' AS adjustment_apply_date FROM corporate_events WHERE source_event_id = %s",
        ("20260308000007",),
    )[0]
    assert row["effective_date"] == "2026-04-30"
    assert row["legal_effective_date"] == "2026-04-30"
    assert row["adjustment_apply_date"] == "2026-04-30"


def test_derive_adjustment_apply_date_can_differ_from_legal_effective_date():
    ds005_row = {"crsc_nstklstprd": "2026-04-30"}
    doc_text = "\uD6A8\uB825\uBC1C\uC0DD\uC77C 2026\uB144 04\uC6D4 15\uC77C"

    legal_effective_date = _derive_legal_effective_date("CAPITAL_REDUCTION", {}, ds005_row, doc_text)
    adjustment_apply_date = _derive_adjustment_apply_date(
        "CAPITAL_REDUCTION",
        {},
        ds005_row,
        doc_text,
        legal_effective_date=legal_effective_date,
    )

    assert legal_effective_date == "2026-04-15"
    assert adjustment_apply_date == "2026-04-30"
