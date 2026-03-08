from datetime import date

from financial_data_collector.collect_dart_corporate_events import collect_corporate_events
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
            zf.writestr("a.html", "<html><body>\uBB34\uC0C1\uC99D\uC790 1\uC8FC\uB2F9 0.5\uC8FC \uBC30\uC815</body></html>")
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
                "<html><body>\uC720\uC0C1\uC99D\uC790 \uACB0\uC815 1. \uC2E0\uC8FC\uC758 \uC885\uB958\uC640 \uC218 \uBCF4\uD1B5\uC8FC\uC2DD (\uC8FC) 100 2. 1\uC8FC\uB2F9 \uC561\uBA74\uAC00\uC561 (\uC6D0) 500 3. \uC99D\uC790\uC804 \uBC1C\uD589\uC8FC\uC2DD\uCD1D\uC218 (\uC8FC) \uBCF4\uD1B5\uC8FC\uC2DD (\uC8FC) 900</body></html>",
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
