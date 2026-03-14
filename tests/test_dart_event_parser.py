import io
import zipfile

from financial_data_collector.dart_event_parser import extract_text_from_document_zip, infer_effective_date, infer_event_status, infer_raw_factor, infer_rights_issue_subtype


def test_extract_text_from_document_zip_reads_html_payload():
    body = _zip_single(
        "doc.html",
        "<html><body><h1>테스트</h1><p>1주당 0.5주</p></body></html>",
    )
    text = extract_text_from_document_zip(body)
    assert "1주당 0.5주" in text


def test_extract_text_from_document_zip_decodes_cp949_xml_payload():
    xml_text = (
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        "<DOC><P>\uc8fc\uc694\uc0ac\ud56d\ubcf4\uace0\uc11c(\ubd84\ud560(\ubd84\ud560\ud569\ubcd1)\uacb0\uc815)</P>"
        "<P>\uc0bc\uc131\uc804\uc790\uc8fc\uc2dd\ud68c\uc0ac</P></DOC>"
    )
    body = _zip_single_bytes("doc.xml", xml_text.encode("cp949"))
    text = extract_text_from_document_zip(body)
    assert "\uc8fc\uc694\uc0ac\ud56d\ubcf4\uace0\uc11c(\ubd84\ud560(\ubd84\ud560\ud569\ubcd1)\uacb0\uc815)" in text
    assert "\uc0bc\uc131\uc804\uc790\uc8fc\uc2dd\ud68c\uc0ac" in text


def test_infer_raw_factor_bonus_issue():
    factor, rule = infer_raw_factor("BONUS_ISSUE", "무상증자 1주당 0.5주 배정")
    assert round(factor, 6) == round(1 / 1.5, 6)
    assert rule == "bonus_issue_1_share_allocation"


def test_infer_raw_factor_capital_reduction_phrase_with_gap_text():
    text = "액면가 1,000원 보통주식 10주를동일 액면가의 보통주식 1주로 병합"
    factor, rule = infer_raw_factor("CAPITAL_REDUCTION", text)
    assert factor == 10.0
    assert rule == "capital_reduction_ratio"


def test_infer_raw_factor_capital_reduction_section4_before_after():
    text = """
    감자 결정
    4. 감자전후 발행주식수
    구 분 감자전 (주) 감자후 (주)
    보통주식(주) 67,236,039 6,723,603
    """
    factor, rule = infer_raw_factor("CAPITAL_REDUCTION", text)
    assert round(factor, 6) == round(67236039 / 6723603, 6)
    assert rule == "capital_reduction_section4_before_after"


def test_infer_raw_factor_section_parser_ignores_dates():
    text = """
    유상증자 결정 2.3 회사명 공시일자 2024.03.25 관련법규 공정거래법
    1. 신주의 종류와 수 보통주식 (주) 4,450,000 우선주식 (주) -
    2. 1주당 액면가액 (원) 500
    3. 증자전 발행주식총수 (주) 보통주식 (주) 40,000,000 우선주식 (주) -
    """
    factor, rule = infer_raw_factor("RIGHTS_ISSUE", text)
    assert round(factor, 10) == round(40000000 / 44450000, 10)
    assert rule == "rights_issue_keyword_sections"


def test_infer_raw_factor_rights_issue_section1_3():
    text = """
    유상증자 결정
    1. 신주의 종류와 수 보통주식 (주) 118,040 기타주식 (주) -
    2. 1주당 액면가액 (원) 0
    3. 증자전 발행주식총수 (주) 보통주식 (주) 16,651,267 기타주식 (주) 117,647
    """
    factor, rule = infer_raw_factor("RIGHTS_ISSUE", text)
    expected = 16651267 / (16651267 + 118040)
    assert round(factor, 10) == round(expected, 10)
    assert rule == "rights_issue_keyword_sections"


def test_infer_raw_factor_split_par_value_row_returns_inverse_ratio():
    text = "주식분할결정 1. 주식분할 내용 구분 분할 전 분할 후 1주당 가액(원) 5,000 100 발행주식총수 보통주식(주) 128,386,494 6,419,324,700"
    factor, rule = infer_raw_factor("SPLIT", text)
    assert round(factor, 10) == round(100 / 5000, 10)
    assert rule == "split_par_value_ratio"


def test_infer_raw_factor_split_no_direct_ratio_returns_one():
    text = "물적분할 방식이므로 분할비율을 산정하지 않는다"
    factor, rule = infer_raw_factor("SPLIT", text)
    assert factor == 1.0
    assert rule == "split_no_direct_ratio"


def test_infer_raw_factor_merger_no_new_share_returns_one():
    text = "합병신주를 발행하지 않는 무증자합병"
    factor, rule = infer_raw_factor("MERGER", text)
    assert factor == 1.0
    assert rule == "structural_no_new_share_text"


def test_infer_event_status_detects_withdrawn_event():
    text = "[기재정정] 주요사항보고서(유상증자결정) 유상증자 결정 철회"
    status, rule = infer_event_status("RIGHTS_ISSUE", text)
    assert status == "REJECTED"
    assert rule == "event_withdrawn_or_cancelled"


def _zip_single_bytes(name: str, payload: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(name, payload)
    return buf.getvalue()


def _zip_single(name: str, text: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(name, text)
    return buf.getvalue()


def test_infer_rights_issue_subtype_third_party_from_ds005():
    subtype = infer_rights_issue_subtype(ds005_row={"ic_mthn": "\uC81C3\uC790\uBC30\uC815\uC99D\uC790"})
    assert subtype == "RIGHTS_ISSUE_THIRD_PARTY"


def test_infer_effective_date_from_capital_reduction_listing_date():
    value = infer_effective_date("CAPITAL_REDUCTION", "", {"crsc_nstklstprd": "2026-04-30"})
    assert value == "2026-04-30"


def test_infer_effective_date_from_split_listing_date_text():
    text = "주식분할결정 매매거래정지기간 2018-04-25 ~ 신주변경상장일 전일 신주권상장예정일 2018-05-16"
    value = infer_effective_date("SPLIT", text)
    assert value == "2018-05-16"


def test_infer_effective_date_from_capital_reduction_trading_resumption_notice():
    text = "주권매매거래정지해제 1.대상종목 (주)DM테크놀로지 보통주 2.해제사유 감자 주권 변경상장 3.해제일시 2010-02-09"
    value = infer_effective_date("CAPITAL_REDUCTION", text)
    assert value == "2010-02-09"


def test_infer_raw_factor_skips_capital_reduction_market_notice():
    text = "주권매매거래정지해제 1.대상종목 (주)DM테크놀로지 보통주 2.해제사유 감자 주권 변경상장 3.해제일시 2010-02-09"
    factor, rule = infer_raw_factor("CAPITAL_REDUCTION", text)
    assert factor is None
    assert rule == "capital_reduction_market_notice_no_factor"


def test_infer_raw_factor_capital_reduction_method_ratio_with_par_value_between_counts():
    text = """
    \uAC10\uC790 \uACB0\uC815
    7. \uAC10\uC790\uBC29\uBC95
    \uAE30\uBA85\uC2DD \uBCF4\uD1B5\uC8FC\uC2DD \uC561\uBA74\uAC00 500\uC6D0 30\uC8FC\uB97C \uB3D9\uC77C \uC561\uBA74\uAC00 500\uC6D0 1\uC8FC\uB85C \uBCD1\uD569
    """
    factor, rule = infer_raw_factor("CAPITAL_REDUCTION", text)
    assert factor == 30.0
    assert rule == "capital_reduction_ratio"