import io
import zipfile

from financial_data_collector.dart_event_parser import extract_text_from_document_zip, infer_event_status, infer_raw_factor


def test_extract_text_from_document_zip_reads_html_payload():
    body = _zip_single(
        "doc.html",
        "<html><body><h1>테스트</h1><p>1주당 0.5주</p></body></html>",
    )
    text = extract_text_from_document_zip(body)
    assert "1주당 0.5주" in text


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


def _zip_single(name: str, text: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(name, text)
    return buf.getvalue()
