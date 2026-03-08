import io
import re
import zipfile
from html import unescape
from typing import List, Optional, Tuple


BONUS_ISSUE_PER_SHARE = r"1\s*\uC8FC\s*\uB2F9\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*\uC8FC"
RIGHTS_NEW_SHARES = r"\uC2E0\uC8FC\s*\uC218\s*[:\uFF1A]?\s*([0-9][0-9,]*)"
RIGHTS_OLD_SHARES = r"\uC99D\uC790\s*\uC804\s*\uBC1C\uD589\s*\uC8FC\uC2DD\s*\uCD1D\uC218\s*[:\uFF1A]?\s*([0-9][0-9,]*)"
RIGHTS_SECTION1_COMMON = (
    r"(?:\uC2E0\uC8FC\uC758?\s*\uC885\uB958\uC640\s*\uC218|\uC2E0\uC8FC\s*\uBC1C\uD589\s*\uC8FC\uC2DD\uC218)"
    r"[\s\S]{0,140}?\uBCF4\uD1B5\uC8FC\uC2DD\s*\(\s*\uC8FC\s*\)\s*([0-9][0-9,]*)"
)
RIGHTS_SECTION3_COMMON = (
    r"(?:\uC99D\uC790\s*\uC804\s*\uBC1C\uD589\s*\uC8FC\uC2DD\s*\uCD1D\uC218|\uAE30\uBC1C\uD589\s*\uC8FC\uC2DD\uC218)"
    r"[\s\S]{0,180}?\uBCF4\uD1B5\uC8FC\uC2DD\s*\(\s*\uC8FC\s*\)\s*([0-9][0-9,]*)"
)


def _strip_tags(text: str) -> str:
    no_script = re.sub(r"<script[\s\S]*?</script>", " ", text, flags=re.IGNORECASE)
    no_style = re.sub(r"<style[\s\S]*?</style>", " ", no_script, flags=re.IGNORECASE)
    plain = re.sub(r"<[^>]+>", " ", no_style)
    plain = unescape(plain)
    plain = re.sub(r"\s+", " ", plain)
    return plain.strip()


def extract_text_from_document_zip(body: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(body)) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith((".xml", ".xhtml", ".html", ".htm", ".txt"))]
        if not names:
            return ""
        chunks = []
        for name in names[:8]:
            try:
                raw = zf.read(name)
            except KeyError:
                continue
            text = raw.decode("utf-8", errors="ignore")
            chunks.append(_strip_tags(text))
        return " ".join([c for c in chunks if c]).strip()


def _find_first_number(pattern: str, text: str) -> Optional[float]:
    m = re.search(pattern, text)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _find_all_numbers(pattern: str, text: str) -> List[float]:
    out: List[float] = []
    for m in re.findall(pattern, text):
        try:
            out.append(float(str(m).replace(",", "")))
        except ValueError:
            continue
    return out


def _extract_section_text(body: str, section_no: int, max_len: int = 500) -> Optional[str]:
    # Avoid matching dates like 2024.03.25 as section headers.
    pattern = rf"(?<![0-9]){section_no}\.\s*([\s\S]{{0,{max_len}}}?)(?=(?<![0-9])\d+\.\s|$)"
    m = re.search(pattern, body)
    if not m:
        return None
    return m.group(1)


def _pick_ratio_pair(numbers: List[float], prefer_before_ge_after: bool) -> Optional[Tuple[float, float]]:
    if len(numbers) < 2:
        return None
    for i in range(len(numbers) - 1):
        before = numbers[i]
        after = numbers[i + 1]
        if before <= 0 or after <= 0:
            continue
        ratio = before / after
        if ratio <= 0 or ratio > 100:
            continue
        if prefer_before_ge_after and before < after:
            continue
        return before, after
    return None


def infer_event_status(event_type: str, text: str) -> Optional[Tuple[str, str]]:
    body = str(text or "")
    et = str(event_type or "").strip().upper()
    if et not in {"BONUS_ISSUE", "RIGHTS_ISSUE", "RIGHTS_BONUS_ISSUE", "CAPITAL_REDUCTION"}:
        return None

    if re.search(r"(철회|취소)", body) and re.search(r"(유상증자|무상증자|유무상증자|감자)", body):
        return "REJECTED", "event_withdrawn_or_cancelled"

    return None


def infer_raw_factor(event_type: str, text: str) -> Tuple[Optional[float], str]:
    body = str(text or "")
    et = str(event_type or "").strip().upper()

    if et == "BONUS_ISSUE":
        x = _find_first_number(BONUS_ISSUE_PER_SHARE, body)
        if x is not None and x > 0:
            return 1.0 / (1.0 + x), "bonus_issue_1_share_allocation"

    if et == "CAPITAL_REDUCTION":
        m = re.search(
            r"([0-9][0-9,]*(?:\.[0-9]+)?)\s*\uC8FC\s*\uB97C\s*[^0-9]{0,24}([0-9][0-9,]*(?:\.[0-9]+)?)\s*\uC8FC\s*\uB85C\s*\uBCD1\uD569",
            body,
        )
        if m:
            try:
                before = float(m.group(1).replace(",", ""))
                after = float(m.group(2).replace(",", ""))
            except ValueError:
                before = 0.0
                after = 0.0
            if before > 0 and after > 0:
                return before / after, "capital_reduction_ratio"

        section4 = _extract_section_text(body, 4, max_len=600)
        if section4:
            nums = _find_all_numbers(r"([0-9][0-9,]*)", section4)
            pair = _pick_ratio_pair(nums, prefer_before_ge_after=True)
            if pair:
                before, after = pair
                return before / after, "capital_reduction_section4_before_after"

        section5 = _extract_section_text(body, 5, max_len=220)
        if section5:
            percent = _find_first_number(r"([0-9]+(?:\.[0-9]+)?)\s*%", section5)
            if percent is not None and 0 < percent < 100:
                remain = 1.0 - (percent / 100.0)
                if remain > 0:
                    return 1.0 / remain, "capital_reduction_section5_percent"

    if et in {"SPLIT", "SPLIT_MERGER"}:
        m = re.search(r"([0-9][0-9,]*(?:\.[0-9]+)?)\s*\uC8FC\s*\uB97C\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*\uC8FC", body)
        if m:
            try:
                before = float(m.group(1).replace(",", ""))
                after = float(m.group(2).replace(",", ""))
            except ValueError:
                before = 0.0
                after = 0.0
            if before > 0 and after > 0:
                return before / after, "split_ratio"

        section4 = _extract_section_text(body, 4, max_len=600)
        if section4:
            nums = _find_all_numbers(r"([0-9][0-9,]*)", section4)
            pair = _pick_ratio_pair(nums, prefer_before_ge_after=False)
            if pair:
                before, after = pair
                return before / after, "split_section4_before_after"

        if re.search(r"\uBD84\uD560\uBE44\uC728[\s\S]{0,24}\uC0B0\uC815\uD558\uC9C0\s*\uC54A", body):
            return 1.0, "split_no_direct_ratio"

    if et in {"RIGHTS_ISSUE", "RIGHTS_BONUS_ISSUE"}:
        new_shares = _find_first_number(RIGHTS_NEW_SHARES, body)
        old_shares = _find_first_number(RIGHTS_OLD_SHARES, body)
        if old_shares and new_shares and old_shares > 0 and new_shares > 0:
            return old_shares / (old_shares + new_shares), "rights_issue_share_count"

        new_shares = _find_first_number(RIGHTS_SECTION1_COMMON, body)
        old_shares = _find_first_number(RIGHTS_SECTION3_COMMON, body)
        if old_shares and new_shares and old_shares > 0 and new_shares > 0:
            return old_shares / (old_shares + new_shares), "rights_issue_keyword_sections"

        sec1 = _extract_section_text(body, 1, max_len=450)
        sec3 = _extract_section_text(body, 3, max_len=500)
        if sec1 and sec3:
            sec1_nums = _find_all_numbers(r"([0-9][0-9,]*)", sec1)
            sec3_nums = _find_all_numbers(r"([0-9][0-9,]*)", sec3)
            if sec1_nums and sec3_nums:
                new_v = sec1_nums[0]
                old_v = sec3_nums[0]
                # If section 1 and 3 each expose a leading share count, trust the structured section values.
                if old_v > 0 and new_v > 0:
                    return old_v / (old_v + new_v), "rights_issue_section1_3"

    if et in {"MERGER", "STOCK_SWAP", "STOCK_TRANSFER"}:
        if re.search(r"1(?:\.0+)?\s*[:：]\s*0(?:\.0+)?", body):
            return 1.0, "structural_no_new_share_ratio"
        if re.search(r"\uD569\uBCD1\uC2E0\uC8FC[\s\S]{0,24}\uBC1C\uD589[\s\S]{0,12}\uC54A", body):
            return 1.0, "structural_no_new_share_text"

    return None, "no_rule_matched"
