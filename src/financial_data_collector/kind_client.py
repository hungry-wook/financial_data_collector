from dataclasses import dataclass
from datetime import date, datetime
from html.parser import HTMLParser
from typing import Dict, List, Optional

import requests


class KINDClientError(RuntimeError):
    pass


@dataclass
class KINDClientConfig:
    base_url: str = "https://kind.krx.co.kr"
    timeout_sec: int = 20
    max_retries: int = 5
    retry_backoff_sec: float = 1.0


MARKET_TYPE_TO_CODE = {
    "1": "KOSPI",
    "2": "KOSDAQ",
    "6": "KONEX",
}


def map_market_type_to_code(market_type: str) -> str:
    code = MARKET_TYPE_TO_CODE.get(str(market_type))
    if not code:
        raise ValueError(f"unsupported market_type={market_type}")
    return code


def _normalize_external_code(value: str) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.isdigit():
        if len(raw) > 6:
            return None
        return raw.zfill(6)
    return None


def _normalize_date_str(value: str) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError:
        return None


class _SimpleHtmlTableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self._in_table = False
        self._in_tr = False
        self._in_cell = False
        self._cell_buf: List[str] = []
        self._current_row: List[str] = []
        self.rows: List[List[str]] = []

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self._in_table = True
        elif self._in_table and tag == "tr":
            self._in_tr = True
            self._current_row = []
        elif self._in_tr and tag in {"td", "th"}:
            self._in_cell = True
            self._cell_buf = []

    def handle_data(self, data):
        if self._in_cell:
            self._cell_buf.append(data)

    def handle_endtag(self, tag):
        if self._in_tr and tag in {"td", "th"} and self._in_cell:
            value = "".join(self._cell_buf).strip()
            self._current_row.append(value)
            self._in_cell = False
        elif self._in_table and tag == "tr" and self._in_tr:
            if self._current_row:
                self.rows.append(self._current_row)
            self._in_tr = False
        elif tag == "table" and self._in_table:
            self._in_table = False


def parse_delisting_excel(content: bytes, market_code: str, source_name: str, collected_at: str) -> List[Dict]:
    try:
        html_text = content.decode("euc-kr", errors="replace")
    except Exception as exc:
        raise KINDClientError(f"failed to decode KIND response: {exc}") from exc

    parser = _SimpleHtmlTableParser()
    parser.feed(html_text)

    header_idx = None
    for idx, row in enumerate(parser.rows):
        if {"종목코드", "폐지일자"}.issubset(set(row)):
            header_idx = idx
            break
    if header_idx is None:
        raise KINDClientError("failed to parse KIND table header")

    headers = parser.rows[header_idx]
    hpos = {h: i for i, h in enumerate(headers)}
    rows: List[Dict] = []
    for row in parser.rows[header_idx + 1 :]:
        if len(row) < len(headers):
            continue
        external_code = _normalize_external_code(row[hpos["종목코드"]])
        delisting_date = _normalize_date_str(row[hpos["폐지일자"]])
        if not external_code or not delisting_date:
            continue
        rows.append(
            {
                "market_code": market_code,
                "external_code": external_code,
                "delisting_date": delisting_date,
                "delisting_reason": row[hpos["폐지사유"]].strip() if "폐지사유" in hpos else None,
                "note": row[hpos["비고"]].strip() if "비고" in hpos else None,
                "source_name": source_name,
                "collected_at": collected_at,
            }
        )
    return rows


class KINDClient:
    def __init__(self, config: Optional[KINDClientConfig] = None, session: Optional[requests.Session] = None):
        self.config = config or KINDClientConfig()
        self.session = session or requests.Session()

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        last_exc = None
        for attempt in range(self.config.max_retries):
            try:
                response = self.session.request(method, url, timeout=self.config.timeout_sec, **kwargs)
                response.raise_for_status()
                return response
            except Exception as exc:  # requests raises many subclasses
                last_exc = exc
                if attempt == self.config.max_retries - 1:
                    break
                delay = self.config.retry_backoff_sec * (2**attempt)
                try:
                    import time

                    time.sleep(delay)
                except Exception:
                    pass
        raise KINDClientError(f"KIND request failed after retries: {last_exc}") from last_exc

    def fetch_delistings(
        self,
        market_type: str,
        date_from: str,
        date_to: str,
        source_name: str = "kind",
        collected_at: Optional[str] = None,
    ) -> List[Dict]:
        market_code = map_market_type_to_code(market_type)
        now = collected_at or datetime.utcnow().isoformat() + "Z"
        main_url = f"{self.config.base_url}/investwarn/delcompany.do?method=searchDelCompanyMain"
        download_url = f"{self.config.base_url}/investwarn/delcompany.do"

        self._request_with_retry("GET", main_url)
        payload = {
            "method": "searchDelCompanySub",
            "forward": "delcompany_down",
            "pageIndex": "1",
            "currentPageSize": "3000",
            "marketType": str(market_type),
            "fromDate": date_from,
            "toDate": date_to,
            "searchCorpName": "",
            "searchCorpNameTmp": "",
            "searchCodeType": "",
            "repIsuSrtCd": "",
            "isurCd": "",
            "orderMode": "",
            "orderStat": "",
        }
        response = self._request_with_retry("POST", download_url, data=payload)
        return parse_delisting_excel(response.content, market_code=market_code, source_name=source_name, collected_at=now)
