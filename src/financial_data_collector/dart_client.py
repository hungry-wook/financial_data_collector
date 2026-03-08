import io
import re
import zipfile
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Optional

import requests

from .settings import OpenDARTSettings


class DARTClientError(RuntimeError):
    pass


@dataclass
class DARTClientConfig:
    api_key: str
    daily_limit: int = 10000
    base_url: str = "https://opendart.fss.or.kr/api"

    @classmethod
    def from_settings(cls, s: OpenDARTSettings) -> "DARTClientConfig":
        return cls(api_key=s.api_key, daily_limit=s.daily_limit)


class DARTClient:
    def __init__(self, config: DARTClientConfig, session: Optional[requests.Session] = None):
        self.config = config
        self.session = session or requests.Session()
        self._call_count = 0

    def _check_limit(self) -> None:
        if self._call_count >= self.config.daily_limit:
            raise DARTClientError("Daily API limit exceeded")

    @staticmethod
    def _yyyymmdd(value: date) -> str:
        return value.strftime("%Y%m%d")

    def _request_json(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        self._check_limit()
        query = {"crtfc_key": self.config.api_key, **params}
        url = f"{self.config.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        try:
            res = self.session.get(url, params=query, timeout=30)
            res.raise_for_status()
            payload = res.json()
        except requests.RequestException as exc:
            raise DARTClientError(f"OpenDART request failed: {exc}") from exc
        except ValueError as exc:
            raise DARTClientError("OpenDART response is not valid JSON") from exc
        finally:
            self._call_count += 1

        status = str(payload.get("status", ""))
        if status and status not in {"000", "013"}:
            msg = payload.get("message", "Unknown error")
            raise DARTClientError(f"OpenDART returned error status={status}: {msg}")
        return payload

    def list_filings(
        self,
        bgn_de: date,
        end_de: date,
        corp_code: str = "",
        pblntf_ty: str = "",
        page_no: int = 1,
        page_count: int = 100,
        last_reprt_at: str = "Y",
    ) -> Dict[str, Any]:
        if page_count < 1 or page_count > 100:
            raise DARTClientError("page_count must be between 1 and 100")
        params = {
            "bgn_de": self._yyyymmdd(bgn_de),
            "end_de": self._yyyymmdd(end_de),
            "page_no": page_no,
            "page_count": page_count,
            "last_reprt_at": last_reprt_at,
        }
        if corp_code:
            params["corp_code"] = corp_code
        if pblntf_ty:
            params["pblntf_ty"] = pblntf_ty
        return self._request_json("list.json", params)

    def get_ds005_endpoint(self, endpoint: str, corp_code: str, bgn_de: date, end_de: date) -> Dict[str, Any]:
        if not corp_code:
            raise DARTClientError("corp_code is required for DS005 endpoint")
        return self._request_json(
            endpoint,
            {
                "corp_code": corp_code,
                "bgn_de": self._yyyymmdd(bgn_de),
                "end_de": self._yyyymmdd(end_de),
            },
        )

    def get_bonus_issue_disclosures(self, corp_code: str, bgn_de: date, end_de: date) -> Dict[str, Any]:
        # DS005: Decision on Bonus Issue
        return self.get_ds005_endpoint("fricDecsn.json", corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)

    def get_rights_issue_disclosures(self, corp_code: str, bgn_de: date, end_de: date) -> Dict[str, Any]:
        # DS005: Decision on Rights Issue
        return self.get_ds005_endpoint("piicDecsn.json", corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)

    def get_rights_bonus_issue_disclosures(self, corp_code: str, bgn_de: date, end_de: date) -> Dict[str, Any]:
        # DS005: Decision on Rights and Bonus Issue
        return self.get_ds005_endpoint("pifricDecsn.json", corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)

    def get_capital_reduction_disclosures(self, corp_code: str, bgn_de: date, end_de: date) -> Dict[str, Any]:
        # DS005: Decision on Capital Reduction
        return self.get_ds005_endpoint("crDecsn.json", corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)

    def get_merger_disclosures(self, corp_code: str, bgn_de: date, end_de: date) -> Dict[str, Any]:
        # DS005: Decision on Merger
        return self.get_ds005_endpoint("cmpMgDecsn.json", corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)

    def get_split_disclosures(self, corp_code: str, bgn_de: date, end_de: date) -> Dict[str, Any]:
        # DS005: Decision on Company Split
        return self.get_ds005_endpoint("cmpDvDecsn.json", corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)

    def get_split_merger_disclosures(self, corp_code: str, bgn_de: date, end_de: date) -> Dict[str, Any]:
        # DS005: Decision on Split and Merger
        return self.get_ds005_endpoint("cmpDvmgDecsn.json", corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)

    def get_stock_swap_disclosures(self, corp_code: str, bgn_de: date, end_de: date) -> Dict[str, Any]:
        # DS005: Decision on Stock Exchange / Transfer
        return self.get_ds005_endpoint("stkExtrDecsn.json", corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)

    def get_document_zip(self, rcept_no: str) -> bytes:
        self._check_limit()
        url = f"{self.config.base_url.rstrip('/')}/document.xml"
        try:
            res = self.session.get(url, params={"crtfc_key": self.config.api_key, "rcept_no": rcept_no}, timeout=60)
            res.raise_for_status()
            body = res.content
        except requests.RequestException as exc:
            raise DARTClientError(f"OpenDART document request failed: {exc}") from exc
        finally:
            self._call_count += 1

        if not body:
            raise DARTClientError("OpenDART document body is empty")

        if body.lstrip().startswith(b"<") and b"<status>" in body[:5000]:
            text = body.decode("utf-8", errors="ignore")
            status = ""
            message = ""
            if "<status>" in text and "</status>" in text:
                status = text.split("<status>", 1)[1].split("</status>", 1)[0].strip()
            if "<message>" in text and "</message>" in text:
                message = text.split("<message>", 1)[1].split("</message>", 1)[0].strip()
            raise DARTClientError(f"OpenDART document error status={status or '?'}: {message or 'unknown'}")
        return body

    def get_corp_codes(self) -> Dict[str, str]:
        """
        Return mapping of corp_code -> stock_code from corpCode.xml.
        """
        self._check_limit()
        url = f"{self.config.base_url.rstrip('/')}/corpCode.xml"
        try:
            res = self.session.get(url, params={"crtfc_key": self.config.api_key}, timeout=60)
            res.raise_for_status()
            body = res.content
        except requests.RequestException as exc:
            raise DARTClientError(f"OpenDART corpCode request failed: {exc}") from exc
        finally:
            self._call_count += 1

        mapping: Dict[str, str] = {}
        try:
            with zipfile.ZipFile(io.BytesIO(body)) as zf:
                names = zf.namelist()
                if not names:
                    return mapping
                xml_text = zf.read(names[0]).decode("utf-8", errors="ignore")
        except Exception as exc:
            raise DARTClientError(f"OpenDART corpCode decode failed: {exc}") from exc

        for corp_code, stock_code in re.findall(
            r"<corp_code>\s*([0-9]{8})\s*</corp_code>[\s\S]*?<stock_code>\s*([0-9]{6})\s*</stock_code>",
            xml_text,
        ):
            mapping[str(corp_code)] = str(stock_code)
        return mapping


