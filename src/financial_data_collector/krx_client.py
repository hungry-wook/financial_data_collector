import time
from dataclasses import dataclass
from datetime import date
from typing import Dict, Optional

import requests

from .settings import KRXSettings


class KRXClientError(RuntimeError):
    pass


@dataclass
class KRXClientConfig:
    base_url: str
    auth_key: str
    api_path_instruments: str = "stock/master"
    api_path_daily_market: str = "stock/daily"
    api_path_index_daily: str = "index/daily"
    timeout_sec: int = 20
    max_retries: int = 5
    daily_limit: int = 10000

    @classmethod
    def from_settings(cls, s: KRXSettings) -> "KRXClientConfig":
        return cls(
            base_url=s.base_url,
            auth_key=s.auth_key,
            api_path_instruments=s.api_path_instruments,
            api_path_daily_market=s.api_path_daily_market,
            api_path_index_daily=s.api_path_index_daily,
            timeout_sec=s.timeout_sec,
            max_retries=s.max_retries,
            daily_limit=s.daily_limit,
        )


class KRXClient:
    def __init__(self, config: KRXClientConfig, session: Optional[requests.Session] = None):
        self.config = config
        self.session = session or requests.Session()
        self._call_count = 0

    def _headers(self) -> Dict[str, str]:
        return {"AUTH_KEY": self.config.auth_key}

    def _check_limit(self) -> None:
        if self._call_count >= self.config.daily_limit:
            raise KRXClientError("Daily API limit exceeded")

    def request(self, api_path: str, params: Dict) -> Dict:
        self._check_limit()
        last_error: Optional[Exception] = None

        for attempt in range(self.config.max_retries):
            try:
                resp = self.session.get(
                    f"{self.config.base_url.rstrip('/')}/{api_path.lstrip('/')}",
                    params=params,
                    headers=self._headers(),
                    timeout=self.config.timeout_sec,
                )
                self._call_count += 1
                if resp.status_code >= 500:
                    raise KRXClientError(f"Server error {resp.status_code}")
                if resp.status_code >= 400:
                    raise KRXClientError(f"Client error {resp.status_code}")
                payload = resp.json()
                if not payload:
                    raise KRXClientError("Empty response payload")
                return payload
            except (requests.RequestException, ValueError, KRXClientError) as exc:
                last_error = exc
                if attempt == self.config.max_retries - 1:
                    break
                time.sleep(2**attempt)

        raise KRXClientError(f"Request failed after retries: {last_error}")

    def get_instruments(self, market_code: str, base_date: date) -> Dict:
        return self.request(
            self.config.api_path_instruments,
            {"market_code": market_code, "base_date": base_date.isoformat()},
        )

    def get_daily_market(self, market_code: str, trade_date: date) -> Dict:
        return self.request(
            self.config.api_path_daily_market,
            {"market_code": market_code, "trade_date": trade_date.isoformat()},
        )

    def get_index_daily(self, index_code: str, trade_date: date) -> Dict:
        return self.request(
            self.config.api_path_index_daily,
            {"index_code": index_code, "trade_date": trade_date.isoformat()},
        )
