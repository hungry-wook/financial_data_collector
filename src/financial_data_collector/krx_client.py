from dataclasses import dataclass
from datetime import date
from typing import Dict, Optional

from .settings import KRXSettings


class KRXClientError(RuntimeError):
    pass


@dataclass
class KRXClientConfig:
    auth_key: str
    daily_limit: int = 10000

    @classmethod
    def from_settings(cls, s: KRXSettings) -> "KRXClientConfig":
        return cls(
            auth_key=s.auth_key,
            daily_limit=s.daily_limit,
        )


class KRXClient:
    def __init__(
        self,
        config: KRXClientConfig,
        openapi_client=None,
    ):
        self.config = config
        self.openapi_client = openapi_client or self._build_openapi_client()
        self._call_count = 0
        if self.openapi_client is None:
            raise KRXClientError(
                "pykrx_openapi is required but unavailable. Install dependency and verify AUTH_KEY."
            )

    def _build_openapi_client(self):
        try:
            from pykrx_openapi import KRXOpenAPI
        except Exception:
            return None
        try:
            return KRXOpenAPI(api_key=self.config.auth_key)
        except Exception:
            return None

    def _check_limit(self) -> None:
        if self._call_count >= self.config.daily_limit:
            raise KRXClientError("Daily API limit exceeded")

    @staticmethod
    def _to_bas_dd(value: date) -> str:
        return value.strftime("%Y%m%d")

    def _request_with_openapi(self, method_name: str, bas_dd: str) -> Dict:
        if not self.openapi_client:
            raise KRXClientError("pykrx_openapi client is unavailable")
        self._check_limit()
        try:
            fn = getattr(self.openapi_client, method_name)
        except AttributeError as exc:
            raise KRXClientError(f"pykrx_openapi method not found: {method_name}") from exc
        try:
            payload = fn(bas_dd=bas_dd)
            self._call_count += 1
            if payload is None:
                raise KRXClientError("Empty response payload")
            return payload
        except Exception as exc:
            raise KRXClientError(f"pykrx_openapi request failed: {exc}") from exc

    @staticmethod
    def _instrument_method_name(market_code: str) -> Optional[str]:
        code = market_code.upper()
        return {
            "KOSPI": "get_stock_base_info",
            "KOSDAQ": "get_kosdaq_stock_base_info",
            "KONEX": "get_konex_base_info",
        }.get(code)

    @staticmethod
    def _daily_market_method_name(market_code: str) -> Optional[str]:
        code = market_code.upper()
        return {
            "KOSPI": "get_stock_daily_trade",
            "KOSDAQ": "get_kosdaq_stock_daily_trade",
            "KONEX": "get_konex_daily_trade",
        }.get(code)

    @staticmethod
    def _index_daily_method_name(index_code: str) -> Optional[str]:
        code = index_code.upper()
        return {
            "KOSPI": "get_kospi_daily_trade",
            "KOSDAQ": "get_kosdaq_daily_trade",
            "KRX": "get_krx_daily_trade",
        }.get(code)

    def get_instruments(self, market_code: str, base_date: date) -> Dict:
        method_name = self._instrument_method_name(market_code)
        if not method_name:
            raise KRXClientError(f"Unsupported market_code={market_code}")
        return self._request_with_openapi(method_name, self._to_bas_dd(base_date))

    def get_daily_market(self, market_code: str, trade_date: date) -> Dict:
        method_name = self._daily_market_method_name(market_code)
        if not method_name:
            raise KRXClientError(f"Unsupported market_code={market_code}")
        return self._request_with_openapi(method_name, self._to_bas_dd(trade_date))

    def get_index_daily(self, index_code: str, trade_date: date) -> Dict:
        method_name = self._index_daily_method_name(index_code)
        if not method_name:
            raise KRXClientError(f"Unsupported index_code={index_code}")
        return self._request_with_openapi(method_name, self._to_bas_dd(trade_date))
