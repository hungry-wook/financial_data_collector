import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict


def load_dotenv(path: str = ".env") -> Dict[str, str]:
    env_path = Path(path)
    if not env_path.exists():
        return {}

    loaded = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        loaded[key] = value
        os.environ[key] = value
    return loaded


@dataclass
class KRXSettings:
    auth_key: str
    base_url: str
    api_path_instruments: str
    api_path_daily_market: str
    api_path_index_daily: str
    timeout_sec: int
    max_retries: int
    daily_limit: int

    @classmethod
    def from_env(cls) -> "KRXSettings":
        return cls(
            auth_key=os.getenv("KRX_AUTH_KEY", ""),
            base_url=os.getenv("KRX_BASE_URL", ""),
            api_path_instruments=os.getenv("KRX_API_PATH_INSTRUMENTS", ""),
            api_path_daily_market=os.getenv("KRX_API_PATH_DAILY_MARKET", ""),
            api_path_index_daily=os.getenv("KRX_API_PATH_INDEX_DAILY", ""),
            timeout_sec=int(os.getenv("KRX_TIMEOUT_SEC", "20")),
            max_retries=int(os.getenv("KRX_MAX_RETRIES", "5")),
            daily_limit=int(os.getenv("KRX_DAILY_LIMIT", "10000")),
        )

    def validate(self) -> None:
        missing = []
        if not self.auth_key:
            missing.append("KRX_AUTH_KEY")
        if not self.base_url:
            missing.append("KRX_BASE_URL")
        if not self.api_path_instruments:
            missing.append("KRX_API_PATH_INSTRUMENTS")
        if not self.api_path_daily_market:
            missing.append("KRX_API_PATH_DAILY_MARKET")
        if not self.api_path_index_daily:
            missing.append("KRX_API_PATH_INDEX_DAILY")
        if self.timeout_sec <= 0:
            missing.append("KRX_TIMEOUT_SEC(>0)")
        if self.max_retries <= 0:
            missing.append("KRX_MAX_RETRIES(>0)")
        if self.daily_limit <= 0:
            missing.append("KRX_DAILY_LIMIT(>0)")
        if missing:
            raise ValueError("Missing/invalid env: " + ", ".join(missing))
