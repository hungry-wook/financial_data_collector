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
    daily_limit: int

    @classmethod
    def from_env(cls) -> "KRXSettings":
        auth_key = os.getenv("KRX_AUTH_KEY", "") or os.getenv("KRX_OPENAPI_KEY", "")
        return cls(
            auth_key=auth_key,
            daily_limit=int(os.getenv("KRX_DAILY_LIMIT", "10000")),
        )

    def validate(self) -> None:
        missing = []
        if not self.auth_key:
            missing.append("KRX_AUTH_KEY(or KRX_OPENAPI_KEY)")
        if self.daily_limit <= 0:
            missing.append("KRX_DAILY_LIMIT(>0)")
        if missing:
            raise ValueError("Missing/invalid env: " + ", ".join(missing))
