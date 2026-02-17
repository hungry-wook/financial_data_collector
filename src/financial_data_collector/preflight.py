from dataclasses import dataclass
from typing import List

from .settings import KRXSettings, load_dotenv


@dataclass
class PreflightResult:
    ok: bool
    errors: List[str]


def run_preflight(env_path: str = ".env") -> PreflightResult:
    load_dotenv(env_path)
    settings = KRXSettings.from_env()
    try:
        settings.validate()
        return PreflightResult(ok=True, errors=[])
    except ValueError as exc:
        return PreflightResult(ok=False, errors=[str(exc)])

