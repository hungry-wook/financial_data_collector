from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Optional

from .collectors import BenchmarkCollector, DailyMarketCollector, InstrumentCollector
from .export_service import ExportRequest, ExportService
from .repository import Repository


@dataclass
class SampleRunConfig:
    database_url: str
    output_path: str
    market_code: str = "KOSDAQ"
    index_code: str = "KOSDAQ"
    base_date: date = date(2026, 1, 2)


def run_backtest_sample(config: SampleRunConfig, export_service: Optional[ExportService] = None) -> Dict:
    repo = Repository(config.database_url)
    repo.init_schema()

    InstrumentCollector(repo).collect(
        [
            {
                "instrument_id": "550e8400-e29b-41d4-a716-446655440001",
                "external_code": "0001",
                "market_code": config.market_code,
                "instrument_name": "Sample Instrument",
                "listing_date": date(2020, 1, 1),
            }
        ],
        "sample",
    )
    DailyMarketCollector(repo).collect(
        [
            {
                "instrument_id": "550e8400-e29b-41d4-a716-446655440001",
                "trade_date": config.base_date,
                "open": 100,
                "high": 110,
                "low": 95,
                "close": 105,
                "volume": 1000,
                "turnover_value": 102000,
                "market_value": 1000000,
            }
        ],
        "sample",
        "sample_run",
    )
    BenchmarkCollector(repo).collect(
        [
            {
                "index_code": config.index_code,
                "trade_date": config.base_date,
                "open": 1000,
                "high": 1010,
                "low": 995,
                "close": 1005,
            }
        ],
        "sample",
        "sample_run",
    )
    repo.upsert_trading_calendar(
        [
            {
                "market_code": config.market_code,
                "trade_date": config.base_date.isoformat(),
                "is_open": True,
                "holiday_name": None,
                "source_name": "sample",
                "collected_at": f"{config.base_date.isoformat()}T00:00:00",
                "run_id": None,
            }
        ]
    )

    service = export_service or ExportService(repo)
    request = ExportRequest(
        market_codes=[config.market_code],
        index_codes=[config.index_code],
        date_from=config.base_date.isoformat(),
        date_to=config.base_date.isoformat(),
        include_issues=True,
        output_format="parquet",
        output_path=config.output_path,
    )
    created = service.create_job(request)
    result = service.run_job(created["job_id"])
    return {"job_id": created["job_id"], "result": result, "output_path": Path(config.output_path).as_posix()}
