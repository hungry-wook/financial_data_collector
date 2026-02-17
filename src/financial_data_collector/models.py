from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional


@dataclass
class CollectionRun:
    run_id: str
    pipeline_name: str
    source_name: str
    window_start: date
    window_end: date
    status: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    success_count: int = 0
    failure_count: int = 0
    warning_count: int = 0
    metadata: Optional[str] = None


@dataclass
class InstrumentRow:
    instrument_id: str
    external_code: str
    market_code: str
    instrument_name: str
    listing_date: date
    delisting_date: Optional[date]
    source_name: str
    collected_at: datetime
    updated_at: Optional[datetime] = None


@dataclass
class DailyMarketRow:
    instrument_id: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    turnover_value: Optional[float]
    market_value: Optional[float]
    is_trade_halted: bool
    is_under_supervision: bool
    record_status: str
    source_name: str
    collected_at: datetime
    run_id: Optional[str]


@dataclass
class BenchmarkRow:
    index_code: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    source_name: str
    collected_at: datetime
    run_id: Optional[str]


@dataclass
class TradingCalendarRow:
    market_code: str
    trade_date: date
    is_open: bool
    holiday_name: Optional[str]
    source_name: str
    collected_at: datetime
    run_id: Optional[str]


@dataclass
class DataQualityIssue:
    dataset_name: str
    trade_date: Optional[date]
    instrument_id: Optional[str]
    index_code: Optional[str]
    issue_code: str
    severity: str
    issue_detail: Optional[str]
    source_name: Optional[str]
    detected_at: datetime
    run_id: Optional[str]
    resolved_at: Optional[datetime] = None

