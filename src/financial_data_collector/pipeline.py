from datetime import date
from typing import Dict, List

from .calendar_builder import TradingCalendarBuilder
from .collectors import BenchmarkCollector, DailyMarketCollector, InstrumentCollector
from .repository import Repository
from .runs import RunManager
from .validation import ValidationJob


class Phase1Pipeline:
    def __init__(self, repo: Repository):
        self.repo = repo
        self.runs = RunManager(repo)
        self.instrument_collector = InstrumentCollector(repo)
        self.daily_collector = DailyMarketCollector(repo)
        self.benchmark_collector = BenchmarkCollector(repo)
        self.calendar_builder = TradingCalendarBuilder(repo)
        self.validation = ValidationJob(repo)

    def run(
        self,
        market_code: str,
        date_from: date,
        date_to: date,
        instruments: List[Dict],
        daily_market: List[Dict],
        benchmark: List[Dict],
    ) -> Dict:
        run_id = self.runs.start("phase1", "krx", str(date_from), str(date_to))
        try:
            i_count = self.instrument_collector.collect(instruments, "krx")
            d_count = self.daily_collector.collect(daily_market, "krx", run_id)
            b_count = self.benchmark_collector.collect(benchmark, "krx", run_id)
            index_days = sorted({row["trade_date"] for row in benchmark})
            c_count = self.calendar_builder.build_from_index_days(
                market_code=market_code,
                date_from=date_from,
                date_to=date_to,
                index_trade_dates=index_days,
                source_name="krx",
                run_id=run_id,
            )
            v = self.validation.validate_range(market_code, str(date_from), str(date_to), run_id)
            failures = v["issues"]
            self.runs.finish(
                run_id,
                success_count=i_count + d_count + b_count + c_count,
                failure_count=failures,
                warning_count=0,
            )
            return {"run_id": run_id, "counts": {"instruments": i_count, "daily": d_count, "benchmark": b_count, "calendar": c_count}, "validation": v}
        except Exception:
            self.runs.fail(run_id)
            raise

