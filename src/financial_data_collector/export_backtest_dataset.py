import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import requests


JsonGetter = Callable[[str, Dict[str, object]], object]

INSTRUMENT_SCHEMA = pa.schema([
    pa.field("external_code", pa.string()),
    pa.field("market_code", pa.string()),
    pa.field("instrument_name", pa.string()),
    pa.field("listing_date", pa.string()),
    pa.field("delisting_date", pa.string()),
    pa.field("listed_status", pa.string()),
    pa.field("delisting_reason", pa.string()),
    pa.field("delisting_note", pa.string()),
])

INSTRUMENT_DAILY_SCHEMA = pa.schema([
    pa.field("instrument_id", pa.string()),
    pa.field("external_code", pa.string()),
    pa.field("market_code", pa.string()),
    pa.field("instrument_name", pa.string()),
    pa.field("trade_date", pa.string()),
    pa.field("listing_date", pa.string()),
    pa.field("delisting_date", pa.string()),
    pa.field("is_trade_halted", pa.bool_()),
    pa.field("record_status", pa.string()),
    pa.field("open", pa.float64()),
    pa.field("high", pa.float64()),
    pa.field("low", pa.float64()),
    pa.field("close", pa.float64()),
    pa.field("volume", pa.int64()),
    pa.field("turnover_value", pa.float64()),
    pa.field("market_value", pa.float64()),
    pa.field("listed_shares", pa.int64()),
    pa.field("adj_open", pa.float64()),
    pa.field("adj_high", pa.float64()),
    pa.field("adj_low", pa.float64()),
    pa.field("adj_close", pa.float64()),
    pa.field("adj_volume", pa.float64()),
    pa.field("base_price", pa.float64()),
    pa.field("daily_factor", pa.float64()),
    pa.field("cumulative_factor", pa.float64()),
])

BENCHMARK_SCHEMA = pa.schema([
    pa.field("index_code", pa.string()),
    pa.field("index_name", pa.string()),
    pa.field("record_count", pa.int64()),
    pa.field("date_from", pa.string()),
    pa.field("date_to", pa.string()),
])

BENCHMARK_DAILY_SCHEMA = pa.schema([
    pa.field("index_code", pa.string()),
    pa.field("index_name", pa.string()),
    pa.field("trade_date", pa.string()),
    pa.field("open", pa.float64()),
    pa.field("high", pa.float64()),
    pa.field("low", pa.float64()),
    pa.field("close", pa.float64()),
    pa.field("volume", pa.int64()),
    pa.field("turnover_value", pa.float64()),
    pa.field("market_cap", pa.float64()),
    pa.field("record_status", pa.string()),
])

CALENDAR_SCHEMA = pa.schema([
    pa.field("market_code", pa.string()),
    pa.field("trade_date", pa.string()),
    pa.field("is_open", pa.bool_()),
    pa.field("holiday_name", pa.string()),
])

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class HttpApiClient:
    def __init__(self, base_url: str, timeout_sec: int = 30):
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.session = requests.Session()

    def get_json(self, path: str, params: Dict[str, object]) -> object:
        response = self.session.get(f"{self.base_url}{path}", params=params, timeout=self.timeout_sec)
        response.raise_for_status()
        return response.json()


class ParquetTableWriter:
    def __init__(self, path: Path, schema: pa.Schema):
        self.path = path
        self.schema = pa.schema([pa.field(field.name, field.type, nullable=True) for field in schema])
        self.fieldnames = list(self.schema.names)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.writer = pq.ParquetWriter(self.path, self.schema, compression="zstd")
        self.row_count = 0

    def write_rows(self, rows: Iterable[Dict[str, object]]) -> None:
        payload = [{field: row.get(field) for field in self.fieldnames} for row in rows]
        if not payload:
            return
        table = pa.Table.from_pylist(payload, schema=self.schema)
        self.writer.write_table(table)
        self.row_count += len(payload)

    def close(self) -> None:
        self.writer.close()


def _resolve_date_window(api_get: JsonGetter, date_from: str, date_to: str) -> tuple[str, str]:
    if date_from and date_to:
        return date_from, date_to
    summary = api_get("/api/v1/dashboard/summary", {})
    resolved_from = date_from or str(summary.get("price_date_from") or "")
    resolved_to = date_to or str(summary.get("price_date_to") or "")
    if not resolved_from or not resolved_to:
        raise ValueError("date range could not be inferred from API summary")
    return resolved_from, resolved_to


def _iter_paged_batches(api_get: JsonGetter, path: str, params: Dict[str, object], limit: int) -> Iterable[List[Dict[str, object]]]:
    offset = 0
    while True:
        payload = api_get(path, {**params, "limit": limit, "offset": offset})
        if not isinstance(payload, dict):
            raise ValueError(f"expected paged payload from {path}")
        items = payload.get("items") or []
        if items:
            yield items
        if not items or not payload.get("has_more"):
            break
        offset += len(items)


def export_backtest_dataset(
    base_url: str,
    output_dir: str,
    date_from: str = "",
    date_to: str = "",
    instrument_page_size: int = 200,
    series_page_size: int = 1000,
    fail_on_incomplete_factors: bool = True,
    api_get: Optional[JsonGetter] = None,
) -> Dict[str, object]:
    if instrument_page_size <= 0 or series_page_size <= 0:
        raise ValueError("page sizes must be positive")

    client = HttpApiClient(base_url) if api_get is None else None
    get_json = api_get or client.get_json
    resolved_from, resolved_to = _resolve_date_window(get_json, date_from, date_to)
    coverage = get_json(
        "/api/v1/adjustments/coverage",
        {"date_from": resolved_from, "date_to": resolved_to, "as_of_date": "9999-12-31"},
    )
    if not isinstance(coverage, dict):
        raise ValueError("expected dict payload from /api/v1/adjustments/coverage")
    if fail_on_incomplete_factors and not coverage.get("is_complete"):
        raise ValueError(
            "adjustment factors are incomplete for "
            f"{resolved_from}..{resolved_to}: daily_rows={coverage.get('daily_rows')} "
            f"factor_rows={coverage.get('factor_rows')}"
        )

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    instrument_writer = ParquetTableWriter(output_path / "instruments.parquet", INSTRUMENT_SCHEMA)
    instrument_daily_writer = ParquetTableWriter(output_path / "instrument_daily.parquet", INSTRUMENT_DAILY_SCHEMA)
    benchmark_writer = ParquetTableWriter(output_path / "benchmarks.parquet", BENCHMARK_SCHEMA)
    benchmark_daily_writer = ParquetTableWriter(output_path / "benchmark_daily.parquet", BENCHMARK_DAILY_SCHEMA)
    calendar_writer = ParquetTableWriter(output_path / "trading_calendar.parquet", CALENDAR_SCHEMA)

    try:
        instruments: List[Dict[str, object]] = []
        market_codes = set()
        for batch in _iter_paged_batches(get_json, "/api/v1/instruments", {}, min(instrument_page_size, 200)):
            instruments.extend(batch)
            instrument_writer.write_rows(batch)
            for row in batch:
                market_code = str(row.get("market_code") or "").strip().upper()
                if market_code:
                    market_codes.add(market_code)

        for instrument in instruments:
            code = str(instrument.get("external_code") or "").strip()
            if not code:
                continue
            for batch in _iter_paged_batches(
                get_json,
                f"/api/v1/instruments/{code}/daily",
                {"date_from": resolved_from, "date_to": resolved_to},
                min(series_page_size, 2000),
            ):
                instrument_daily_writer.write_rows(batch)

        benchmarks = get_json("/api/v1/benchmarks", {})
        if not isinstance(benchmarks, list):
            raise ValueError("expected list payload from /api/v1/benchmarks")
        benchmark_writer.write_rows(benchmarks)
        for benchmark in benchmarks:
            index_code = str(benchmark.get("index_code") or "").strip().upper()
            index_name = str(benchmark.get("index_name") or "").strip()
            if not index_code:
                continue
            for batch in _iter_paged_batches(
                get_json,
                f"/api/v1/benchmarks/{index_code}/daily",
                {"series_name": index_name, "date_from": resolved_from, "date_to": resolved_to},
                min(series_page_size, 2000),
            ):
                benchmark_daily_writer.write_rows(batch)

        if market_codes:
            calendar_rows = get_json(
                "/api/v1/calendar",
                {
                    "market_codes": ",".join(sorted(market_codes)),
                    "date_from": resolved_from,
                    "date_to": resolved_to,
                },
            )
            if not isinstance(calendar_rows, list):
                raise ValueError("expected list payload from /api/v1/calendar")
            calendar_writer.write_rows(calendar_rows)

        manifest = {
            "exported_at": _utc_now_iso(),
            "base_url": base_url,
            "date_from": resolved_from,
            "date_to": resolved_to,
            "counts": {
                "instruments": instrument_writer.row_count,
                "instrument_daily": instrument_daily_writer.row_count,
                "benchmarks": benchmark_writer.row_count,
                "benchmark_daily": benchmark_daily_writer.row_count,
                "trading_calendar": calendar_writer.row_count,
            },
            "adjustment_coverage": coverage,
            "files": {
                "instruments": "instruments.parquet",
                "instrument_daily": "instrument_daily.parquet",
                "benchmarks": "benchmarks.parquet",
                "benchmark_daily": "benchmark_daily.parquet",
                "trading_calendar": "trading_calendar.parquet",
            },
        }
        (output_path / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return manifest
    finally:
        instrument_writer.close()
        instrument_daily_writer.close()
        benchmark_writer.close()
        benchmark_daily_writer.close()
        calendar_writer.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export all instruments and benchmarks through the public API into Parquet datasets")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--date-from", default="")
    parser.add_argument("--date-to", default="")
    parser.add_argument("--instrument-page-size", type=int, default=200)
    parser.add_argument("--series-page-size", type=int, default=1000)
    parser.add_argument("--allow-incomplete-factors", action="store_true")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    manifest = export_backtest_dataset(
        base_url=args.base_url,
        output_dir=args.output_dir,
        date_from=args.date_from,
        date_to=args.date_to,
        instrument_page_size=args.instrument_page_size,
        series_page_size=args.series_page_size,
        fail_on_incomplete_factors=not args.allow_incomplete_factors,
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
