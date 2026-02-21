import argparse
import json
import logging
import os
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional
from uuid import UUID, uuid5

from .calendar_builder import TradingCalendarBuilder
from .collectors import BenchmarkCollector, DailyMarketCollector, InstrumentCollector
from .krx_client import KRXClient, KRXClientConfig
from .repository import Repository
from .runs import RunManager
from .settings import KRXSettings, load_dotenv
from .validation import ValidationJob

logger = logging.getLogger(__name__)
INSTRUMENT_UUID_NAMESPACE = UUID("0d9a6af7-e603-4c9d-8ca6-e7f6af20d9e0")


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _date_range(date_from: date, date_to: date) -> Iterable[date]:
    current = date_from
    while current <= date_to:
        yield current
        current += timedelta(days=1)


def _first_not_none(row: Dict[str, Any], keys: List[str]) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _normalize_date_str(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    # Handle datetime objects directly
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raw = str(value).strip()
    if len(raw) == 8 and raw.isdigit():
        return datetime.strptime(raw, "%Y%m%d").date().isoformat()
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError:
        return None


def _parse_number(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value in (None, ""):
        return default
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return default


def _raw_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _normalize_instrument_code(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None

    raw = str(value).strip().upper()
    if not raw:
        return None

    prefixed_match = re.fullmatch(r"A(\d{6})", raw)
    if prefixed_match:
        return prefixed_match.group(1)

    if re.fullmatch(r"\d+(\.0+)?", raw):
        raw = str(int(float(raw)))

    if raw.isdigit():
        if len(raw) > 6:
            return None
        return raw.zfill(6)

    if re.fullmatch(r"[A-Z0-9]{6}", raw):
        return raw

    return None


def _extract_rows(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        logger.debug("_extract_rows: payload is None")
        return []
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        logger.debug(f"_extract_rows: payload keys: {list(payload.keys())}")
        # Add OutBlock_1, OutBlock_2 to handle KRX API responses
        candidate_keys = ["OutBlock_1", "OutBlock_2", "output", "outputs", "result", "results", "items", "data", "list"]
        for key in candidate_keys:
            value = payload.get(key)
            if isinstance(value, list):
                logger.debug(f"_extract_rows: found data in key '{key}', {len(value)} rows")
                return [r for r in value if isinstance(r, dict)]
            if isinstance(value, dict):
                for nested in value.values():
                    if isinstance(nested, list):
                        return [r for r in nested if isinstance(r, dict)]

        # Fallback: check all dict values for lists
        for key, value in payload.items():
            if isinstance(value, list) and value and len(value) > 0:
                if isinstance(value[0], dict):
                    logger.debug(f"_extract_rows: found data in fallback key '{key}', {len(value)} rows")
                    return [r for r in value if isinstance(r, dict)]

        # Only return the payload itself if it has values and none are dict/list
        if payload and all(not isinstance(v, (dict, list)) for v in payload.values()):
            return [payload]
    return []


def _instrument_uuid(market_code: str, external_code: str) -> str:
    return str(uuid5(INSTRUMENT_UUID_NAMESPACE, f"{market_code.upper()}:{external_code.upper()}"))


def _normalize_instruments(rows: List[Dict[str, Any]], market_code: str) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        instrument_code_raw = _first_not_none(
            row,
            [
                "instrument_id",
                "external_code",
                "isu_srt_cd",
                "ISU_SRT_CD",
                "short_code",
                "symbol",
            ],
        )
        instrument_code = _normalize_instrument_code(instrument_code_raw)

        listing_date = _normalize_date_str(
            _first_not_none(row, ["listing_date", "list_date", "LIST_DD", "list_dd", "bas_dt"])
        )
        if not instrument_code or not listing_date:
            continue

        # Parse listed_shares with proper handling
        listed_shares_raw = _parse_number(_first_not_none(row, ["listed_shares", "LIST_SHRS", "list_shrs"]), None)
        listed_shares = int(listed_shares_raw) if listed_shares_raw and listed_shares_raw > 0 else None

        # Use ISU_NM for instrument_name (full name), not ISU_ABBRV (abbreviation)
        instrument_name = str(
            _first_not_none(row, ["instrument_name", "ISU_NM", "isu_nm", "ISU_ABBRV", "isu_abbrv", "name"])
            or instrument_code
        )

        normalized.append(
            {
                "instrument_id": _instrument_uuid(market_code, instrument_code),
                "external_code": instrument_code,
                "market_code": market_code,
                "instrument_name": instrument_name,
                "instrument_name_abbr": str(_first_not_none(row, ["instrument_name_abbr", "ISU_ABBRV", "isu_abbrv"]) or ""),
                "instrument_name_eng": str(_first_not_none(row, ["instrument_name_eng", "ISU_ENG_NM", "isu_eng_nm"]) or ""),
                "listing_date": listing_date,
                "delisting_date": _normalize_date_str(_first_not_none(row, ["delisting_date", "delist_date", "DELIST_DD", "delist_dd"])),
                "listed_shares": listed_shares,
                "security_group": str(_first_not_none(row, ["security_group", "SECUGRP_NM", "secugrp_nm"]) or ""),
                "sector_name": str(_first_not_none(row, ["sector_name", "SECT_TP_NM", "sect_tp_nm"]) or ""),
                "stock_type": str(_first_not_none(row, ["stock_type", "KIND_STKCERT_TP_NM", "kind_stkcert_tp_nm"]) or ""),
                "par_value": _parse_number(_first_not_none(row, ["par_value", "PARVAL", "parval"]), None),
            }
        )
    return normalized


def _normalize_daily_market(rows: List[Dict[str, Any]], market_code: str, trade_date: date) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        instrument_id_raw = _first_not_none(
            row,
            [
                "instrument_id",
                "ISU_CD",
                "isu_cd",
                "isu_srt_cd",
                "ISU_SRT_CD",
                "external_code",
                "symbol",
            ],
        )
        external_code = _normalize_instrument_code(instrument_id_raw)
        if not external_code:
            continue

        open_price = _parse_number(_first_not_none(row, ["open", "TDD_OPNPRC", "tdd_opnprc", "stck_oprc"]))
        high_price = _parse_number(_first_not_none(row, ["high", "TDD_HGPRC", "tdd_hgprc", "stck_hgpr"]))
        low_price = _parse_number(_first_not_none(row, ["low", "TDD_LWPRC", "tdd_lwprc", "stck_lwpr"]))
        close_price = _parse_number(_first_not_none(row, ["close", "TDD_CLSPRC", "tdd_clsprc", "stck_clpr"]))
        volume = _parse_number(_first_not_none(row, ["volume", "ACC_TRDVOL", "acc_trdvol", "acml_vol"]), 0)
        turnover_value = _parse_number(_first_not_none(row, ["turnover_value", "ACC_TRDVAL", "acc_trdval", "acml_tr_pbmn"]))
        market_value = _parse_number(_first_not_none(row, ["market_value", "MKTCAP", "mktcap", "lstg_stcnt"]))
        if None in (open_price, high_price, low_price, close_price):
            continue

        is_trade_halted = False
        if close_price != 0 and open_price == 0 and high_price == 0 and low_price == 0:
            is_trade_halted = True
            open_price = close_price
            high_price = close_price
            low_price = close_price
        elif int(volume or 0) == 0 and open_price == high_price == low_price == close_price:
            is_trade_halted = True
        elif int(volume or 0) == 0 and close_price != 0:
            if high_price == 0 or low_price == 0:
                is_trade_halted = True
                if open_price == 0:
                    open_price = close_price
                if high_price == 0:
                    high_price = max(open_price, close_price)
                if low_price == 0:
                    low_price = min(open_price, close_price)

        # Parse listed_shares with proper handling
        listed_shares_raw = _parse_number(_first_not_none(row, ["listed_shares", "LIST_SHRS", "list_shrs"]), None)
        listed_shares = int(listed_shares_raw) if listed_shares_raw and listed_shares_raw > 0 else None

        normalized.append(
            {
                "instrument_id": _instrument_uuid(market_code, external_code),
                "external_code": external_code,
                "market_code": market_code.upper(),
                "trade_date": trade_date.isoformat(),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": int(volume or 0),
                "turnover_value": turnover_value,
                "market_value": market_value,
                "price_change": _parse_number(_first_not_none(row, ["price_change", "CMPPREVDD_PRC", "cmpprevdd_prc"]), None),
                "change_rate": _parse_number(_first_not_none(row, ["change_rate", "FLUC_RT", "fluc_rt"]), None),
                "listed_shares": listed_shares,
                "is_trade_halted": is_trade_halted,
            }
        )
    return normalized


def _normalize_benchmark(rows: List[Dict[str, Any]], index_code: str, trade_date: date) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        raw_open = _raw_text(_first_not_none(row, ["open", "OPNPRC_IDX", "OPNPRC", "opnprc", "TDD_OPNPRC", "tdd_opnprc"]))
        raw_high = _raw_text(_first_not_none(row, ["high", "HGPRC_IDX", "HGPRC", "hgprc", "TDD_HGPRC", "tdd_hgprc"]))
        raw_low = _raw_text(_first_not_none(row, ["low", "LWPRC_IDX", "LWPRC", "lwprc", "TDD_LWPRC", "tdd_lwprc"]))
        raw_close = _raw_text(_first_not_none(row, ["close", "CLSPRC_IDX", "CLSPRC", "clsprc", "TDD_CLSPRC", "tdd_clsprc"]))

        open_price = _parse_number(raw_open)
        high_price = _parse_number(raw_high)
        low_price = _parse_number(raw_low)
        close_price = _parse_number(raw_close)
        if close_price is None:
            continue

        status = "VALID"
        if None in (open_price, high_price, low_price):
            status = "PARTIAL"

        # Parse volume with proper handling
        volume_raw = _parse_number(_first_not_none(row, ["volume", "ACC_TRDVOL", "acc_trdvol"]), None)
        volume = int(volume_raw) if volume_raw and volume_raw > 0 else None

        normalized.append(
            {
                "index_code": index_code.upper(),
                "trade_date": trade_date.isoformat(),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "raw_open": raw_open,
                "raw_high": raw_high,
                "raw_low": raw_low,
                "raw_close": raw_close,
                "volume": volume,
                "turnover_value": _parse_number(_first_not_none(row, ["turnover_value", "ACC_TRDVAL", "acc_trdval"]), None),
                "market_cap": _parse_number(_first_not_none(row, ["market_cap", "MKTCAP", "mktcap"]), None),
                "price_change": _parse_number(_first_not_none(row, ["price_change", "CMPPREVDD_IDX", "cmpprevdd_idx"]), None),
                "change_rate": _parse_number(_first_not_none(row, ["change_rate", "FLUC_RT", "fluc_rt"]), None),
                "index_name": str(_first_not_none(row, ["index_name", "IDX_NM", "idx_nm"]) or index_code).strip() or index_code,
                "record_status": status,
            }
        )
    return normalized


def run_collection(
    database_url: str,
    market_code: str,
    index_code: str,
    date_from: date,
    date_to: date,
    source_name: str = "krx",
) -> Dict[str, Any]:
    load_dotenv(".env")
    settings = KRXSettings.from_env()
    settings.validate()

    repo = Repository(database_url)
    repo.init_schema()

    client = KRXClient(KRXClientConfig.from_settings(settings))
    run_manager = RunManager(repo)
    run_id = run_manager.start(
        f"phase1-collect-{market_code.upper()}",
        source_name,
        date_from.isoformat(),
        date_to.isoformat(),
    )

    logger.info(f"Starting collection for {market_code}/{index_code} from {date_from} to {date_to}")

    instrument_collector = InstrumentCollector(repo)
    daily_collector = DailyMarketCollector(repo)
    benchmark_collector = BenchmarkCollector(repo)
    calendar_builder = TradingCalendarBuilder(repo)
    validation_job = ValidationJob(repo)

    instrument_count = 0
    daily_count = 0
    benchmark_count = 0

    try:
        instruments_payload = client.get_instruments(market_code, date_to)
        extracted_instruments = _extract_rows(instruments_payload)
        logger.info(f"Fetched instruments: {len(extracted_instruments)} raw rows")

        instrument_rows = _normalize_instruments(extracted_instruments, market_code)
        logger.info(f"Normalized instruments: {len(instrument_rows)} rows")

        instrument_count = instrument_collector.collect(instrument_rows, source_name)
        logger.info(f"Collected {instrument_count} instruments to database")

        index_days: List[date] = []
        for trade_day in _date_range(date_from, date_to):
            daily_payload = client.get_daily_market(market_code, trade_day)
            benchmark_payload = client.get_index_daily(index_code, trade_day)

            extracted_daily = _extract_rows(daily_payload)
            extracted_benchmark = _extract_rows(benchmark_payload)

            normalized_daily = _normalize_daily_market(extracted_daily, market_code, trade_day)
            normalized_benchmark = _normalize_benchmark(extracted_benchmark, index_code, trade_day)

            daily_day_count = daily_collector.collect(normalized_daily, source_name, run_id)
            benchmark_day_count = benchmark_collector.collect(normalized_benchmark, source_name, run_id)

            logger.info(f"Processing {trade_day}: extracted {len(extracted_daily)} daily/{len(extracted_benchmark)} benchmark, "
                       f"normalized {len(normalized_daily)} daily/{len(normalized_benchmark)} benchmark, "
                       f"collected {daily_day_count} daily/{benchmark_day_count} benchmark records")

            daily_count += daily_day_count
            benchmark_count += benchmark_day_count
            if normalized_benchmark:
                index_days.append(trade_day)

        calendar_count = calendar_builder.build_from_index_days(
            market_code=market_code.upper(),
            date_from=date_from,
            date_to=date_to,
            index_trade_dates=index_days,
            source_name=source_name,
            run_id=run_id,
        )
        validation = validation_job.validate_range(market_code.upper(), date_from.isoformat(), date_to.isoformat(), run_id)
        run_manager.finish(
            run_id=run_id,
            success_count=instrument_count + daily_count + benchmark_count + calendar_count,
            failure_count=validation["errors"],
            warning_count=validation["warnings"],
        )
    except Exception:
        run_manager.fail(run_id)
        raise

    return {
        "run_id": run_id,
        "counts": {
            "instruments": instrument_count,
            "daily_market": daily_count,
            "benchmark": benchmark_count,
        },
    }


def run_collection_multi(
    database_url: str,
    market_codes: List[str],
    index_codes: Optional[List[str]],
    date_from: date,
    date_to: date,
    source_name: str = "krx",
) -> Dict[str, Any]:
    markets = [m.strip().upper() for m in market_codes if m.strip()]
    if not markets:
        raise ValueError("at least one market code is required")

    if index_codes:
        indices = [i.strip().upper() for i in index_codes if i.strip()]
    else:
        indices = markets

    if len(indices) == 1 and len(markets) > 1:
        indices = indices * len(markets)

    if len(indices) != len(markets):
        raise ValueError("index codes count must match market codes count (or provide one shared index code)")

    results = []
    for market_code, index_code in zip(markets, indices):
        results.append(
            run_collection(
                database_url=database_url,
                market_code=market_code,
                index_code=index_code,
                date_from=date_from,
                date_to=date_to,
                source_name=source_name,
            )
        )

    return {"markets": results}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect KRX market data and store in PostgreSQL.")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--market-code", default="", help="Single market code override (e.g. KOSDAQ)")
    parser.add_argument("--index-code", default="", help="Single index code override (defaults to market code)")
    parser.add_argument("--market-codes", default="KOSDAQ,KOSPI", help="Comma-separated market codes (default: KOSDAQ,KOSPI)")
    parser.add_argument(
        "--index-codes",
        default="",
        help="Comma-separated index codes mapped by order to --market-codes. If omitted, market codes are reused.",
    )
    parser.add_argument("--date-from", required=True, help="YYYY-MM-DD")
    parser.add_argument("--date-to", required=True, help="YYYY-MM-DD")
    parser.add_argument("--source-name", default="krx")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    # Configure logging based on debug flag
    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    date_from = _parse_date(args.date_from)
    date_to = _parse_date(args.date_to)
    if date_from > date_to:
        raise ValueError("date-from must be <= date-to")
    if not args.database_url:
        raise ValueError("--database-url or DATABASE_URL is required")
    if args.market_code.strip():
        market_code = args.market_code.strip().upper()
        index_code = (args.index_code.strip() or market_code).upper()
        result = run_collection(
            database_url=args.database_url,
            market_code=market_code,
            index_code=index_code,
            date_from=date_from,
            date_to=date_to,
            source_name=args.source_name,
        )
    else:
        result = run_collection_multi(
            database_url=args.database_url,
            market_codes=args.market_codes.split(","),
            index_codes=args.index_codes.split(",") if args.index_codes.strip() else None,
            date_from=date_from,
            date_to=date_to,
            source_name=args.source_name,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
