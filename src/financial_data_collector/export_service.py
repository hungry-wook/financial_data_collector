import shutil
from collections import defaultdict
from collections import deque
from dataclasses import asdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional
from uuid import uuid4

from .adjustment_service import AdjustmentService
from .parquet_writer import ParquetWriter
from .repository import Repository

STRICT_ADJUSTMENT_EVENT_TYPES = (
    "BONUS_ISSUE",
    "CAPITAL_REDUCTION",
    "SPLIT",
    "SPLIT_MERGER",
    "MERGER",
    "STOCK_SWAP",
    "STOCK_TRANSFER",
)


class ManifestUnavailableError(RuntimeError):
    pass


class AdjustedExportCoverageError(RuntimeError):
    pass


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class ExportRequest:
    market_codes: List[str]
    index_codes: List[str]
    date_from: str
    date_to: str
    include_issues: bool
    output_format: str
    output_path: str
    series_names: Optional[List[str]] = None
    series_type: str = "raw"
    as_of_timestamp: Optional[str] = None


class ExportService:
    STREAM_FETCH_SIZE = 10000

    def __init__(self, repo: Repository, writer: Optional[ParquetWriter] = None, adjustment_service: Optional[AdjustmentService] = None):
        self.repo = repo
        self.writer = writer or ParquetWriter()
        self.adjustment_service = adjustment_service or AdjustmentService(repo)

    def create_job(self, req: ExportRequest) -> Dict:
        self._validate_request(req)
        job_id = str(uuid4())
        submitted_at = _utc_now_iso()
        self.repo.insert_export_job(
            {
                "job_id": job_id,
                "status": "PENDING",
                "progress": 0,
                "submitted_at": submitted_at,
                "request_payload": asdict(req),
            }
        )
        return {"job_id": job_id, "status": "PENDING", "submitted_at": submitted_at}

    def run_job(self, job_id: str) -> Dict:
        job = self._get_job(job_id)
        req = ExportRequest(**job["request_payload"])
        self.repo.update_export_job(
            job_id,
            {
                "status": "RUNNING",
                "started_at": _utc_now_iso(),
            },
        )

        final_path = Path(req.output_path)
        temp_path = final_path.parent / f".tmp_{job_id}"
        if temp_path.exists():
            shutil.rmtree(temp_path)
        temp_path.mkdir(parents=True, exist_ok=True)

        try:
            self._assert_adjusted_factor_coverage(req)
            instrument_rows = self._decorate_instrument_rows(
                req,
                self.repo.stream_core_market(
                    req.market_codes,
                    req.date_from,
                    req.date_to,
                    series_type=req.series_type,
                    as_of_timestamp=req.as_of_timestamp,
                    fetch_size=self.STREAM_FETCH_SIZE,
                ),
            )
            benchmark_rows = self.repo.stream_benchmark(
                req.index_codes,
                req.date_from,
                req.date_to,
                series_names=req.series_names,
                fetch_size=self.STREAM_FETCH_SIZE,
            )
            calendar_rows = self.repo.stream_calendar(
                req.market_codes,
                req.date_from,
                req.date_to,
                fetch_size=self.STREAM_FETCH_SIZE,
            )
            issue_rows = (
                self.repo.stream_issues(req.date_from, req.date_to, fetch_size=self.STREAM_FETCH_SIZE)
                if req.include_issues
                else ()
            )
            self.repo.update_export_job(job_id, {"progress": 60})

            files = []
            row_counts = {}
            files.append("instrument_daily.parquet")
            row_counts["instrument_daily"] = self.writer.write(temp_path / "instrument_daily.parquet", instrument_rows)
            files.append("benchmark_daily.parquet")
            row_counts["benchmark_daily"] = self.writer.write(temp_path / "benchmark_daily.parquet", benchmark_rows)
            files.append("trading_calendar.parquet")
            row_counts["trading_calendar"] = self.writer.write(temp_path / "trading_calendar.parquet", calendar_rows)
            if req.include_issues:
                files.append("data_quality_issues.parquet")
                row_counts["data_quality_issues"] = self.writer.write(temp_path / "data_quality_issues.parquet", issue_rows)

            manifest = {
                "job_id": job_id,
                "market_codes": req.market_codes,
                "index_codes": req.index_codes,
                "series_names": req.series_names or [],
                "series_type": req.series_type,
                "as_of_timestamp": req.as_of_timestamp,
                "date_from": req.date_from,
                "date_to": req.date_to,
                "schema_version": "phase1-v1",
                "generated_at": _utc_now_iso(),
                "files": [],
            }
            for f in files:
                manifest["files"].append(
                    {
                        "name": f,
                        "rows": row_counts[f.replace(".parquet", "")],
                        "sha256": self.writer.sha256(temp_path / f),
                    }
                )
            self.writer.write_manifest(temp_path / "manifest.json", manifest)
            files.append("manifest.json")

            if final_path.exists():
                shutil.rmtree(final_path)
            temp_path.replace(final_path)

            self.repo.update_export_job(
                job_id,
                {
                    "status": "SUCCEEDED",
                    "progress": 100,
                    "finished_at": _utc_now_iso(),
                    "output_path": final_path.as_posix(),
                    "files": files,
                    "row_counts": row_counts,
                    "error_code": None,
                    "error_message": None,
                },
            )
            return self.get_job(job_id)
        except Exception as exc:
            self.repo.update_export_job(
                job_id,
                {
                    "status": "FAILED",
                    "error_code": "EXPORT_FAILED",
                    "error_message": str(exc),
                    "finished_at": _utc_now_iso(),
                },
            )
            if temp_path.exists():
                shutil.rmtree(temp_path)
            return self.get_job(job_id)

    def get_job(self, job_id: str) -> Dict:
        return {k: v for k, v in self._get_job(job_id).items() if k != "request_payload"}

    def get_manifest(self, job_id: str) -> Dict:
        job = self._get_job(job_id)
        if job.get("status") != "SUCCEEDED":
            raise ManifestUnavailableError("Manifest is available only after success")
        output_path = str(job.get("output_path") or "").strip()
        if not output_path:
            raise ManifestUnavailableError(f"Export job {job_id} is missing output_path")
        manifest_path = Path(output_path) / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found for job_id={job_id}")

        import json

        try:
            return json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            print(f"Manifest decode failed for {job_id}: {exc}")
            raise ManifestUnavailableError(f"Manifest is invalid for job_id={job_id}") from exc

    def _get_job(self, job_id: str) -> Dict:
        job = self.repo.get_export_job(job_id)
        if job is None:
            raise KeyError(f"Unknown job_id={job_id}")
        return job

    def _assert_adjusted_factor_coverage(self, req: ExportRequest) -> None:
        if req.series_type not in {"adjusted", "both"}:
            return
        missing = self.repo.get_adjustment_factor_gaps(
            market_codes=req.market_codes,
            date_from=req.date_from,
            date_to=req.date_to,
            as_of_timestamp=req.as_of_timestamp,
            event_types=STRICT_ADJUSTMENT_EVENT_TYPES,
        )
        if not missing:
            return
        preview = ", ".join(
            f"{row['external_code']}:{row['missing_trade_dates']}[{row['first_missing_trade_date']}..{row['last_missing_trade_date']}]"
            for row in missing[:5]
        )
        raise AdjustedExportCoverageError(
            f"adjusted export requires materialized factors for all eligible rows; missing coverage for {len(missing)} instrument(s): {preview}"
        )

    def _decorate_instrument_rows(self, req: ExportRequest, rows: Iterable[Dict]) -> Iterable[Dict]:
        review_events = self.repo.get_adjustment_review_events(
            market_codes=req.market_codes,
            date_from=req.date_from,
            date_to=req.date_to,
        )
        unresolved_dates: Dict[str, set[date]] = defaultdict(set)
        unresolved_types: Dict[str, Dict[date, set[str]]] = defaultdict(lambda: defaultdict(set))
        unresolved_issues: Dict[str, Dict[date, set[str]]] = defaultdict(lambda: defaultdict(set))
        for event in review_events:
            instrument_id = str(event.get("instrument_id") or "")
            event_date = str(event.get("event_date") or "")
            if not instrument_id or not event_date:
                continue
            parsed_event_date = date.fromisoformat(event_date)
            unresolved_dates[instrument_id].add(parsed_event_date)
            event_type = str(event.get("event_type") or "").strip()
            if event_type:
                unresolved_types[instrument_id][parsed_event_date].add(event_type)
            activation_issue = str(event.get("activation_issue") or "").strip()
            if activation_issue:
                unresolved_issues[instrument_id][parsed_event_date].add(activation_issue)

        prior_special_rows = self.repo.get_recent_special_trading_markers(
            market_codes=req.market_codes,
            date_before=req.date_from,
            lookback_rows=5,
            lookback_days=10,
        )
        recent_special_by_instrument: Dict[str, Deque[bool]] = defaultdict(lambda: deque(maxlen=5))
        for prior_row in prior_special_rows:
            instrument_id = str(prior_row.get("instrument_id") or "")
            if not instrument_id:
                continue
            recent_special_by_instrument[instrument_id].append(bool(prior_row.get("is_special")))

        for row in rows:
            instrument_id = str(row.get("instrument_id") or "")
            trade_date = date.fromisoformat(str(row["trade_date"]))
            recent_markers = recent_special_by_instrument[instrument_id]
            has_recent_halt_or_zero_volume = any(recent_markers)
            current_volume = row.get("volume")
            current_volume_value = float(current_volume or 0)
            current_special = bool(row.get("is_trade_halted")) or current_volume_value <= 0
            event_dates = unresolved_dates.get(instrument_id, set())
            nearby_dates = sorted(d for d in event_dates if abs((trade_date - d).days) <= 5)
            has_unresolved_corporate_action = bool(nearby_dates)
            unresolved_event_types = sorted({etype for d in nearby_dates for etype in unresolved_types[instrument_id].get(d, set())})
            unresolved_activation_issues = sorted({issue for d in nearby_dates for issue in unresolved_issues[instrument_id].get(d, set())})
            is_special_trading_regime = current_special or has_recent_halt_or_zero_volume

            reasons = []
            if row.get("record_status") != "VALID":
                reasons.append("invalid_record")
            if bool(row.get("is_under_supervision")):
                reasons.append("under_supervision")
            if current_special:
                reasons.append("current_halt_or_zero_volume")
            elif has_recent_halt_or_zero_volume:
                reasons.append("recent_halt_or_zero_volume")
            if has_unresolved_corporate_action:
                reasons.append("unresolved_corporate_action")

            decorated = dict(row)
            decorated["has_recent_halt_or_zero_volume"] = has_recent_halt_or_zero_volume
            decorated["has_unresolved_corporate_action"] = has_unresolved_corporate_action
            decorated["unresolved_corporate_action_types"] = unresolved_event_types
            decorated["unresolved_corporate_action_issues"] = unresolved_activation_issues
            decorated["is_special_trading_regime"] = is_special_trading_regime
            decorated["is_tradable_for_signal"] = len(reasons) == 0
            decorated["signal_validity_reason"] = "OK" if not reasons else ",".join(reasons)
            yield decorated

            recent_markers.append(current_special)

    @staticmethod
    def _validate_request(req: ExportRequest) -> None:
        try:
            parsed_from = date.fromisoformat(req.date_from)
            parsed_to = date.fromisoformat(req.date_to)
        except ValueError as exc:
            raise ValueError("date_from and date_to must be valid YYYY-MM-DD dates") from exc
        if parsed_from > parsed_to:
            raise ValueError("date_from must be <= date_to")
        if req.output_format != "parquet":
            raise ValueError("output_format must be parquet")
        if not req.market_codes:
            raise ValueError("market_codes is required")
        allowed_markets = {"KOSDAQ", "KOSPI"}
        normalized_markets = [str(m).upper() for m in req.market_codes]
        invalid_markets = [m for m in normalized_markets if m not in allowed_markets]
        if invalid_markets:
            raise ValueError(f"unsupported market_codes: {', '.join(sorted(set(invalid_markets)))}")
        req.market_codes = normalized_markets
        if not req.index_codes:
            raise ValueError("index_codes is required")

        req.series_type = str(req.series_type or "raw").strip().lower()
        if req.series_type not in {"raw", "adjusted", "both"}:
            raise ValueError("series_type must be one of: raw, adjusted, both")

        if req.as_of_timestamp:
            try:
                date.fromisoformat(str(req.as_of_timestamp).split("T", 1)[0])
            except ValueError as exc:
                raise ValueError("as_of_timestamp must be ISO date or datetime") from exc
