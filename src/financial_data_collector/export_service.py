import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from uuid import uuid4

from .parquet_writer import ParquetWriter
from .repository import Repository


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class ExportRequest:
    market_code: str
    index_codes: List[str]
    date_from: str
    date_to: str
    include_issues: bool
    output_format: str
    output_path: str
    series_names: Optional[List[str]] = None


class ExportService:
    def __init__(self, repo: Repository, writer: Optional[ParquetWriter] = None):
        self.repo = repo
        self.writer = writer or ParquetWriter()
        self.jobs: Dict[str, Dict] = {}

    def create_job(self, req: ExportRequest) -> Dict:
        self._validate_request(req)
        job_id = str(uuid4())
        self.jobs[job_id] = {
            "job_id": job_id,
            "status": "PENDING",
            "progress": 0,
            "submitted_at": _utc_now_iso(),
            "request": req,
        }
        return {"job_id": job_id, "status": "PENDING", "submitted_at": self.jobs[job_id]["submitted_at"]}

    def run_job(self, job_id: str) -> Dict:
        job = self._get_job(job_id)
        req: ExportRequest = job["request"]
        job["status"] = "RUNNING"
        job["started_at"] = _utc_now_iso()

        final_path = Path(req.output_path)
        temp_path = final_path.parent / f".tmp_{job_id}"
        if temp_path.exists():
            shutil.rmtree(temp_path)
        temp_path.mkdir(parents=True, exist_ok=True)

        try:
            instrument_rows = self.repo.get_core_market(req.market_code, req.date_from, req.date_to)
            benchmark_rows = self.repo.get_benchmark(
                req.index_codes,
                req.date_from,
                req.date_to,
                series_names=req.series_names,
            )
            calendar_rows = self.repo.get_calendar(req.market_code, req.date_from, req.date_to)
            issue_rows = self.repo.get_issues(req.date_from, req.date_to) if req.include_issues else []
            job["progress"] = 60

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
                "market_code": req.market_code,
                "index_codes": req.index_codes,
                "series_names": req.series_names or [],
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

            job.update(
                {
                    "status": "SUCCEEDED",
                    "progress": 100,
                    "finished_at": _utc_now_iso(),
                    "output_path": final_path.as_posix(),
                    "files": files,
                    "row_counts": row_counts,
                }
            )
            return self.get_job(job_id)
        except Exception as exc:
            job.update(
                {
                    "status": "FAILED",
                    "error_code": "EXPORT_FAILED",
                    "error_message": str(exc),
                    "finished_at": _utc_now_iso(),
                }
            )
            if temp_path.exists():
                shutil.rmtree(temp_path)
            return self.get_job(job_id)

    def get_job(self, job_id: str) -> Dict:
        return {k: v for k, v in self._get_job(job_id).items() if k != "request"}

    def get_manifest(self, job_id: str) -> Dict:
        job = self._get_job(job_id)
        if job.get("status") != "SUCCEEDED":
            raise KeyError("Manifest is available only after success")
        manifest_path = Path(job["output_path"]) / "manifest.json"
        import json

        return json.loads(manifest_path.read_text(encoding="utf-8"))

    def _get_job(self, job_id: str) -> Dict:
        if job_id not in self.jobs:
            raise KeyError(f"Unknown job_id={job_id}")
        return self.jobs[job_id]

    @staticmethod
    def _validate_request(req: ExportRequest) -> None:
        if req.date_from > req.date_to:
            raise ValueError("date_from must be <= date_to")
        if req.output_format != "parquet":
            raise ValueError("output_format must be parquet")
        if not req.market_code:
            raise ValueError("market_code is required")
        if not req.index_codes:
            raise ValueError("index_codes is required")
