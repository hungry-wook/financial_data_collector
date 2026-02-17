from typing import Dict, Tuple

from .export_service import ExportRequest, ExportService


class BacktestExportAPI:
    def __init__(self, service: ExportService):
        self.service = service

    def post_exports(self, body: Dict) -> Tuple[int, Dict]:
        try:
            req = ExportRequest(
                market_code=body["market_code"],
                index_codes=body["index_codes"],
                date_from=body["date_from"],
                date_to=body["date_to"],
                include_issues=bool(body.get("include_issues", False)),
                output_format=body.get("output_format", "parquet"),
                output_path=body["output_path"],
            )
            payload = self.service.create_job(req)
            return 202, payload
        except (KeyError, ValueError) as exc:
            return 400, {"error": str(exc)}

    def get_export(self, job_id: str) -> Tuple[int, Dict]:
        try:
            return 200, self.service.get_job(job_id)
        except KeyError as exc:
            return 404, {"error": str(exc)}

    def get_manifest(self, job_id: str) -> Tuple[int, Dict]:
        try:
            return 200, self.service.get_manifest(job_id)
        except KeyError as exc:
            return 404, {"error": str(exc)}

