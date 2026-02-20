from datetime import datetime, timezone
from uuid import uuid4

from .repository import Repository


class RunManager:
    def __init__(self, repo: Repository):
        self.repo = repo

    def start(self, pipeline_name: str, source_name: str, window_start: str, window_end: str) -> str:
        run_id = str(uuid4())
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        self.repo.insert_run(
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "source_name": source_name,
                "window_start": window_start,
                "window_end": window_end,
                "status": "RUNNING",
                "started_at": now,
                "finished_at": None,
                "success_count": 0,
                "failure_count": 0,
                "warning_count": 0,
                "metadata": None,
            }
        )
        return run_id

    def finish(self, run_id: str, success_count: int, failure_count: int, warning_count: int) -> None:
        if failure_count == 0 and warning_count == 0:
            status = "SUCCESS"
        elif failure_count > 0:
            status = "FAILED"
        else:
            status = "PARTIAL"
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        self.repo.update_run(
            run_id,
            {
                "status": status,
                "finished_at": now,
                "success_count": success_count,
                "failure_count": failure_count,
                "warning_count": warning_count,
            },
        )

    def fail(self, run_id: str, failure_count: int = 1) -> None:
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        self.repo.update_run(
            run_id,
            {
                "status": "FAILED",
                "finished_at": now,
                "failure_count": failure_count,
            },
        )
