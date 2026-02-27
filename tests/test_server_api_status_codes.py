import asyncio

import pytest
from fastapi import BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse

from financial_data_collector import server


class _FakeAPI:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self.payload = payload

    def post_exports(self, _body):
        return self.status_code, self.payload


def _request_body(tmp_path):
    return server.ExportRequestBody(
        market_codes=["KOSDAQ"],
        index_codes=["KOSDAQ"],
        date_from="2026-01-01",
        date_to="2026-01-03",
        output_path=(tmp_path / "out").as_posix(),
    )


def test_create_export_returns_status_code_from_api(tmp_path, monkeypatch):
    monkeypatch.setattr(server, "get_api", lambda: _FakeAPI(201, {"job_id": "j1", "status": "PENDING"}))
    tasks = BackgroundTasks()

    response = asyncio.run(server.create_export(_request_body(tmp_path), tasks))

    assert isinstance(response, JSONResponse)
    assert response.status_code == 201
    assert len(tasks.tasks) == 0


def test_create_export_schedules_background_job_only_for_202(tmp_path, monkeypatch):
    monkeypatch.setattr(server, "get_api", lambda: _FakeAPI(202, {"job_id": "j2", "status": "PENDING"}))
    tasks = BackgroundTasks()

    response = asyncio.run(server.create_export(_request_body(tmp_path), tasks))

    assert response.status_code == 202
    assert len(tasks.tasks) == 1


def test_create_export_raises_http_exception_for_error_status(tmp_path, monkeypatch):
    monkeypatch.setattr(server, "get_api", lambda: _FakeAPI(400, {"error": "bad request"}))
    tasks = BackgroundTasks()

    with pytest.raises(HTTPException) as exc:
        asyncio.run(server.create_export(_request_body(tmp_path), tasks))

    assert exc.value.status_code == 400
    assert exc.value.detail == "bad request"
    assert len(tasks.tasks) == 0
