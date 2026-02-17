# Bulk Export API Spec (Backtest Input)

## 1. Purpose
- Generate backtest-ready Parquet files for a date range in one job.
- Avoid repeated per-symbol or per-day REST calls.

## 2. Job Flow
1. Client calls `POST /api/v1/backtest/exports`.
2. Server validates request and creates `job_id`.
3. Worker runs SQL extracts from Phase 1 views/tables.
4. Worker writes Parquet files to output path.
5. Worker writes `manifest.json` and marks job status.
6. Client polls `GET /api/v1/backtest/exports/{job_id}`.

## 3. Endpoints
## 3.1 Create Export Job
`POST /api/v1/backtest/exports`

Request body:
```json
{
  "market_code": "KOSDAQ",
  "index_codes": ["KOSDAQ"],
  "date_from": "2024-01-01",
  "date_to": "2024-12-31",
  "include_issues": true,
  "output_format": "parquet",
  "output_path": "D:/bt_input/run_2024_full"
}
```

Response `202 Accepted`:
```json
{
  "job_id": "2ab1f5e6-2a9f-4a14-9e7a-2d8f1f8ee451",
  "status": "PENDING",
  "submitted_at": "2026-02-17T10:30:00Z"
}
```

Validation rules:
1. `date_from <= date_to`
2. `output_format` in `["parquet"]` for phase 1
3. `market_code` required
4. `index_codes` required (at least one)

## 3.2 Get Job Status
`GET /api/v1/backtest/exports/{job_id}`

Response `200 OK` (running):
```json
{
  "job_id": "2ab1f5e6-2a9f-4a14-9e7a-2d8f1f8ee451",
  "status": "RUNNING",
  "progress": 60,
  "started_at": "2026-02-17T10:30:05Z",
  "updated_at": "2026-02-17T10:31:20Z"
}
```

Response `200 OK` (done):
```json
{
  "job_id": "2ab1f5e6-2a9f-4a14-9e7a-2d8f1f8ee451",
  "status": "SUCCEEDED",
  "submitted_at": "2026-02-17T10:30:00Z",
  "started_at": "2026-02-17T10:30:05Z",
  "finished_at": "2026-02-17T10:32:10Z",
  "output_path": "D:/bt_input/run_2024_full",
  "files": [
    "instrument_daily.parquet",
    "benchmark_daily.parquet",
    "trading_calendar.parquet",
    "data_quality_issues.parquet",
    "manifest.json"
  ],
  "row_counts": {
    "instrument_daily": 820000,
    "benchmark_daily": 250,
    "trading_calendar": 250,
    "data_quality_issues": 132
  }
}
```

## 3.3 Get Manifest
`GET /api/v1/backtest/exports/{job_id}/manifest`

Response `200 OK`:
```json
{
  "job_id": "2ab1f5e6-2a9f-4a14-9e7a-2d8f1f8ee451",
  "market_code": "KOSDAQ",
  "index_codes": ["KOSDAQ"],
  "date_from": "2024-01-01",
  "date_to": "2024-12-31",
  "schema_version": "phase1-v1",
  "generated_at": "2026-02-17T10:32:10Z",
  "files": [
    {
      "name": "instrument_daily.parquet",
      "rows": 820000,
      "sha256": "..."
    },
    {
      "name": "benchmark_daily.parquet",
      "rows": 250,
      "sha256": "..."
    },
    {
      "name": "trading_calendar.parquet",
      "rows": 250,
      "sha256": "..."
    },
    {
      "name": "data_quality_issues.parquet",
      "rows": 132,
      "sha256": "..."
    }
  ]
}
```

## 4. Output Files
Required:
1. `instrument_daily.parquet`
2. `benchmark_daily.parquet`
3. `trading_calendar.parquet`
4. `manifest.json`

Optional:
1. `data_quality_issues.parquet` (`include_issues=true`)

## 5. File Schema
## 5.1 instrument_daily.parquet
Columns:
1. `instrument_id`
2. `external_code`
3. `market_code`
4. `instrument_name`
5. `listing_date`
6. `delisting_date`
7. `trade_date`
8. `open`
9. `high`
10. `low`
11. `close`
12. `volume`
13. `turnover_value`
14. `market_value`
15. `is_trade_halted`
16. `is_under_supervision`
17. `record_status`

Sort order:
1. `trade_date ASC`
2. `instrument_id ASC`

## 5.2 benchmark_daily.parquet
Columns:
1. `index_code`
2. `trade_date`
3. `open`
4. `high`
5. `low`
6. `close`

Sort order:
1. `trade_date ASC`
2. `index_code ASC`

## 5.3 trading_calendar.parquet
Columns:
1. `market_code`
2. `trade_date`
3. `is_open`
4. `holiday_name`

Sort order:
1. `trade_date ASC`

## 5.4 data_quality_issues.parquet
Columns:
1. `dataset_name`
2. `trade_date`
3. `instrument_id`
4. `index_code`
5. `issue_code`
6. `severity`
7. `issue_detail`
8. `run_id`
9. `detected_at`

## 6. SQL Source Contract
1. `instrument_daily.parquet` -> `core_market_dataset_v1`
2. `benchmark_daily.parquet` -> `benchmark_dataset_v1`
3. `trading_calendar.parquet` -> `trading_calendar_v1`
4. `data_quality_issues.parquet` -> `data_quality_issues`

## 7. Failure and Idempotency
1. Partial files must be written to temp path first.
2. Move to final `output_path` only after all files succeed.
3. On failure, job status is `FAILED` with `error_code` and `error_message`.
4. Same request may create a new `job_id`; dedupe is optional in phase 1.

## 8. Recommended Implementation Stack
1. Python + FastAPI (API)
2. SQLAlchemy/psycopg (DB read)
3. Polars or PyArrow (Parquet write)
4. Background worker (RQ/Celery/Arq or built-in task runner)
