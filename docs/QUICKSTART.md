# Quick Start Guide

## 🚀 5분만에 시작하기

### 1. 서버 실행
```bash
# FastAPI 서버 시작
cd /c/workspace/financial_data_collector
uv run uvicorn financial_data_collector.server:app --host 0.0.0.0 --port 8000

# 브라우저에서 열기:
# http://localhost:8000          - API 정보
# http://localhost:8000/docs     - Swagger UI (대화형 API 문서)
# http://localhost:8000/redoc    - ReDoc (읽기 쉬운 API 문서)
```

### 2. API 테스트 (Swagger UI 사용)

1. 브라우저에서 http://localhost:8000/docs 열기
2. `POST /api/v1/backtest/exports` 클릭
3. "Try it out" 버튼 클릭
4. Request body 입력:
```json
{
  "market_code": "KOSDAQ",
  "index_codes": ["KOSDAQ", "KOSPI"],
  "date_from": "2026-01-02",
  "date_to": "2026-01-03",
  "include_issues": false,
  "output_format": "parquet",
  "output_path": "data/backtest_export_test"
}
```
5. "Execute" 클릭
6. Response에서 `job_id` 복사
7. `GET /api/v1/backtest/exports/{job_id}`로 상태 확인

### 3. curl로 테스트
```bash
# 1. Export job 생성
JOB_ID=$(curl -X POST http://localhost:8000/api/v1/backtest/exports \
  -H "Content-Type: application/json" \
  -d '{
    "market_code": "KOSDAQ",
    "index_codes": ["KOSDAQ", "KOSPI"],
    "date_from": "2026-01-02",
    "date_to": "2026-01-03",
    "include_issues": false,
    "output_format": "parquet",
    "output_path": "data/backtest_export_test"
  }' | jq -r '.job_id')

echo "Job ID: $JOB_ID"

# 2. Job 상태 확인
curl http://localhost:8000/api/v1/backtest/exports/$JOB_ID | jq

# 3. Manifest 확인 (완료 후)
curl http://localhost:8000/api/v1/backtest/exports/$JOB_ID/manifest | jq
```

### 4. Python으로 사용
```python
import requests
import time

# Export 요청
response = requests.post("http://localhost:8000/api/v1/backtest/exports", json={
    "market_code": "KOSDAQ",
    "index_codes": ["KOSDAQ", "KOSPI"],
    "date_from": "2026-01-02",
    "date_to": "2026-01-03",
    "include_issues": False,
    "output_format": "parquet",
    "output_path": "data/my_backtest"
})

job_id = response.json()["job_id"]
print(f"Job created: {job_id}")

# Poll until complete
while True:
    status = requests.get(f"http://localhost:8000/api/v1/backtest/exports/{job_id}").json()

    if status["status"] == "SUCCEEDED":
        print(f"✅ Export completed!")
        print(f"Files: {status['files']}")
        print(f"Row counts: {status['row_counts']}")
        break
    elif status["status"] == "FAILED":
        print(f"❌ Export failed: {status.get('error_message')}")
        break
    else:
        print(f"⏳ Status: {status['status']}")
        time.sleep(2)

# Read Parquet files
import pyarrow.parquet as pq
df = pq.read_table("data/my_backtest/instrument_daily.parquet").to_pandas()
print(df.head())
```

## 📊 Daily 데이터 수집

### 수동 실행
```bash
# 오늘 데이터 수집
uv run python -m financial_data_collector.collect_krx_data \
  --date-from 2026-01-02 \
  --date-to 2026-01-02

# 디버그 모드
uv run python -m financial_data_collector.collect_krx_data \
  --date-from 2026-01-02 \
  --date-to 2026-01-02 \
  --debug
```

### Cron 설정 (Linux/Mac)
```bash
# crontab -e
0 18 * * * cd /app && uv run python -m financial_data_collector.collect_krx_data --date-from $(date -d "yesterday" +\%Y-\%m-\%d) --date-to $(date -d "yesterday" +\%Y-\%m-\%d) >> /var/log/krx.log 2>&1
```

### Windows 작업 스케줄러
```powershell
# collect_daily.ps1
$yesterday = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd")
cd C:\workspace\financial_data_collector
uv run python -m financial_data_collector.collect_krx_data --date-from $yesterday --date-to $yesterday
```

## 🎯 운영 시나리오

### 시나리오 1: 백테스트 준비 (가장 일반적)

```bash
# 1. 데이터 수집 (한 번만)
uv run python -m financial_data_collector.collect_krx_data \
  --date-from 2024-01-01 \
  --date-to 2024-12-31

# 2. 서버 실행 (상시)
uv run uvicorn financial_data_collector.server:app --host 0.0.0.0 --port 8000

# 3. Parquet export (필요할 때마다)
curl -X POST http://localhost:8000/api/v1/backtest/exports \
  -H "Content-Type: application/json" \
  -d '{
    "market_code": "KOSDAQ",
    "index_codes": ["KOSDAQ", "KOSPI"],
    "date_from": "2024-01-01",
    "date_to": "2024-12-31",
    "output_path": "data/backtest_2024"
  }'
```

### 시나리오 2: Daily 운영

```bash
# Option A: 두 개의 프로세스
# Terminal 1: Daily 수집 (Cron)
# Terminal 2: API 서버
uv run uvicorn financial_data_collector.server:app

# Option B: Docker Compose (권장)
docker-compose up -d

# Option C: Systemd (Linux 프로덕션)
sudo systemctl start krx-collector  # Cron
sudo systemctl start krx-api        # API
```

### 시나리오 3: 대량 백테스트

```python
# 여러 기간의 데이터를 병렬로 export
import requests
from concurrent.futures import ThreadPoolExecutor

periods = [
    ("2024-01-01", "2024-03-31", "q1"),
    ("2024-04-01", "2024-06-30", "q2"),
    ("2024-07-01", "2024-09-30", "q3"),
    ("2024-10-01", "2024-12-31", "q4"),
]

def export_period(date_from, date_to, label):
    response = requests.post("http://localhost:8000/api/v1/backtest/exports", json={
        "market_code": "KOSDAQ",
        "index_codes": ["KOSDAQ", "KOSPI"],
        "date_from": date_from,
        "date_to": date_to,
        "output_path": f"data/backtest_2024_{label}"
    })
    return response.json()["job_id"]

# 병렬 실행
with ThreadPoolExecutor(max_workers=4) as executor:
    jobs = list(executor.map(lambda p: export_period(*p), periods))
    print(f"Created {len(jobs)} export jobs")
```

## 🔍 데이터 확인

### SQLite로 확인
```bash
sqlite3 data/financial_data.db

# 수집된 데이터 요약
SELECT
    COUNT(DISTINCT instrument_id) as instruments,
    COUNT(*) as daily_records,
    MIN(trade_date) as from_date,
    MAX(trade_date) as to_date
FROM daily_market_data;

# 최근 수집 상태
SELECT * FROM collection_runs ORDER BY started_at DESC LIMIT 5;
```

### Parquet 파일 확인
```python
import pyarrow.parquet as pq
import pandas as pd

# 메타데이터 확인
metadata = pq.read_metadata("data/backtest_export/instrument_daily.parquet")
print(f"Rows: {metadata.num_rows}")
print(f"Columns: {metadata.num_columns}")

# 데이터 로드
df = pq.read_table("data/backtest_export/instrument_daily.parquet").to_pandas()
print(df.info())
print(df.describe())

# 샘플 데이터
print(df.head())
```

## 🆘 문제 해결

### 서버가 시작되지 않음
```bash
# Port 8000이 이미 사용 중인지 확인
lsof -i :8000  # Linux/Mac
netstat -ano | findstr :8000  # Windows

# 다른 포트 사용
uv run uvicorn financial_data_collector.server:app --port 8001
```

### Export가 실패함
```bash
# 디스크 공간 확인
df -h  # Linux/Mac
wmic logicaldisk get size,freespace,caption  # Windows

# Output 경로 권한 확인
ls -la data/

# 로그 확인
# 서버 로그에서 상세 에러 메시지 확인
```

### 데이터가 없음
```bash
# DB 파일 확인
ls -lh data/financial_data.db

# 데이터 확인
sqlite3 data/financial_data.db "SELECT COUNT(*) FROM daily_market_data;"

# 데이터 수집 다시 실행
uv run python -m financial_data_collector.collect_krx_data \
  --date-from 2026-01-02 --date-to 2026-01-03
```

## 📚 더 자세한 내용

- [BULK_EXPORT_API_SPEC.md](./BULK_EXPORT_API_SPEC.md) - API 상세 스펙
- [DEPLOYMENT_GUIDE.md](./DEPLOYMENT_GUIDE.md) - 프로덕션 배포 가이드
- [README.md](../README.md) - 프로젝트 개요

## 💡 Tips

1. **Swagger UI 활용**: http://localhost:8000/docs 에서 모든 API를 직접 테스트할 수 있습니다
2. **디버그 모드**: `--debug` 플래그로 상세 로그 확인
3. **작은 기간부터**: 처음에는 1-2일치 데이터로 테스트 후 확장
4. **디스크 공간**: 1년치 데이터 ~10GB 정도 필요 (Parquet 포함)
5. **백업**: SQLite DB 파일을 정기적으로 백업
