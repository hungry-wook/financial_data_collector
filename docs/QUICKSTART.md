# Quick Start Guide

## ?? 5遺꾨쭔???쒖옉?섍린

### 1. ?쒕쾭 ?ㅽ뻾
```bash
# FastAPI ?쒕쾭 ?쒖옉
cd /c/workspace/financial_data_collector
uv run uvicorn financial_data_collector.server:app --host 0.0.0.0 --port 8000

# 釉뚮씪?곗??먯꽌 ?닿린:
# http://localhost:8000          - API ?뺣낫
# http://localhost:8000/docs     - Swagger UI (??뷀삎 API 臾몄꽌)
# http://localhost:8000/redoc    - ReDoc (?쎄린 ?ъ슫 API 臾몄꽌)
```

### 2. API ?뚯뒪??(Swagger UI ?ъ슜)

1. 釉뚮씪?곗??먯꽌 http://localhost:8000/docs ?닿린
2. `POST /api/v1/backtest/exports` ?대┃
3. "Try it out" 踰꾪듉 ?대┃
4. Request body ?낅젰:
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
5. "Execute" ?대┃
6. Response?먯꽌 `job_id` 蹂듭궗
7. `GET /api/v1/backtest/exports/{job_id}`濡??곹깭 ?뺤씤

### 3. curl濡??뚯뒪??```bash
# 1. Export job ?앹꽦
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

# 2. Job ?곹깭 ?뺤씤
curl http://localhost:8000/api/v1/backtest/exports/$JOB_ID | jq

# 3. Manifest ?뺤씤 (?꾨즺 ??
curl http://localhost:8000/api/v1/backtest/exports/$JOB_ID/manifest | jq
```

### 4. Python?쇰줈 ?ъ슜
```python
import requests
import time

# Export ?붿껌
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
        print(f"??Export completed!")
        print(f"Files: {status['files']}")
        print(f"Row counts: {status['row_counts']}")
        break
    elif status["status"] == "FAILED":
        print(f"??Export failed: {status.get('error_message')}")
        break
    else:
        print(f"??Status: {status['status']}")
        time.sleep(2)

# Read Parquet files
import pyarrow.parquet as pq
df = pq.read_table("data/my_backtest/instrument_daily.parquet").to_pandas()
print(df.head())
```

## ?뱤 Daily ?곗씠???섏쭛

### ?섎룞 ?ㅽ뻾
```bash
# ?ㅻ뒛 ?곗씠???섏쭛
uv run python -m financial_data_collector.collect_krx_data \
  --date-from 2026-01-02 \
  --date-to 2026-01-02

# ?붾쾭洹?紐⑤뱶
uv run python -m financial_data_collector.collect_krx_data \
  --date-from 2026-01-02 \
  --date-to 2026-01-02 \
  --debug
```

### Cron ?ㅼ젙 (Linux/Mac)
```bash
# crontab -e
0 18 * * * cd /app && uv run python -m financial_data_collector.collect_krx_data --date-from $(date -d "yesterday" +\%Y-\%m-\%d) --date-to $(date -d "yesterday" +\%Y-\%m-\%d) >> /var/log/krx.log 2>&1
```

### Windows ?묒뾽 ?ㅼ?以꾨윭
```powershell
# collect_daily.ps1
$yesterday = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd")
cd C:\workspace\financial_data_collector
uv run python -m financial_data_collector.collect_krx_data --date-from $yesterday --date-to $yesterday
```

## ?렞 ?댁쁺 ?쒕굹由ъ삤

### ?쒕굹由ъ삤 1: 諛깊뀒?ㅽ듃 以鍮?(媛???쇰컲??

```bash
# 1. ?곗씠???섏쭛 (??踰덈쭔)
uv run python -m financial_data_collector.collect_krx_data \
  --date-from 2024-01-01 \
  --date-to 2024-12-31

# 2. ?쒕쾭 ?ㅽ뻾 (?곸떆)
uv run uvicorn financial_data_collector.server:app --host 0.0.0.0 --port 8000

# 3. Parquet export (?꾩슂???뚮쭏??
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

### ?쒕굹由ъ삤 2: Daily ?댁쁺

```bash
# Option A: ??媛쒖쓽 ?꾨줈?몄뒪
# Terminal 1: Daily ?섏쭛 (Cron)
# Terminal 2: API ?쒕쾭
uv run uvicorn financial_data_collector.server:app

# Option B: Docker Compose (沅뚯옣)
docker-compose up -d

# Option C: Systemd (Linux ?꾨줈?뺤뀡)
sudo systemctl start krx-collector  # Cron
sudo systemctl start krx-api        # API
```

### ?쒕굹由ъ삤 3: ???諛깊뀒?ㅽ듃

```python
# ?щ윭 湲곌컙???곗씠?곕? 蹂묐젹濡?export
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

# 蹂묐젹 ?ㅽ뻾
with ThreadPoolExecutor(max_workers=4) as executor:
    jobs = list(executor.map(lambda p: export_period(*p), periods))
    print(f"Created {len(jobs)} export jobs")
```

## ?뵇 ?곗씠???뺤씤

### SQLite濡??뺤씤
```bash
psql "$DATABASE_URL"

# ?섏쭛???곗씠???붿빟
SELECT
    COUNT(DISTINCT instrument_id) as instruments,
    COUNT(*) as daily_records,
    MIN(trade_date) as from_date,
    MAX(trade_date) as to_date
FROM daily_market_data;

# 理쒓렐 ?섏쭛 ?곹깭
SELECT * FROM collection_runs ORDER BY started_at DESC LIMIT 5;
```

### Parquet ?뚯씪 ?뺤씤
```python
import pyarrow.parquet as pq
import pandas as pd

# 硫뷀??곗씠???뺤씤
metadata = pq.read_metadata("data/backtest_export/instrument_daily.parquet")
print(f"Rows: {metadata.num_rows}")
print(f"Columns: {metadata.num_columns}")

# ?곗씠??濡쒕뱶
df = pq.read_table("data/backtest_export/instrument_daily.parquet").to_pandas()
print(df.info())
print(df.describe())

# ?섑뵆 ?곗씠??print(df.head())
```

## ?넊 臾몄젣 ?닿껐

### ?쒕쾭媛 ?쒖옉?섏? ?딆쓬
```bash
# Port 8000???대? ?ъ슜 以묒씤吏 ?뺤씤
lsof -i :8000  # Linux/Mac
netstat -ano | findstr :8000  # Windows

# ?ㅻⅨ ?ы듃 ?ъ슜
uv run uvicorn financial_data_collector.server:app --port 8001
```

### Export媛 ?ㅽ뙣??```bash
# ?붿뒪??怨듦컙 ?뺤씤
df -h  # Linux/Mac
wmic logicaldisk get size,freespace,caption  # Windows

# Output 寃쎈줈 沅뚰븳 ?뺤씤
ls -la data/

# 濡쒓렇 ?뺤씤
# ?쒕쾭 濡쒓렇?먯꽌 ?곸꽭 ?먮윭 硫붿떆吏 ?뺤씤
```

### ?곗씠?곌? ?놁쓬
```bash
# DB ?뚯씪 ?뺤씤
psql "$DATABASE_URL" -c "SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size;"

# ?곗씠???뺤씤
psql "$DATABASE_URL" "SELECT COUNT(*) FROM daily_market_data;"

# ?곗씠???섏쭛 ?ㅼ떆 ?ㅽ뻾
uv run python -m financial_data_collector.collect_krx_data \
  --date-from 2026-01-02 --date-to 2026-01-03
```

## ?뱴 ???먯꽭???댁슜

- [BULK_EXPORT_API_SPEC.md](./BULK_EXPORT_API_SPEC.md) - API ?곸꽭 ?ㅽ럺
- [DEPLOYMENT_GUIDE.md](./DEPLOYMENT_GUIDE.md) - ?꾨줈?뺤뀡 諛고룷 媛?대뱶
- [README.md](../README.md) - ?꾨줈?앺듃 媛쒖슂

## ?뮕 Tips

1. **Swagger UI ?쒖슜**: http://localhost:8000/docs ?먯꽌 紐⑤뱺 API瑜?吏곸젒 ?뚯뒪?명븷 ???덉뒿?덈떎
2. **?붾쾭洹?紐⑤뱶**: `--debug` ?뚮옒洹몃줈 ?곸꽭 濡쒓렇 ?뺤씤
3. **?묒? 湲곌컙遺??*: 泥섏쓬?먮뒗 1-2?쇱튂 ?곗씠?곕줈 ?뚯뒪?????뺤옣
4. **?붿뒪??怨듦컙**: 1?꾩튂 ?곗씠??~10GB ?뺣룄 ?꾩슂 (Parquet ?ы븿)
5. **諛깆뾽**: PostgreSQL DB ?뚯씪???뺢린?곸쑝濡?諛깆뾽


## Compose Runtime (Current)

```bash
# 1) Start postgres + api + daily collector
docker compose up -d --build

# 2) Check health
docker compose ps
curl http://localhost:8000/health

# 3) One-shot backfill (optional)
docker compose --profile collector run --rm \
  -e DATE_FROM=2026-02-14 \
  -e DATE_TO=2026-02-21 \
  collector-once
```

Notes:
- Dashboard instrument tab is code-based (`external_code`) and does not expose UUIDs.
- Run status policy: `FAILED` only when errors exist, `PARTIAL` when warnings only.
