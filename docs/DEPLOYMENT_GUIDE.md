# KRX Data Collector - ?댁쁺 媛?대뱶

## ?뱥 紐⑹감
1. [?꾪궎?띿쿂 媛쒖슂](#?꾪궎?띿쿂-媛쒖슂)
2. [Daily ?곗씠???섏쭛](#daily-?곗씠???섏쭛)
3. [FastAPI ?쒕쾭 ?댁쁺](#fastapi-?쒕쾭-?댁쁺)
4. [紐⑤땲?곕쭅 諛??좎?蹂댁닔](#紐⑤땲?곕쭅-諛??좎?蹂댁닔)

## ?룛截??꾪궎?띿쿂 媛쒖슂

```
?뚢????????????????????? Cron Job       ?? 留ㅼ씪 18:00 - ?꾨궇 ?곗씠???섏쭛
?? (Daily ?섏쭛)   ?? ??SQLite DB ????붴?????????р??????????         ??         ???뚢????????????????????? SQLite DB      ?? 紐⑤뱺 ?섏쭛 ?곗씠??????? (Local)        ?? - instruments
?붴?????????р?????????? - daily_market_data
         ??          - benchmark_index_data
         ??         ???뚢????????????????????? FastAPI Server ?? Backtest Export API
?? (Port 8000)    ?? ??Parquet ?뚯씪 ?앹꽦
?붴???????????????????```

## ?뱤 Daily ?곗씠???섏쭛

### 1. ?섎룞 ?ㅽ뻾
```bash
# ?댁젣 ?곗씠???섏쭛
uv run python -m financial_data_collector.collect_krx_data \
  --date-from $(date -d "yesterday" +%Y-%m-%d) \
  --date-to $(date -d "yesterday" +%Y-%m-%d)

# ?뱀젙 湲곌컙 ?섏쭛
uv run python -m financial_data_collector.collect_krx_data \
  --date-from 2026-01-02 \
  --date-to 2026-01-10
```

### 2. Cron Job ?ㅼ젙 (Linux)
```bash
# crontab ?몄쭛
crontab -e

# 留ㅼ씪 ?ㅽ썑 6?쒖뿉 ?ㅽ뻾
0 18 * * * cd /app/financial_data_collector && \
  uv run python -m financial_data_collector.collect_krx_data \
  --date-from $(date -d "yesterday" +\%Y-\%m-\%d) \
  --date-to $(date -d "yesterday" +\%Y-\%m-\%d) \
  >> /var/log/krx_collection.log 2>&1
```

### 3. Windows ?묒뾽 ?ㅼ?以꾨윭
```powershell
# PowerShell ?ㅽ겕由쏀듃 ?앹꽦: collect_daily.ps1
$yesterday = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd")
cd C:\workspace\financial_data_collector
uv run python -m financial_data_collector.collect_krx_data --date-from $yesterday --date-to $yesterday

# ?묒뾽 ?ㅼ?以꾨윭???깅줉
# - ?몃━嫄? 留ㅼ씪 ?ㅽ썑 6??# - ?묒뾽: powershell.exe -File "C:\path\to\collect_daily.ps1"
```

### 4. Docker Cron (沅뚯옣)
```dockerfile
# Dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY . .
RUN pip install uv && uv sync

# Cron ?ㅼ젙
RUN apt-get update && apt-get install -y cron
COPY crontab /etc/cron.d/krx-collector
RUN chmod 0644 /etc/cron.d/krx-collector
RUN crontab /etc/cron.d/krx-collector

CMD ["cron", "-f"]
```

```bash
# crontab ?뚯씪
0 18 * * * cd /app && uv run python -m financial_data_collector.collect_krx_data --date-from $(date -d "yesterday" +\%Y-\%m-\%d) --date-to $(date -d "yesterday" +\%Y-\%m-\%d) >> /var/log/cron.log 2>&1
```

## ?? FastAPI ?쒕쾭 ?댁쁺

### 1. 濡쒖뺄 媛쒕컻 ?ㅽ뻾
```bash
# 吏곸젒 ?ㅽ뻾
uv run python -m financial_data_collector.server

# ?먮뒗 uvicorn ?ъ슜
uv run uvicorn financial_data_collector.server:app --reload --host 0.0.0.0 --port 8000
```

### 2. Production ?ㅽ뻾 (Gunicorn + Uvicorn Workers)
```bash
# ?ㅼ튂
uv add gunicorn

# ?ㅽ뻾 (4 workers)
uv run gunicorn financial_data_collector.server:app \
  --workers 4 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000 \
  --timeout 300 \
  --access-logfile /var/log/api_access.log \
  --error-logfile /var/log/api_error.log
```

### 3. Systemd Service (Linux)
```ini
# /etc/systemd/system/krx-api.service
[Unit]
Description=KRX Backtest Export API
After=network.target

[Service]
Type=notify
User=app
Group=app
WorkingDirectory=/app/financial_data_collector
Environment="PATH=/app/financial_data_collector/.venv/bin"
ExecStart=/app/financial_data_collector/.venv/bin/gunicorn \
  financial_data_collector.server:app \
  --workers 4 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000 \
  --timeout 300
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
# ?쒕퉬???쒖옉
sudo systemctl daemon-reload
sudo systemctl enable krx-api
sudo systemctl start krx-api
sudo systemctl status krx-api

# 濡쒓렇 ?뺤씤
journalctl -u krx-api -f
```

### 4. Docker Compose (沅뚯옣)
```yaml
# docker-compose.yml
version: '3.8'

services:
  # Data Collector (Cron)
  collector:
    build: .
    volumes:
      - ./data:/app/data
      - ./logs:/var/log
    environment:
      - KRX_AUTH_KEY=${KRX_AUTH_KEY}
      - KRX_DAILY_LIMIT=10000
    restart: unless-stopped

  # API Server
  api:
    build: .
    command: >
      gunicorn financial_data_collector.server:app
      --workers 4
      --worker-class uvicorn.workers.UvicornWorker
      --bind 0.0.0.0:8000
      --timeout 300
    ports:
      - "8000:8000"
    volumes:
      - ./data:/app/data
    environment:
      - DATABASE_URL=postgresql://user:pass@postgres:5432/financial_data
    restart: unless-stopped
    depends_on:
      - collector

  # Nginx (Optional - for production)
  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - ./ssl:/etc/nginx/ssl:ro
    depends_on:
      - api
    restart: unless-stopped
```

```bash
# ?ㅽ뻾
docker-compose up -d

# 濡쒓렇 ?뺤씤
docker-compose logs -f api

# 醫낅즺
docker-compose down
```

## ?뵩 API ?ъ슜 ?덉떆

### 1. Export Job ?앹꽦
```bash
curl -X POST http://localhost:8000/api/v1/backtest/exports \
  -H "Content-Type: application/json" \
  -d '{
    "market_code": "KOSDAQ",
    "index_codes": ["KOSDAQ", "KOSPI"],
    "date_from": "2026-01-02",
    "date_to": "2026-01-10",
    "include_issues": true,
    "output_format": "parquet",
    "output_path": "/data/exports/2026_q1"
  }'

# Response:
# {
#   "job_id": "abc-123-def",
#   "status": "PENDING",
#   "submitted_at": "2026-02-20T10:00:00Z"
# }
```

### 2. Job ?곹깭 ?뺤씤
```bash
curl http://localhost:8000/api/v1/backtest/exports/abc-123-def

# Response (Running):
# {
#   "job_id": "abc-123-def",
#   "status": "RUNNING",
#   "progress": 60,
#   "started_at": "2026-02-20T10:00:05Z"
# }

# Response (Completed):
# {
#   "job_id": "abc-123-def",
#   "status": "SUCCEEDED",
#   "output_path": "/data/exports/2026_q1",
#   "files": [...],
#   "row_counts": {...}
# }
```

### 3. Manifest ?뺤씤
```bash
curl http://localhost:8000/api/v1/backtest/exports/abc-123-def/manifest

# Response:
# {
#   "job_id": "abc-123-def",
#   "market_code": "KOSDAQ",
#   "date_from": "2026-01-02",
#   "date_to": "2026-01-10",
#   "files": [
#     {
#       "name": "instrument_daily.parquet",
#       "rows": 16136,
#       "sha256": "..."
#     },
#     ...
#   ]
# }
```

## ?뱤 紐⑤땲?곕쭅 諛??좎?蹂댁닔

### 1. ?곗씠???섏쭛 紐⑤땲?곕쭅
```bash
# 理쒓렐 ?섏쭛 Run ?뺤씤
psql "$DATABASE_URL" -c "
SELECT run_id, status, success_count, failure_count,
       started_at, finished_at
FROM collection_runs
ORDER BY started_at DESC
LIMIT 10;
"

# ?곗씠???덉쭏 ?댁뒋 ?뺤씤
psql "$DATABASE_URL" -c "
SELECT issue_code, severity, COUNT(*) as count
FROM data_quality_issues
GROUP BY issue_code, severity
ORDER BY count DESC;
"
```

### 2. ?붿뒪???ъ슜??紐⑤땲?곕쭅
```bash
# DB ?ш린 ?뺤씤
psql "$DATABASE_URL" -c "SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size;"

# Export ?대뜑 ?ш린 ?뺤씤
du -sh data/backtest_export/

# ?ㅻ옒??export ??젣 (30???댁긽)
find data/backtest_export/ -type d -mtime +30 -exec rm -rf {} \;
```

### 3. API ?ъ뒪泥댄겕
```bash
# Health endpoint
curl http://localhost:8000/health

# Prometheus metrics (異붽? 援ы쁽 ?꾩슂)
# curl http://localhost:8000/metrics
```

### 4. 濡쒓렇 愿由?```bash
# API 濡쒓렇 ?뺤씤
tail -f /var/log/api_access.log
tail -f /var/log/api_error.log

# Collection 濡쒓렇 ?뺤씤
tail -f /var/log/krx_collection.log

# 濡쒓렇 濡쒗뀒?댁뀡 ?ㅼ젙 (/etc/logrotate.d/krx-api)
/var/log/api_*.log {
    daily
    rotate 30
    compress
    delaycompress
    notifempty
    create 0640 app app
    sharedscripts
    postrotate
        systemctl reload krx-api > /dev/null 2>&1 || true
    endscript
}
```

## ?뵏 蹂댁븞 怨좊젮?ы빆

### 1. API ?몄쬆 (異붿쿇 異붽? 援ы쁽)
```python
# server.py??異붽?
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security = HTTPBearer()

@app.post("/api/v1/backtest/exports", dependencies=[Depends(verify_token)])
async def create_export(...):
    ...
```

### 2. Rate Limiting
```python
# slowapi ?ъ슜
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

@app.post("/api/v1/backtest/exports")
@limiter.limit("10/minute")
async def create_export(...):
    ...
```

### 3. CORS ?ㅼ젙 (?꾩슂??
```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://yourdomain.com"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
```

## ?뱢 ?깅뒫 理쒖쟻??
### 1. DB Index 異붽?
```sql
-- ?먯＜ 議고쉶?섎뒗 而щ읆???몃뜳??CREATE INDEX idx_daily_trade_date ON daily_market_data(trade_date);
CREATE INDEX idx_daily_instrument ON daily_market_data(instrument_id);
CREATE INDEX idx_benchmark_date ON benchmark_index_data(trade_date);
```

### 2. Background Job Queue (???Export??
```bash
# Redis + Celery ?ъ슜 沅뚯옣
uv add celery[redis]

# celery worker ?ㅽ뻾
celery -A financial_data_collector.tasks worker --loglevel=info
```

### 3. Cache Layer (?좏깮??
```python
# Redis cache for manifest
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend

@app.on_event("startup")
async def startup():
    redis = aioredis.from_url("redis://localhost")
    FastAPICache.init(RedisBackend(redis), prefix="krx-cache")
```

## ?넊 ?몃윭釉붿뒋??
### 臾몄젣: ?곗씠???섏쭛 ?ㅽ뙣
```bash
# 1. API Key ?뺤씤
echo $KRX_AUTH_KEY

# 2. API ?몄텧 ?쒗븳 ?뺤씤
psql "$DATABASE_URL" -c "
SELECT COUNT(*) as api_calls
FROM collection_runs
WHERE DATE(started_at) = DATE('now');
"

# 3. 濡쒓렇 ?뺤씤
grep -i error /var/log/krx_collection.log
```

### 臾몄젣: Export ?ㅽ뙣
```bash
# ?붿뒪??怨듦컙 ?뺤씤
df -h

# Permission ?뺤씤
ls -la data/backtest_export/

# DB lock ?뺤씤
psql "$DATABASE_URL" -c "SELECT pid, state, query FROM pg_stat_activity WHERE datname = current_database();"
```

## ?뱷 Checklist

**?댁쁺 ?쒖옉 ???뺤씤?ы빆:**
- [ ] KRX API Key ?ㅼ젙 (.env ?뚯씪)
- [ ] Cron job ?ㅼ젙 諛??뚯뒪??- [ ] FastAPI ?쒕쾭 ?뺤긽 ?ㅽ뻾 ?뺤씤
- [ ] ?붿뒪??怨듦컙 異⑸텇?쒖? ?뺤씤 (理쒖냼 100GB 沅뚯옣)
- [ ] 濡쒓렇 濡쒗뀒?댁뀡 ?ㅼ젙
- [ ] 諛깆뾽 ?꾨왂 ?섎┰ (DB 諛깆뾽)
- [ ] 紐⑤땲?곕쭅 ??쒕낫??援ъ텞 (Grafana + Prometheus)
- [ ] ?뚮┝ ?ㅼ젙 (?섏쭛 ?ㅽ뙣 ??Slack/Email)

**?뺢린 ?먭? (二쇨컙):**
- [ ] ?곗씠???섏쭛 ?깃났瑜??뺤씤
- [ ] ?곗씠???덉쭏 ?댁뒋 由щ럭
- [ ] ?붿뒪???ъ슜???뺤씤
- [ ] 濡쒓렇 寃??
**?뺢린 ?먭? (?붽컙):**
- [ ] DB 諛깆뾽 諛?蹂듦뎄 ?뚯뒪??- [ ] ?ㅻ옒??export ?뚯씪 ?뺣━
- [ ] ?깅뒫 硫뷀듃由?由щ럭
- [ ] 蹂댁븞 ?⑥튂 ?곸슜


## Compose Runtime (Current)

```bash
# Start stack
docker compose up -d --build

# Services
# - postgres: persistent DB
# - api: FastAPI server
# - collector: always-on daily collector loop

docker compose ps
curl http://localhost:8000/health

# One-shot range collection
docker compose --profile collector run --rm \
  -e DATE_FROM=2026-02-14 \
  -e DATE_TO=2026-02-21 \
  collector-once
```

Operational policy:
- Run status is `FAILED` only when ERROR issues are detected.
- WARN-only runs are recorded as `PARTIAL`.
- Dashboard instrument list uses `external_code` only (no UUID display).
