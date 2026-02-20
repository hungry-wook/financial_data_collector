# KRX Data Collector - 운영 가이드

## 📋 목차
1. [아키텍처 개요](#아키텍처-개요)
2. [Daily 데이터 수집](#daily-데이터-수집)
3. [FastAPI 서버 운영](#fastapi-서버-운영)
4. [모니터링 및 유지보수](#모니터링-및-유지보수)

## 🏗️ 아키텍처 개요

```
┌─────────────────┐
│  Cron Job       │  매일 18:00 - 전날 데이터 수집
│  (Daily 수집)   │  → SQLite DB 저장
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SQLite DB      │  모든 수집 데이터 저장
│  (Local)        │  - instruments
└────────┬────────┘  - daily_market_data
         │           - benchmark_index_data
         │
         ▼
┌─────────────────┐
│  FastAPI Server │  Backtest Export API
│  (Port 8000)    │  → Parquet 파일 생성
└─────────────────┘
```

## 📊 Daily 데이터 수집

### 1. 수동 실행
```bash
# 어제 데이터 수집
uv run python -m financial_data_collector.collect_krx_data \
  --date-from $(date -d "yesterday" +%Y-%m-%d) \
  --date-to $(date -d "yesterday" +%Y-%m-%d)

# 특정 기간 수집
uv run python -m financial_data_collector.collect_krx_data \
  --date-from 2026-01-02 \
  --date-to 2026-01-10
```

### 2. Cron Job 설정 (Linux)
```bash
# crontab 편집
crontab -e

# 매일 오후 6시에 실행
0 18 * * * cd /app/financial_data_collector && \
  uv run python -m financial_data_collector.collect_krx_data \
  --date-from $(date -d "yesterday" +\%Y-\%m-\%d) \
  --date-to $(date -d "yesterday" +\%Y-\%m-\%d) \
  >> /var/log/krx_collection.log 2>&1
```

### 3. Windows 작업 스케줄러
```powershell
# PowerShell 스크립트 생성: collect_daily.ps1
$yesterday = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd")
cd C:\workspace\financial_data_collector
uv run python -m financial_data_collector.collect_krx_data --date-from $yesterday --date-to $yesterday

# 작업 스케줄러에 등록
# - 트리거: 매일 오후 6시
# - 작업: powershell.exe -File "C:\path\to\collect_daily.ps1"
```

### 4. Docker Cron (권장)
```dockerfile
# Dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY . .
RUN pip install uv && uv sync

# Cron 설정
RUN apt-get update && apt-get install -y cron
COPY crontab /etc/cron.d/krx-collector
RUN chmod 0644 /etc/cron.d/krx-collector
RUN crontab /etc/cron.d/krx-collector

CMD ["cron", "-f"]
```

```bash
# crontab 파일
0 18 * * * cd /app && uv run python -m financial_data_collector.collect_krx_data --date-from $(date -d "yesterday" +\%Y-\%m-\%d) --date-to $(date -d "yesterday" +\%Y-\%m-\%d) >> /var/log/cron.log 2>&1
```

## 🚀 FastAPI 서버 운영

### 1. 로컬 개발 실행
```bash
# 직접 실행
uv run python -m financial_data_collector.server

# 또는 uvicorn 사용
uv run uvicorn financial_data_collector.server:app --reload --host 0.0.0.0 --port 8000
```

### 2. Production 실행 (Gunicorn + Uvicorn Workers)
```bash
# 설치
uv add gunicorn

# 실행 (4 workers)
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
# 서비스 시작
sudo systemctl daemon-reload
sudo systemctl enable krx-api
sudo systemctl start krx-api
sudo systemctl status krx-api

# 로그 확인
journalctl -u krx-api -f
```

### 4. Docker Compose (권장)
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
      - DB_PATH=/app/data/financial_data.db
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
# 실행
docker-compose up -d

# 로그 확인
docker-compose logs -f api

# 종료
docker-compose down
```

## 🔧 API 사용 예시

### 1. Export Job 생성
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

### 2. Job 상태 확인
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

### 3. Manifest 확인
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

## 📊 모니터링 및 유지보수

### 1. 데이터 수집 모니터링
```bash
# 최근 수집 Run 확인
sqlite3 data/financial_data.db "
SELECT run_id, status, success_count, failure_count,
       started_at, finished_at
FROM collection_runs
ORDER BY started_at DESC
LIMIT 10;
"

# 데이터 품질 이슈 확인
sqlite3 data/financial_data.db "
SELECT issue_code, severity, COUNT(*) as count
FROM data_quality_issues
GROUP BY issue_code, severity
ORDER BY count DESC;
"
```

### 2. 디스크 사용량 모니터링
```bash
# DB 크기 확인
du -h data/financial_data.db

# Export 폴더 크기 확인
du -sh data/backtest_export/

# 오래된 export 삭제 (30일 이상)
find data/backtest_export/ -type d -mtime +30 -exec rm -rf {} \;
```

### 3. API 헬스체크
```bash
# Health endpoint
curl http://localhost:8000/health

# Prometheus metrics (추가 구현 필요)
# curl http://localhost:8000/metrics
```

### 4. 로그 관리
```bash
# API 로그 확인
tail -f /var/log/api_access.log
tail -f /var/log/api_error.log

# Collection 로그 확인
tail -f /var/log/krx_collection.log

# 로그 로테이션 설정 (/etc/logrotate.d/krx-api)
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

## 🔒 보안 고려사항

### 1. API 인증 (추천 추가 구현)
```python
# server.py에 추가
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security = HTTPBearer()

@app.post("/api/v1/backtest/exports", dependencies=[Depends(verify_token)])
async def create_export(...):
    ...
```

### 2. Rate Limiting
```python
# slowapi 사용
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

@app.post("/api/v1/backtest/exports")
@limiter.limit("10/minute")
async def create_export(...):
    ...
```

### 3. CORS 설정 (필요시)
```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://yourdomain.com"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
```

## 📈 성능 최적화

### 1. DB Index 추가
```sql
-- 자주 조회되는 컬럼에 인덱스
CREATE INDEX idx_daily_trade_date ON daily_market_data(trade_date);
CREATE INDEX idx_daily_instrument ON daily_market_data(instrument_id);
CREATE INDEX idx_benchmark_date ON benchmark_index_data(trade_date);
```

### 2. Background Job Queue (대량 Export용)
```bash
# Redis + Celery 사용 권장
uv add celery[redis]

# celery worker 실행
celery -A financial_data_collector.tasks worker --loglevel=info
```

### 3. Cache Layer (선택적)
```python
# Redis cache for manifest
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend

@app.on_event("startup")
async def startup():
    redis = aioredis.from_url("redis://localhost")
    FastAPICache.init(RedisBackend(redis), prefix="krx-cache")
```

## 🆘 트러블슈팅

### 문제: 데이터 수집 실패
```bash
# 1. API Key 확인
echo $KRX_AUTH_KEY

# 2. API 호출 제한 확인
sqlite3 data/financial_data.db "
SELECT COUNT(*) as api_calls
FROM collection_runs
WHERE DATE(started_at) = DATE('now');
"

# 3. 로그 확인
grep -i error /var/log/krx_collection.log
```

### 문제: Export 실패
```bash
# 디스크 공간 확인
df -h

# Permission 확인
ls -la data/backtest_export/

# DB lock 확인
lsof | grep financial_data.db
```

## 📝 Checklist

**운영 시작 전 확인사항:**
- [ ] KRX API Key 설정 (.env 파일)
- [ ] Cron job 설정 및 테스트
- [ ] FastAPI 서버 정상 실행 확인
- [ ] 디스크 공간 충분한지 확인 (최소 100GB 권장)
- [ ] 로그 로테이션 설정
- [ ] 백업 전략 수립 (DB 백업)
- [ ] 모니터링 대시보드 구축 (Grafana + Prometheus)
- [ ] 알림 설정 (수집 실패 시 Slack/Email)

**정기 점검 (주간):**
- [ ] 데이터 수집 성공률 확인
- [ ] 데이터 품질 이슈 리뷰
- [ ] 디스크 사용량 확인
- [ ] 로그 검토

**정기 점검 (월간):**
- [ ] DB 백업 및 복구 테스트
- [ ] 오래된 export 파일 정리
- [ ] 성능 메트릭 리뷰
- [ ] 보안 패치 적용
