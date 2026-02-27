set shell := ["powershell.exe", "-NoProfile", "-Command"]

default: help

help:
    just --list

up:
    docker compose up -d --build

down:
    docker compose down

ps:
    docker compose ps

logs service="api":
    docker compose logs --no-color --tail 100 -f {{service}}

logs-once service="api":
    docker compose logs --no-color --tail 300 {{service}}

health:
    $ok = $false; for ($i = 0; $i -lt 30; $i++) { try { $resp = Invoke-WebRequest -UseBasicParsing http://localhost:8000/health -TimeoutSec 2; if ($resp.StatusCode -eq 200) { $resp.Content; $ok = $true; break } } catch { Start-Sleep -Seconds 1 } }; if (-not $ok) { Write-Output "health check failed after retries. run: just doctor"; exit 1 }

collect date_from date_to:
    docker compose --profile collector run --rm -e DATE_FROM={{date_from}} -e DATE_TO={{date_to}} collector-once

collect-local date_from date_to:
    uv run python -m financial_data_collector.collect_krx_data --date-from {{date_from}} --date-to {{date_to}}

collect-delisted date_from="1900-01-01" date_to="":
    $to = if ("{{date_to}}" -eq "") { (Get-Date -Format "yyyy-MM-dd") } else { "{{date_to}}" }; docker compose --profile collector run --rm -e DATE_FROM_ARG={{date_from}} -e DATE_TO_ARG=$to --entrypoint sh collector-once -c 'PYTHONPATH=/app/src uv run python -m financial_data_collector.collect_kind_delistings --database-url ${DATABASE_URL:-postgresql://postgres:postgres@postgres:5432/financial_data} --date-from $DATE_FROM_ARG --date-to $DATE_TO_ARG'

collect-delisted-local date_from="1900-01-01" date_to="":
    $to = if ("{{date_to}}" -eq "") { (Get-Date -Format "yyyy-MM-dd") } else { "{{date_to}}" }; $env:PYTHONPATH='src'; uv run python -m financial_data_collector.collect_kind_delistings --database-url $env:DATABASE_URL --date-from {{date_from}} --date-to $to

serve-local:
    uv run uvicorn financial_data_collector.server:app --host 0.0.0.0 --port 8000

test:
    uv run pytest -q

doctor:
    docker compose ps
    try { (Invoke-WebRequest -UseBasicParsing http://localhost:8000/health).Content } catch { $_.Exception.Message; Write-Output "health check failed" }
    docker compose logs --no-color --tail 120 api

reset-db:
    docker compose down -v
    docker compose up -d --build
