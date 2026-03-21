# Deployment Guide

## ??
- `postgres`: ???
- `api`: ?? API? ????
- `collector`: ?? ? ?? ??? ????

## ?? ????
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- `DATABASE_URL`
- `KRX_AUTH_KEY`

## ??
```powershell
docker compose up -d --build
```

## ??
```powershell
docker compose ps
just health
docker compose logs --no-color --tail 100 api
```

## ?? ??
KRX ??/??? ??:
```powershell
uv run collect-krx-data --date-from 2026-03-01 --date-to 2026-03-20
```

KIND ?? ???:
```powershell
uv run collect-kind-delistings --database-url $env:DATABASE_URL
```

???? ???:
```powershell
uv run rebuild-adjustment-factors --date-from 2026-03-01 --date-to 2026-03-20
```

## ?? ??
- API ?? ??: `just health`
- DB ?? ??: `docker compose exec -T api printenv DATABASE_URL`
- ?? ?? ??: `docker compose logs --no-color --tail 120 api`
