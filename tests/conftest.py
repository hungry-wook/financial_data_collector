import os
import shutil
import subprocess
import time
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import pytest

from financial_data_collector.repository import Repository
from financial_data_collector.settings import load_dotenv


def _is_truthy(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _has_placeholder_dsn(dsn: str) -> bool:
    parsed = urlparse(dsn)
    placeholder_tokens = {"host", "user", "password", "username"}
    host = (parsed.hostname or "").strip().lower()
    user = (parsed.username or "").strip().lower()
    password = (parsed.password or "").strip().lower()
    return host in placeholder_tokens or user in placeholder_tokens or password in placeholder_tokens


def _is_localhost_dsn_with_port(dsn: str, port: str) -> bool:
    parsed = urlparse(dsn)
    host = (parsed.hostname or "").strip().lower()
    parsed_port = parsed.port
    return host in {"127.0.0.1", "localhost"} and str(parsed_port or "") == str(port).strip()


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.db"


@pytest.fixture
def repo(db_path: Path) -> Repository:
    r = Repository(db_path.as_posix())
    r.init_schema()
    return r


@pytest.fixture
def freeze_time_fixture():
    try:
        from freezegun import freeze_time
    except ImportError:
        pytest.skip("freezegun is not installed")

    with freeze_time("2026-01-02 12:00:00"):
        yield


@pytest.fixture(scope="session")
def pg_test_dsn(pg_temp_container_dsn: str) -> str:
    load_dotenv(".env")
    if pg_temp_container_dsn:
        return pg_temp_container_dsn

    dsn = os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not set")
    if _has_placeholder_dsn(dsn):
        pytest.skip(
            "TEST_POSTGRES_DSN contains placeholder values. Replace USER/PASSWORD/HOST with real PostgreSQL connection values."
        )
    return dsn


@pytest.fixture(scope="session")
def pg_temp_container_dsn():
    load_dotenv(".env")
    dsn = os.getenv("TEST_POSTGRES_DSN", "").strip()
    force_temp = _is_truthy(os.getenv("TEST_POSTGRES_USE_TEMP_CONTAINER", "0"))
    host_port = os.getenv("TEST_POSTGRES_DOCKER_PORT", "5431").strip() or "5431"

    should_start = force_temp or (not dsn) or _has_placeholder_dsn(dsn) or _is_localhost_dsn_with_port(dsn, host_port)
    if not should_start:
        yield None
        return

    if not shutil.which("docker"):
        pytest.skip("docker is required to auto-start temporary PostgreSQL container")

    container_name = os.getenv("TEST_POSTGRES_DOCKER_CONTAINER", "fdc-pg-test")
    image = os.getenv("TEST_POSTGRES_DOCKER_IMAGE", "postgres:16")
    pg_user = os.getenv("TEST_POSTGRES_DOCKER_USER", "postgres")
    pg_password = os.getenv("TEST_POSTGRES_DOCKER_PASSWORD", "postgres")
    pg_db = os.getenv("TEST_POSTGRES_DOCKER_DB", "postgres")
    wait_seconds = int(os.getenv("TEST_POSTGRES_DOCKER_WAIT_SEC", "60"))

    subprocess.run(["docker", "rm", "-f", container_name], check=False, capture_output=True, text=True)

    run_cmd = [
        "docker",
        "run",
        "--rm",
        "--name",
        container_name,
        "-e",
        f"POSTGRES_USER={pg_user}",
        "-e",
        f"POSTGRES_PASSWORD={pg_password}",
        "-e",
        f"POSTGRES_DB={pg_db}",
        "-p",
        f"{host_port}:5432",
        "-d",
        image,
    ]
    started = subprocess.run(run_cmd, check=False, capture_output=True, text=True)
    if started.returncode != 0:
        pytest.skip(f"Failed to start temporary PostgreSQL container: {started.stderr.strip() or started.stdout.strip()}")

    ready = False
    for _ in range(wait_seconds):
        probe = subprocess.run(
            ["docker", "exec", container_name, "pg_isready", "-U", pg_user],
            check=False,
            capture_output=True,
            text=True,
        )
        if probe.returncode == 0:
            ready = True
            break
        time.sleep(1)

    if not ready:
        logs = subprocess.run(["docker", "logs", container_name], check=False, capture_output=True, text=True)
        subprocess.run(["docker", "stop", container_name], check=False, capture_output=True, text=True)
        pytest.skip(
            "Temporary PostgreSQL container did not become ready in time. "
            f"docker logs: {logs.stdout[-500:]}"
        )

    dsn = f"postgresql://{pg_user}:{pg_password}@127.0.0.1:{host_port}/{pg_db}"
    try:
        yield dsn
    finally:
        subprocess.run(["docker", "stop", container_name], check=False, capture_output=True, text=True)


@pytest.fixture(scope="session")
def pg_connect_timeout_sec() -> int:
    load_dotenv(".env")
    raw = os.getenv("TEST_POSTGRES_CONNECT_TIMEOUT_SEC", "5").strip()
    try:
        timeout = int(raw)
    except ValueError:
        pytest.skip("TEST_POSTGRES_CONNECT_TIMEOUT_SEC must be an integer")
    if timeout <= 0:
        pytest.skip("TEST_POSTGRES_CONNECT_TIMEOUT_SEC must be > 0")
    return timeout


@pytest.fixture(scope="session")
def pg_preflight(pg_test_dsn: str, pg_connect_timeout_sec: int):
    psycopg = pytest.importorskip("psycopg")
    try:
        with psycopg.connect(pg_test_dsn, connect_timeout=pg_connect_timeout_sec, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    except Exception as exc:
        pytest.skip(f"PostgreSQL preflight failed: {exc}")
    return True


@pytest.fixture
def pg_conn(pg_test_dsn: str, pg_connect_timeout_sec: int, pg_preflight):
    psycopg = pytest.importorskip("psycopg")

    schema_name = f"tdd_{uuid4().hex}"
    with psycopg.connect(pg_test_dsn, connect_timeout=pg_connect_timeout_sec, autocommit=True) as admin_conn:
        with admin_conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema_name}"')

    try:
        with psycopg.connect(pg_test_dsn, connect_timeout=pg_connect_timeout_sec, autocommit=False) as conn:
            with conn.cursor() as cur:
                cur.execute(f'SET search_path TO "{schema_name}"')
                cur.execute(Path("sql/platform_schema.sql").read_text(encoding="utf-8-sig"))
            conn.commit()
            yield conn
    finally:
        try:
            with psycopg.connect(pg_test_dsn, connect_timeout=pg_connect_timeout_sec, autocommit=True) as admin_conn:
                with admin_conn.cursor() as cur:
                    cur.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        except Exception:
            pass
