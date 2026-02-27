import uuid

import pytest


pytestmark = pytest.mark.postgres


def test_pg_views_exist(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = current_schema()
            ORDER BY table_name
            """
        )
        names = {row[0] for row in cur.fetchall()}
    assert "benchmark_dataset_v1" in names
    assert "core_market_dataset_v1" in names
    assert "trading_calendar_v1" in names


def test_pg_delisting_snapshot_table_exists(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_name = 'instrument_delisting_snapshot'
            """
        )
        rows = cur.fetchall()
    assert len(rows) == 1


def test_pg_daily_market_volume_constraint(pg_conn):
    instrument_id = str(uuid.uuid4())
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments(
                instrument_id, external_code, market_code, instrument_name,
                listing_date, delisting_date, source_name, collected_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, now(), %s)
            """,
            (instrument_id, "0001", "KOSDAQ", "A", "2020-01-01", None, "t", None),
        )
    pg_conn.commit()

    with pytest.raises(Exception):
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO daily_market_data(
                    instrument_id, trade_date, open, high, low, close, volume,
                    turnover_value, market_value, is_trade_halted, is_under_supervision,
                    record_status, source_name, collected_at, run_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
                """,
                (instrument_id, "2026-01-02", 10, 11, 9, 10, -1, 10, 10, False, False, "VALID", "t", None),
            )
        pg_conn.commit()
    pg_conn.rollback()


def test_pg_benchmark_ohlc_constraint(pg_conn):
    with pytest.raises(Exception):
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO benchmark_index_data(
                    index_code, index_name, trade_date, open, high, low, close, source_name, collected_at, run_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
                """,
                ("KOSDAQ", "KOSDAQ", "2026-01-02", 100, 99, 98, 99.5, "t", None),
            )
        pg_conn.commit()
    pg_conn.rollback()


def test_pg_run_status_constraint(pg_conn):
    with pytest.raises(Exception):
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO collection_runs(
                    run_id, pipeline_name, source_name, window_start, window_end, status, started_at
                ) VALUES (%s, %s, %s, %s, %s, %s, now())
                """,
                (str(uuid.uuid4()), "p1", "s1", "2026-01-01", "2026-01-02", "WRONG"),
            )
        pg_conn.commit()
    pg_conn.rollback()


def test_pg_issue_severity_constraint(pg_conn):
    with pytest.raises(Exception):
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_quality_issues(
                    dataset_name, trade_date, instrument_id, index_code, issue_code,
                    severity, issue_detail, source_name, detected_at, run_id, resolved_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now(), %s, %s)
                """,
                ("daily_market_data", "2026-01-02", None, None, "X", "FATAL", "bad", "t", None, None),
            )
        pg_conn.commit()
    pg_conn.rollback()
