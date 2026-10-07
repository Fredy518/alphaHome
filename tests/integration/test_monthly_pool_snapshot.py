"""Monthly revalidation reads one snapshot without blocking source ingestion."""
from datetime import date

import psycopg2
import pytest

from alphahome.curation import etf_usable_pool_monthly as pool

pytestmark = [pytest.mark.integration, pytest.mark.requires_db]


def test_monthly_revalidation_does_not_lock_source_writes(isolated_database_url, monkeypatch):
    reader = psycopg2.connect(isolated_database_url)
    writer = psycopg2.connect(isolated_database_url)
    writer.autocommit = True
    with writer.cursor() as cur:
        cur.execute("SELECT to_regclass('public.review_monthly_source')")
        assert cur.fetchone()[0] is None
        cur.execute("CREATE TABLE public.review_monthly_source (id integer PRIMARY KEY, value integer)")
        cur.execute("INSERT INTO public.review_monthly_source VALUES (1,10)")
    payload = dict(executable=True, start_month="2026-09-01", end_month="2026-09-01",
                   as_of="2026-10-07", record_kind="HISTORICAL_RECONSTRUCTION")
    plan = pool.MonthlyPlan(payload, [])
    monkeypatch.setattr(pool, "schema_plan", lambda _: {"missing_objects": []})
    monkeypatch.setattr(pool, "lock_candidate_master", lambda _: None)

    def revalidate(conn, **kwargs):
        with conn.cursor() as cur:
            cur.execute("SHOW transaction_isolation")
            assert cur.fetchone()[0] == "repeatable read"
            cur.execute("SELECT value FROM public.review_monthly_source")
            assert cur.fetchone()[0] == 10
            with writer.cursor() as other:
                other.execute("SET lock_timeout='300ms'")
                other.execute("UPDATE public.review_monthly_source SET value=11 WHERE id=1")
            cur.execute("SELECT value FROM public.review_monthly_source")
            assert cur.fetchone()[0] == 10
        # Exercise the fail-closed path after observing the stable source snapshot.
        return pool.MonthlyPlan({**payload, "drift": True}, [])

    monkeypatch.setattr(pool, "build_monthly_plan", revalidate)
    try:
        with pytest.raises(pool.MonthlyPoolError, match="source changed"):
            pool.execute_monthly_plan(reader, plan, expected_plan_hash=plan.plan_hash)
        with writer.cursor() as cur:
            cur.execute("SELECT value FROM public.review_monthly_source")
            assert cur.fetchone()[0] == 11  # Reader rollback never undoes collection.
    finally:
        reader.rollback()
        reader.close()
        with writer.cursor() as cur:
            cur.execute("DROP TABLE public.review_monthly_source")
        writer.close()
