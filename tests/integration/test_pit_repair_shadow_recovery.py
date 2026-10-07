"""Rehearse the same short rename and restore sequence in the isolated DB."""
import psycopg2
import pytest

pytestmark=[pytest.mark.integration,pytest.mark.requires_db]


def test_shadow_cutover_restore_and_transaction_rollback_preserve_original_oid(isolated_database_url):
    c=psycopg2.connect(isolated_database_url)
    try:
        with c.cursor() as cur:
            for name in ('repair_original','repair_shadow','repair_backup'):
                cur.execute('SELECT to_regclass(%s)',('public.'+name,));assert cur.fetchone()[0] is None
            cur.execute('CREATE MATERIALIZED VIEW public.repair_original AS SELECT 10 AS value')
            cur.execute('CREATE MATERIALIZED VIEW public.repair_shadow AS SELECT 20 AS value')
            cur.execute("SELECT 'public.repair_original'::regclass::oid");old_oid=cur.fetchone()[0]
            cur.execute('SAVEPOINT before_cutover')
            cur.execute("SET LOCAL lock_timeout='250ms'")
            cur.execute('ALTER MATERIALIZED VIEW public.repair_original RENAME TO repair_backup')
            cur.execute('ALTER MATERIALIZED VIEW public.repair_shadow RENAME TO repair_original')
            cur.execute('SELECT value FROM public.repair_original');assert cur.fetchone()[0]==20
            cur.execute('ALTER MATERIALIZED VIEW public.repair_original RENAME TO repair_shadow')
            cur.execute('ALTER MATERIALIZED VIEW public.repair_backup RENAME TO repair_original')
            cur.execute("SELECT 'public.repair_original'::regclass::oid");assert cur.fetchone()[0]==old_oid
            cur.execute('ROLLBACK TO SAVEPOINT before_cutover')
            cur.execute("SELECT 'public.repair_original'::regclass::oid");assert cur.fetchone()[0]==old_oid
            cur.execute('SELECT value FROM public.repair_original');assert cur.fetchone()[0]==10
    finally:c.rollback();c.close()
