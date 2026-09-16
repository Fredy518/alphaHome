from datetime import datetime, timezone

import pytest

from alphahome.curation import etf_candidate_confirmation as confirmation
from alphahome.curation.etf_candidate_confirmation import (
    MIGRATION_SQL,
    PIT_SCHEMA_SQL,
    review_candidate_human,
)
from alphahome.curation.etf_candidate_master import (
    CANDIDATE_WRITE_LOCK_NAME,
    CandidateMasterValidationError,
    lock_candidate_master,
)


def test_human_review_migration_preserves_rejected_history_but_filters_current_pool():
    assert "HUMAN_REJECTED" in MIGRATION_SQL
    assert "include_in_candidate_pool" in MIGRATION_SQL
    assert "etf_candidate_confirmation_audit_status" in MIGRATION_SQL


def test_candidate_pit_uses_actual_availability_and_replays_human_reviews():
    assert "etf_candidate_master_pit_history" in PIT_SCHEMA_SQL
    assert "batch_available_from" in PIT_SCHEMA_SQL
    assert "business_as_of_date" in PIT_SCHEMA_SQL
    assert "before_record" in PIT_SCHEMA_SQL
    assert "after_record" in PIT_SCHEMA_SQL
    assert "active_batches" in PIT_SCHEMA_SQL
    assert "newer.workbook_generated_on" in PIT_SCHEMA_SQL
    assert "etf_candidate_master_as_of" in PIT_SCHEMA_SQL
    assert "p_available_at" in PIT_SCHEMA_SQL


def test_human_review_reject_requires_reason_before_database_access():
    with pytest.raises(ValueError, match="requires review_note"):
        review_candidate_human(
            object(),
            fund_code="159541.SZ",
            reviewer="wuh",
            decision="reject",
        )


def test_human_review_rejects_unknown_decision_before_database_access():
    with pytest.raises(ValueError, match="approve or reject"):
        review_candidate_human(
            object(),
            fund_code="159541.SZ",
            reviewer="wuh",
            decision="maybe",
            review_note="test",
        )


class ReviewConnection:
    autocommit = False
    isolation = "read committed"

    def __init__(self):
        self.locked = False
        self.events = []
        self.changed_at = datetime(2026, 9, 16, 12, 0, tzinfo=timezone.utc)
        self.record = {
            "snapshot_id": "before_waiting_for_lock",
            "fund_code": "159541.SZ",
            "confirmation_status": "AI_CONFIRMED",
            "include_in_candidate_pool": True,
            "ai_run_id": "original_ai_run",
            "ai_decision_hash": "a" * 64,
        }

    def cursor(self):
        return ReviewCursor(self)

    def commit(self):
        self.events.append("commit")
        self.locked = False

    def rollback(self):
        self.events.append("rollback")
        self.locked = False


class ReviewCursor:
    def __init__(self, db):
        self.db = db
        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=()):
        db = self.db
        db.events.append(" ".join(sql.split()))
        if sql == "SHOW transaction_isolation":
            self.result = (db.isolation,)
        elif "pg_advisory_xact_lock" in sql:
            assert params == (CANDIDATE_WRITE_LOCK_NAME,)
            db.locked = True
            # A publication completed while this reviewer waited for the lock.
            db.record["snapshot_id"] = "published_while_waiting"
        elif "FROM fund_pool_on.etf_candidate_master_latest_batch" in sql:
            assert db.locked
            self.result = (db.record["snapshot_id"],)
        elif "SELECT to_jsonb(s)" in sql:
            assert db.locked
            assert "FOR UPDATE" in sql
            assert params == (db.record["snapshot_id"], "159541.SZ")
            self.result = (dict(db.record),)
        elif "UPDATE fund_pool_on.etf_candidate_master_snapshot" in sql:
            assert db.locked
            assert "confirmation_at = clock_timestamp()" in sql
            assert params[-2:] == (db.record["snapshot_id"], "159541.SZ")
            db.record.update(
                confirmation_status=params[0],
                confirmation_actor=params[1],
                confirmation_at=db.changed_at.isoformat(),
                human_review_note=params[2],
                manual_review_status=params[3],
                include_in_candidate_pool=params[4],
            )
            self.result = (dict(db.record),)
        elif "INSERT INTO fund_pool_on.etf_candidate_confirmation_audit" in sql:
            assert db.locked
            assert params[6].adapted["confirmation_status"] == "AI_CONFIRMED"
            assert params[7].adapted == db.record
            # PIT event time must be the post-lock mutation time, not transaction start.
            assert params[8] == db.record["confirmation_at"]
            self.result = (123, db.changed_at)
        else:
            raise AssertionError(f"unexpected SQL: {sql}")

    def fetchone(self):
        return self.result


@pytest.mark.parametrize("decision", ["approve", "reject"])
@pytest.mark.parametrize("commit", [False, True])
def test_review_locks_before_resolving_latest_snapshot_and_keeps_ai_evidence(
    monkeypatch, decision, commit
):
    db = ReviewConnection()

    def check_schema(connection):
        assert connection.locked, "lock order must precede reads of candidate views"
        return []

    monkeypatch.setattr(confirmation, "missing_confirmation_schema", check_schema)
    result = review_candidate_human(
        db,
        fund_code="159541.sz",
        reviewer=" reviewer ",
        decision=decision,
        review_note="Reviewed product facts",
        commit=commit,
    )
    assert result["snapshot_id"] == "published_while_waiting"
    assert result["confirmation_status"] == (
        "HUMAN_CONFIRMED" if decision == "approve" else "HUMAN_REJECTED"
    )
    assert result["include_in_candidate_pool"] is (decision == "approve")
    assert result["confirmation_actor"] == "reviewer"
    assert result["confirmation_at"] == db.changed_at.isoformat()
    assert result["ai_run_id"] == "original_ai_run"
    assert result["ai_decision_hash"] == "a" * 64
    assert db.locked is not commit


def test_candidate_write_lock_rejects_autocommit():
    db = ReviewConnection()
    db.autocommit = True
    with pytest.raises(CandidateMasterValidationError, match="autocommit=False"):
        lock_candidate_master(db)
    assert not db.events


@pytest.mark.parametrize("isolation", ["repeatable read", "serializable"])
def test_candidate_write_lock_rejects_stale_transaction_snapshots(isolation):
    db = ReviewConnection()
    db.isolation = isolation
    with pytest.raises(CandidateMasterValidationError, match="READ COMMITTED"):
        lock_candidate_master(db)
    assert db.events == ["SHOW transaction_isolation"]
