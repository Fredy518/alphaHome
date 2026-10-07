"""Resume one frozen Massive backfill after bounded transient network failures."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
DELAYS = (60, 180, 600, 1800, 3600, 3600)


def now():
    return datetime.now(timezone.utc).isoformat()


def read_state(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def save_state(path, state):
    state["updated_at"] = now()
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    temporary.replace(path)


def retryable(state, plan_hash, attempt_started):
    # Do not reinterpret an old network failure when a new invocation fails
    # before collection (for example, changed code, credentials or plan hash).
    if state.get("plan_hash") != plan_hash or state.get("status") != "failed":
        return False
    try:
        if datetime.fromisoformat(state["updated_at"]) < attempt_started:
            return False
    except (KeyError, TypeError, ValueError):
        return False
    error = state.get("error", "")
    return "Massive 网络请求失败" in error or bool(
        re.search(r"Massive HTTP (?:429|5\d\d)(?!\d)", error)
    )


def progress(state):
    return sum(state.get("completed", {}).get(kind, 0) for kind in ("daily", "basic"))


def supervise(run_dir, plan_hash):
    state_path = run_dir / "supervisor.json"
    child_state_path = run_dir / "status.json"
    state = {
        "pid": os.getpid(),
        "started_at": now(),
        "plan_hash": plan_hash,
        "status": "running",
        "attempt": 0,
        "consecutive_retries": 0,
    }
    command = [
        sys.executable,
        "-u",
        str(ROOT / "scripts/production/backfill_massive_us.py"),
        "--run-dir",
        str(run_dir),
        "--execute",
        "--expected-plan-hash",
        plan_hash,
    ]
    environment = {**os.environ, "PYTHONUTF8": "1", "TQDM_DISABLE": "1"}
    while True:
        before = read_state(child_state_path)
        started = datetime.now(timezone.utc)
        state.update(status="running", attempt=state["attempt"] + 1)
        state.pop("retry_at", None)
        with (
            (run_dir / "stdout.log").open("a", encoding="utf-8") as stdout,
            (run_dir / "stderr.log").open("a", encoding="utf-8") as stderr,
        ):
            child = subprocess.Popen(
                command,
                cwd=ROOT,
                env=environment,
                stdout=stdout,
                stderr=stderr,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            state["child_pid"] = child.pid
            save_state(state_path, state)
            while True:
                try:
                    code = child.wait(timeout=30)
                    break
                except subprocess.TimeoutExpired:
                    save_state(state_path, state)
        after = read_state(child_state_path)
        state["child_exit_code"] = code
        if (
            code == 0
            and after.get("status") == "completed"
            and after.get("plan_hash") == plan_hash
        ):
            state.update(status="completed", finished_at=now())
            save_state(state_path, state)
            return 0
        if not retryable(after, plan_hash, started):
            state.update(
                status="failed", reason="non_transient_failure", finished_at=now()
            )
            save_state(state_path, state)
            return 1
        if progress(after) > progress(before):
            state["consecutive_retries"] = 0
        retries = state["consecutive_retries"]
        if retries >= len(DELAYS):
            state.update(
                status="failed",
                reason="transient_retry_budget_exhausted",
                finished_at=now(),
            )
            save_state(state_path, state)
            return 1
        delay = DELAYS[retries]
        state.update(
            status="backoff",
            consecutive_retries=retries + 1,
            retry_at=datetime.fromtimestamp(
                time.time() + delay, timezone.utc
            ).isoformat(),
        )
        save_state(state_path, state)
        deadline = time.monotonic() + delay
        while time.monotonic() < deadline:
            time.sleep(max(0, min(30, deadline - time.monotonic())))
            save_state(state_path, state)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--expected-plan-hash", required=True)
    args = parser.parse_args()
    run_dir = Path(args.run_dir).resolve(strict=True)
    # Windows advisory file lock prevents duplicate supervisors for this run;
    # the collector separately holds a database-wide Massive backfill lock.
    import msvcrt

    with (run_dir / "supervisor.lock").open("a+b") as lock:
        if lock.tell() == 0:
            lock.write(b"0")
            lock.flush()
        lock.seek(0)
        msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        sys.exit(supervise(run_dir, args.expected_plan_hash))
