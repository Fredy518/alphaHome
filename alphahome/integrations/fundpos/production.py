"""Fail-closed orchestration for the fund position estimation engine.

AlphaHome owns the upstream data refresh and production schedule.  The fundpos
wheel remains in an isolated Python 3.12 runtime until its solver stack is
validated against AlphaHome's Python 3.13 environment.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

FAMILY_TO_ENGINE = {
    "fixed_income_plus": "fixed_income_plus",
    "enhanced_index": "equity",
    "convertible_dominant": "convertible_dominant",
}


class FundposProductionError(RuntimeError):
    """Raised when a production gate fails."""


@dataclass(frozen=True)
class CommandOutput:
    args: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str


CommandRunner = Callable[[Sequence[str], Path, int], CommandOutput]


def _path(value: str | os.PathLike[str]) -> Path:
    return Path(os.path.expandvars(os.path.expanduser(str(value)))).resolve()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def parse_cli_json(output: str) -> dict[str, Any]:
    """Return the final JSON object from CLI output that may contain progress."""

    decoder = json.JSONDecoder()
    candidates: list[dict[str, Any]] = []
    for index, char in enumerate(output):
        if char != "{":
            continue
        try:
            value, end = decoder.raw_decode(output[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and not output[index + end :].strip():
            candidates.append(value)
    if not candidates:
        raise FundposProductionError(
            "Fundpos command did not return a final JSON object"
        )
    return candidates[-1]


def _default_command_runner(
    args: Sequence[str], cwd: Path, timeout_seconds: int
) -> CommandOutput:
    environment = os.environ.copy()
    environment["PYTHONUTF8"] = "1"
    environment["PYTHONNOUSERSITE"] = "1"
    environment.pop("PYTHONPATH", None)
    completed = subprocess.run(
        list(args),
        cwd=str(cwd),
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout_seconds,
        check=False,
    )
    return CommandOutput(
        tuple(str(value) for value in args),
        completed.returncode,
        completed.stdout,
        completed.stderr,
    )


@dataclass(frozen=True)
class FundposProductionConfig:
    project_root: Path
    python_executable: Path
    engine_config: Path
    release_manifest: Path
    expected_revision: str
    expected_tag: str
    expected_package_version: str
    state_dir: Path
    mode: str
    scope: str
    families: tuple[str, ...]
    minimum_count_coverage: float
    timeout_seconds: int
    excel: bool
    expected_universe_counts: Mapping[str, int]
    expected_scope_versions: Mapping[str, str]
    non_applicable_reasons: Mapping[str, tuple[str, ...]]
    publication_keys: Mapping[str, str]
    publication_validations: Mapping[str, str]

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> FundposProductionConfig:
        required = {
            "project_root",
            "python_executable",
            "engine_config",
            "release_manifest",
            "expected_revision",
            "expected_tag",
            "expected_package_version",
            "state_dir",
        }
        missing = sorted(required - values.keys())
        if missing:
            raise FundposProductionError(
                "Missing fundpos production settings: " + ", ".join(missing)
            )
        project_root = _path(values["project_root"])
        engine_config = Path(str(values["engine_config"]))
        if not engine_config.is_absolute():
            engine_config = project_root / engine_config
        families = tuple(values.get("families", FAMILY_TO_ENGINE))
        unknown = sorted(set(families) - FAMILY_TO_ENGINE.keys())
        if unknown:
            raise FundposProductionError(
                "Unknown fundpos families: " + ", ".join(unknown)
            )
        if not families or len(families) != len(set(families)):
            raise FundposProductionError(
                "Fundpos families must be non-empty and unique"
            )
        coverage = float(values.get("minimum_count_coverage", 0.80))
        if not 0 < coverage <= 1:
            raise FundposProductionError("minimum_count_coverage must be in (0, 1]")
        mode = str(values.get("mode", "shadow"))
        if mode not in {"check", "shadow", "publish"}:
            raise FundposProductionError("mode must be check, shadow, or publish")
        revision = str(values["expected_revision"])
        if len(revision) != 40 or any(
            c not in "0123456789abcdef" for c in revision.lower()
        ):
            raise FundposProductionError(
                "expected_revision must be a full Git commit hash"
            )
        expected_counts = {
            str(key): int(value)
            for key, value in dict(values.get("expected_universe_counts", {})).items()
        }
        expected_scopes = {
            str(key): str(value)
            for key, value in dict(values.get("expected_scope_versions", {})).items()
        }
        for setting_name, mapping in (
            ("expected_universe_counts", expected_counts),
            ("expected_scope_versions", expected_scopes),
        ):
            missing_families = sorted(set(families) - mapping.keys())
            if missing_families:
                raise FundposProductionError(
                    f"{setting_name} lacks enabled families: "
                    + ", ".join(missing_families)
                )
        if any(value <= 0 for value in expected_counts.values()):
            raise FundposProductionError("expected_universe_counts must be positive")
        non_applicable = {
            str(key): tuple(str(reason) for reason in value)
            for key, value in dict(values.get("non_applicable_reasons", {})).items()
        }
        unknown_reason_families = sorted(set(non_applicable) - set(families))
        if unknown_reason_families:
            raise FundposProductionError(
                "non_applicable_reasons has disabled families: "
                + ", ".join(unknown_reason_families)
            )
        return cls(
            project_root=project_root,
            python_executable=_path(values["python_executable"]),
            engine_config=engine_config.resolve(),
            release_manifest=_path(values["release_manifest"]),
            expected_revision=revision.lower(),
            expected_tag=str(values["expected_tag"]),
            expected_package_version=str(values["expected_package_version"]),
            state_dir=_path(values["state_dir"]),
            mode=mode,
            scope=str(values.get("scope", "v3-pilot")),
            families=families,
            minimum_count_coverage=coverage,
            timeout_seconds=int(values.get("timeout_seconds", 7200)),
            excel=bool(values.get("excel", True)),
            expected_universe_counts=expected_counts,
            expected_scope_versions=expected_scopes,
            non_applicable_reasons=non_applicable,
            publication_keys=dict(values.get("publication_keys", {})),
            publication_validations=dict(values.get("publication_validations", {})),
        )


def load_production_config(path: str | os.PathLike[str]) -> FundposProductionConfig:
    config_path = _path(path)
    payload = json.loads(config_path.read_text(encoding="utf-8-sig"))
    values = payload.get("fundpos", payload)
    if not isinstance(values, Mapping):
        raise FundposProductionError("fundpos production config must be an object")
    return FundposProductionConfig.from_mapping(values)


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    """Hold a non-blocking process lock without adding a runtime dependency."""

    path.parent.mkdir(parents=True, exist_ok=True)
    stream = path.open("a+b")
    stream.seek(0, os.SEEK_END)
    if stream.tell() == 0:
        stream.write(b"0")
        stream.flush()
    stream.seek(0)
    try:
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:  # pragma: no cover - Windows is the production host.
            import fcntl

            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        stream.close()
        raise FundposProductionError(
            "Another fundpos production run is active"
        ) from exc
    try:
        yield
    finally:
        stream.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        else:  # pragma: no cover
            import fcntl

            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        stream.close()


def assess_run_manifest(
    manifest: Mapping[str, Any],
    *,
    family: str,
    minimum_count_coverage: float,
    expected_universe_count: int | None = None,
    expected_scope_version: str | None = None,
    expected_revision: str | None = None,
    non_applicable_reasons: Sequence[str] = (),
) -> dict[str, Any]:
    expected_family = FAMILY_TO_ENGINE[family]
    if manifest.get("model_family") != expected_family:
        raise FundposProductionError(
            f"Run family mismatch: {manifest.get('model_family')} != {expected_family}"
        )
    status_counts = manifest.get("status_counts")
    if not isinstance(status_counts, Mapping) or not status_counts:
        raise FundposProductionError("Run manifest has no status_counts")
    total = sum(int(value) for value in status_counts.values())
    if expected_universe_count is not None and total != expected_universe_count:
        raise FundposProductionError(
            f"{family} universe count {total} differs from frozen scope "
            f"count {expected_universe_count}"
        )
    if expected_scope_version is not None and (
        manifest.get("scope_version") != expected_scope_version
    ):
        raise FundposProductionError(
            f"{family} scope version differs: {manifest.get('scope_version')} != "
            f"{expected_scope_version}"
        )
    if expected_revision is not None and (
        str(manifest.get("git_revision", "")).lower() != expected_revision.lower()
    ):
        raise FundposProductionError(
            f"{family} run was not produced by the frozen revision"
        )
    unavailable = int(status_counts.get("unavailable", 0))
    available = total - unavailable
    reason_counts = manifest.get("reason_counts", {})
    if not isinstance(reason_counts, Mapping):
        raise FundposProductionError("Run manifest reason_counts must be an object")
    not_applicable = sum(
        int(reason_counts.get(reason, 0)) for reason in non_applicable_reasons
    )
    if not_applicable > unavailable:
        raise FundposProductionError("Non-applicable count exceeds unavailable count")
    applicable_total = total - not_applicable
    coverage = available / applicable_total if applicable_total else 0.0
    raw_coverage = available / total if total else 0.0
    if coverage + 1e-12 < minimum_count_coverage:
        raise FundposProductionError(
            f"{family} count coverage {coverage:.2%} is below "
            f"{minimum_count_coverage:.2%}"
        )
    provenance = manifest.get("provenance", {})
    if (
        provenance.get("provider") != "alphadb"
        or provenance.get("sql_mode") != "read_only"
    ):
        raise FundposProductionError(
            "Production input must be a read-only AlphaDB snapshot"
        )
    if not manifest.get("valuation_date") or not manifest.get("information_cutoff"):
        raise FundposProductionError(
            "Run manifest lacks valuation date or information cutoff"
        )
    return {
        "valuation_date": str(manifest["valuation_date"]),
        "information_cutoff": str(manifest["information_cutoff"]),
        "total_count": total,
        "available_count": available,
        "applicable_count": applicable_total,
        "not_applicable_count": not_applicable,
        "count_coverage": coverage,
        "raw_count_coverage": raw_coverage,
        "status_counts": dict(status_counts),
        "reason_counts": dict(reason_counts),
        "complete": bool(manifest.get("complete", False)),
        "input_hash": manifest.get("input_hash"),
        "code_hash": manifest.get("code_hash"),
    }


class FundposProductionRunner:
    def __init__(
        self,
        config: FundposProductionConfig,
        *,
        command_runner: CommandRunner = _default_command_runner,
    ) -> None:
        self.config = config
        self.command_runner = command_runner

    def _run(self, args: Sequence[str], *, json_output: bool = False) -> Any:
        output = self.command_runner(
            [str(value) for value in args],
            self.config.project_root,
            self.config.timeout_seconds,
        )
        if output.returncode:
            detail = (output.stderr or output.stdout)[-4000:].strip()
            raise FundposProductionError(
                f"Command failed ({output.returncode}): {' '.join(output.args)}\n{detail}"
            )
        return parse_cli_json(output.stdout) if json_output else output.stdout.strip()

    def _engine(self, *args: str) -> dict[str, Any]:
        return self._run(
            [
                str(self.config.python_executable),
                "-I",
                "-m",
                "fundpos.cli",
                *args,
            ],
            json_output=True,
        )

    def preflight(self) -> dict[str, Any]:
        for path, label in (
            (self.config.project_root / ".git", "fundpos Git repository"),
            (self.config.python_executable, "fundpos Python runtime"),
            (self.config.engine_config, "fundpos engine config"),
            (self.config.release_manifest, "fundpos release manifest"),
        ):
            if not path.exists():
                raise FundposProductionError(f"Missing {label}: {path}")
        head = self._run(["git", "rev-parse", "HEAD"]).strip().lower()
        if head != self.config.expected_revision:
            raise FundposProductionError(
                f"Fundpos revision drift: {head} != {self.config.expected_revision}"
            )
        worktree = self._run(["git", "status", "--porcelain"])
        if worktree:
            raise FundposProductionError("Fundpos worktree is not clean")
        tagged = self._run(["git", "rev-list", "-n", "1", self.config.expected_tag])
        if tagged.strip().lower() != head:
            raise FundposProductionError(
                "Fundpos release tag does not point to expected revision"
            )
        probe_code = (
            "import importlib.metadata,json,sys;"
            "import cvxpy,osqp,psycopg;"
            "print(json.dumps({'python':list(sys.version_info[:3]),"
            "'package':importlib.metadata.version('fund-industry-position'),"
            "'cvxpy':cvxpy.__version__,'osqp':osqp.__version__}))"
        )
        runtime = parse_cli_json(
            self._run([str(self.config.python_executable), "-I", "-c", probe_code])
        )
        if runtime["python"][:2] != [3, 12]:
            raise FundposProductionError("Fundpos runtime must use Python 3.12")
        if runtime["package"] != self.config.expected_package_version:
            raise FundposProductionError("Installed fundpos package version differs")
        release = json.loads(self.config.release_manifest.read_text(encoding="utf-8"))
        if release.get("source_revision", "").lower() != head:
            raise FundposProductionError("Release manifest source revision differs")
        if release.get("package_version") != self.config.expected_package_version:
            raise FundposProductionError("Release manifest package version differs")
        if release.get("python_version") != ".".join(map(str, runtime["python"])):
            raise FundposProductionError("Release manifest Python version differs")
        artifact = _path(release["wheel_path"])
        if not artifact.exists() or _sha256(artifact) != release.get("wheel_sha256"):
            raise FundposProductionError("Fundpos wheel is missing or its hash differs")
        migrations = self._engine(
            "db-migrate", "--config", str(self.config.engine_config)
        )
        statuses = {row["status"] for row in migrations.get("migrations", [])}
        if not statuses or statuses != {"applied"}:
            raise FundposProductionError(
                f"Fundpos migrations are not current: {statuses}"
            )
        return {
            "source_revision": head,
            "source_tag": self.config.expected_tag,
            "runtime": runtime,
            "wheel_sha256": release["wheel_sha256"],
            "migration_count": len(migrations["migrations"]),
        }

    def _estimate(self, family: str, date: str, cutoff: str | None) -> dict[str, Any]:
        arguments = [
            "estimate",
            "--config",
            str(self.config.engine_config),
            "--family",
            FAMILY_TO_ENGINE[family],
            "--scope",
            self.config.scope,
            "--date",
            date,
            "--report",
        ]
        if cutoff:
            arguments.extend(["--cutoff", cutoff])
        if not self.config.excel:
            arguments.append("--no-excel")
        return self._engine(*arguments)

    def _run_family(
        self, family: str, *, date: str, cutoff: str | None, mode: str
    ) -> dict[str, Any]:
        estimate = self._estimate(family, date, cutoff)
        run_path = _path(estimate["run"])
        manifest_path = run_path / "manifest.json"
        if not manifest_path.exists():
            raise FundposProductionError(f"Missing run manifest: {manifest_path}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        assessment = assess_run_manifest(
            manifest,
            family=family,
            minimum_count_coverage=self.config.minimum_count_coverage,
            expected_universe_count=self.config.expected_universe_counts[family],
            expected_scope_version=self.config.expected_scope_versions[family],
            expected_revision=self.config.expected_revision,
            non_applicable_reasons=self.config.non_applicable_reasons.get(family, ()),
        )
        ingest = self._engine(
            "ingest",
            "--config",
            str(self.config.engine_config),
            "--run",
            str(run_path),
            "--commit",
        )
        reconciliation = self._engine(
            "reconcile",
            "--config",
            str(self.config.engine_config),
            "--run",
            str(run_path),
        )
        if reconciliation.get("status") != "passed":
            raise FundposProductionError(f"{family} reconciliation did not pass")
        publication = None
        if mode == "publish":
            validation = self.config.publication_validations.get(family)
            publication_key = self.config.publication_keys.get(family)
            if not validation or not publication_key:
                raise FundposProductionError(
                    f"{family} has no approved daily validation/publication configuration"
                )
            publication = self._engine(
                "publish",
                "--config",
                str(self.config.engine_config),
                "--run",
                str(run_path),
                "--validation",
                str(_path(validation)),
                "--key",
                publication_key,
            )
        return {
            "family": family,
            "run": str(run_path),
            "run_id": manifest["run_id"],
            "assessment": assessment,
            "ingest": ingest,
            "reconciliation": reconciliation,
            "publication": publication,
        }

    def _record_shadow_day(self, result: Mapping[str, Any]) -> dict[str, Any]:
        completed_families = {item["family"] for item in result.get("families", [])}
        expected_families = set(self.config.families)
        if completed_families != expected_families:
            return {
                "status": "partial_smoke_not_counted",
                "completed_families": sorted(completed_families),
                "required_families": sorted(expected_families),
            }
        dates = {
            item["assessment"]["valuation_date"] for item in result.get("families", [])
        }
        if len(dates) != 1:
            raise FundposProductionError(
                "All fundpos families must use one valuation date"
            )
        valuation_date = dates.pop()
        path = self.config.state_dir / "observation.json"
        ledger = (
            json.loads(path.read_text(encoding="utf-8"))
            if path.exists()
            else {"kind": "alphahome_fundpos_shadow", "days": {}}
        )
        ledger["days"][valuation_date] = {
            "recorded_at": result["finished_at"],
            "source_revision": result["preflight"]["source_revision"],
            "mode": result["mode"],
            "runs": {item["family"]: item["run_id"] for item in result["families"]},
        }
        ledger["successful_distinct_valuation_days"] = len(ledger["days"])
        ledger["required_shadow_days"] = 10
        ledger["status"] = (
            "shadow_days_collected_pending_operational_review"
            if len(ledger["days"]) >= 10
            else "observing"
        )
        _atomic_json(path, ledger)
        return ledger

    def run(
        self,
        *,
        mode: str | None = None,
        date: str = "latest",
        cutoff: str | None = None,
        families: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        selected_mode = mode or self.config.mode
        if selected_mode not in {"check", "shadow", "publish"}:
            raise FundposProductionError("mode must be check, shadow, or publish")
        selected_families = tuple(families or self.config.families)
        unknown = sorted(set(selected_families) - set(self.config.families))
        if unknown:
            raise FundposProductionError(
                "Families are not enabled by production config: " + ", ".join(unknown)
            )
        started_at = datetime.now().astimezone().isoformat()
        summary: dict[str, Any] = {
            "status": "running",
            "mode": selected_mode,
            "date_request": date,
            "cutoff_request": cutoff,
            "started_at": started_at,
            "families": [],
        }
        self.config.state_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S_%f")
        try:
            with _exclusive_lock(self.config.state_dir / ".run.lock"):
                summary["preflight"] = self.preflight()
                if selected_mode != "check":
                    for family in selected_families:
                        summary["families"].append(
                            self._run_family(
                                family,
                                date=date,
                                cutoff=cutoff,
                                mode=selected_mode,
                            )
                        )
                summary["status"] = "passed"
                summary["finished_at"] = datetime.now().astimezone().isoformat()
                if selected_mode in {"shadow", "publish"}:
                    summary["observation"] = self._record_shadow_day(summary)
        # Scheduled runs must persist unexpected solver, I/O, and database failures.
        except Exception as exc:  # noqa: BLE001
            summary.update(
                status="failed",
                finished_at=datetime.now().astimezone().isoformat(),
                error_type=type(exc).__name__,
                error=str(exc),
            )
        _atomic_json(self.config.state_dir / "runs" / f"{timestamp}.json", summary)
        _atomic_json(self.config.state_dir / "latest.json", summary)
        return summary
