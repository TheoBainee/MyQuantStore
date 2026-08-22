"""Utilitaires communs schedule (jobs, binary, backends, defaults)."""

from __future__ import annotations

import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

JOB_FETCH = "fetch"
JOB_CACHES = "caches"
JOB_IDS: tuple[str, ...] = (JOB_FETCH, JOB_CACHES)


@dataclass(frozen=True)
class JobSpec:
    """Identité d'un job schedule (units systemd + bloc cron + horaires)."""

    id: str
    service_name: str
    description: str
    unit_description: str
    timer_description: str
    default_on_calendar: str
    default_cron: str
    cron_begin: str
    cron_end: str
    cron_log_name: str
    run_cli: tuple[str, ...]

    @property
    def timer_name(self) -> str:
        return f"{self.service_name}.timer"


JOB_SPECS: dict[str, JobSpec] = {
    JOB_FETCH: JobSpec(
        id=JOB_FETCH,
        service_name="myquantstore-fetch",
        description="historisation OHLCV : fetch → aggregate → status --check",
        unit_description="MyQuantStore OHLCV fetch + aggregate + health check",
        timer_description="MyQuantStore periodic historisation timer",
        default_on_calendar="Sat *-*-* 07:00:00",
        default_cron="0 7 * * 6",
        cron_begin="# BEGIN MYQUANTSTORE",
        cron_end="# END MYQUANTSTORE",
        cron_log_name="schedule.log",
        run_cli=("schedule", "run"),
    ),
    JOB_CACHES: JobSpec(
        id=JOB_CACHES,
        service_name="myquantstore-caches",
        description="refresh caches Massive : tickers + futures contracts",
        unit_description="MyQuantStore Massive cache refresh (tickers + futures contracts)",
        timer_description="MyQuantStore periodic Massive cache refresh",
        default_on_calendar="Sat *-*-* 03:00:00",
        default_cron="0 3 * * 6",
        cron_begin="# BEGIN MYQUANTSTORE-CACHES",
        cron_end="# END MYQUANTSTORE-CACHES",
        cron_log_name="schedule-caches.log",
        run_cli=("schedule", "run", "caches"),
    ),
}

DEFAULT_ON_CALENDAR = JOB_SPECS[JOB_FETCH].default_on_calendar
DEFAULT_CRON = JOB_SPECS[JOB_FETCH].default_cron
SERVICE_NAME = JOB_SPECS[JOB_FETCH].service_name
TIMER_NAME = JOB_SPECS[JOB_FETCH].timer_name
CRON_BEGIN = JOB_SPECS[JOB_FETCH].cron_begin
CRON_END = JOB_SPECS[JOB_FETCH].cron_end


def get_job(job_id: str | JobSpec | None = None) -> JobSpec:
    """Résout un job (défaut: fetch)."""
    if isinstance(job_id, JobSpec):
        return job_id
    key = job_id or JOB_FETCH
    spec = JOB_SPECS.get(key)
    if spec is None:
        raise ValueError(f"job schedule inconnu: {key}")
    return spec


def resolve_job_ids(job_id: str | None) -> tuple[str, ...]:
    """``all`` / None → les deux jobs ; sinon un seul id."""
    if job_id in (None, "all"):
        return JOB_IDS
    get_job(job_id)
    return (job_id,)


def resolve_binary() -> str:
    """Chemin absolu du binaire ``myquantstore`` (PATH) ou fallback module.

    Préfère le script installé (pipx/venv) pour un PATH minimal (cron).
    """
    which = shutil.which("myquantstore")
    if which:
        return str(Path(which).resolve())
    py = str(Path(sys.executable).resolve())
    return f"{py} -m myquantstore"


def detect_backend() -> str:
    """``systemd`` si ``systemctl --user`` répond, sinon ``cron``."""
    if shutil.which("systemctl") is None:
        return "cron"
    import subprocess

    try:
        proc = subprocess.run(
            ["systemctl", "--user", "show-environment"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
        if proc.returncode == 0:
            return "systemd"
    except (OSError, subprocess.TimeoutExpired):
        pass
    return "cron"


def shell_join_exec(binary: str, *args: str) -> str:
    """Construit la ligne ExecStart / cron (binary peut contenir espaces si python -m)."""
    parts = binary.split() + list(args)
    return " ".join(_quote_if_needed(p) for p in parts)


def _quote_if_needed(token: str) -> str:
    if not token:
        return '""'
    if any(c in token for c in " \t\"'$&|;<>()"):
        return "'" + token.replace("'", "'\\''") + "'"
    return token
