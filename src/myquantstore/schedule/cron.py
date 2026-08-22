"""Installation / statut d'une entrée crontab utilisateur (bloc marqué)."""

from __future__ import annotations

import subprocess
from pathlib import Path

from myquantstore.schedule.common import (
    JOB_FETCH,
    JobSpec,
    get_job,
    resolve_binary,
    resolve_job_ids,
    shell_join_exec,
)


def render_cron_line(
    *,
    job: str | JobSpec | None = None,
    schedule: str | None = None,
    binary: str | None = None,
    fetch_args: str = "",
    log_path: Path | None = None,
) -> str:
    spec = get_job(job)
    bin_path = binary or resolve_binary()
    extra = list(spec.run_cli)
    if fetch_args.strip():
        extra.extend(["--fetch-args", fetch_args.strip()])
    cmd = shell_join_exec(bin_path, *extra)
    log = log_path or (
        Path.home() / ".local" / "share" / "myquantstore" / "logs" / spec.cron_log_name
    )
    cron_when = schedule or spec.default_cron
    return f"{cron_when} {cmd} >> {log} 2>&1"


def render_cron_block(
    *,
    job: str | JobSpec | None = None,
    schedule: str | None = None,
    binary: str | None = None,
    fetch_args: str = "",
) -> str:
    spec = get_job(job)
    line = render_cron_line(job=spec, schedule=schedule, binary=binary, fetch_args=fetch_args)
    return f"{spec.cron_begin}\n{line}\n{spec.cron_end}\n"


def merge_crontab(
    existing: str,
    block: str,
    *,
    job: str | JobSpec | None = None,
) -> str:
    """Remplace le bloc du job s'il existe, sinon l'ajoute (les autres jobs restent)."""
    cleaned = strip_job_block(existing, job).rstrip()
    if cleaned and not cleaned.endswith("\n"):
        cleaned += "\n"
    if cleaned:
        cleaned += "\n"
    return cleaned + block


def strip_job_block(crontab: str, job: str | JobSpec | None = None) -> str:
    spec = get_job(job)
    lines = crontab.splitlines(keepends=True)
    out: list[str] = []
    skipping = False
    for line in lines:
        stripped = line.strip()
        if stripped == spec.cron_begin:
            skipping = True
            continue
        if stripped == spec.cron_end:
            skipping = False
            continue
        if not skipping:
            out.append(line)
    return "".join(out)


def strip_myquantstore_block(crontab: str) -> str:
    """Compat : retire uniquement le bloc fetch."""
    return strip_job_block(crontab, JOB_FETCH)


def install_cron(
    *,
    job: str | JobSpec | None = None,
    schedule: str | None = None,
    fetch_args: str = "",
    binary: str | None = None,
    dry_run: bool = False,
) -> dict[str, object]:
    spec = get_job(job)
    cron_when = schedule or spec.default_cron
    block = render_cron_block(job=spec, schedule=cron_when, binary=binary, fetch_args=fetch_args)
    current = _read_crontab()
    merged = merge_crontab(current, block, job=spec)
    if dry_run:
        return {
            "backend": "cron",
            "job": spec.id,
            "dry_run": True,
            "crontab": merged,
            "block": block,
        }
    _write_crontab(merged)
    return {"backend": "cron", "job": spec.id, "schedule": cron_when, "block": block}


def uninstall_cron(*, job: str | None = JOB_FETCH, dry_run: bool = False) -> dict[str, object]:
    job_ids = resolve_job_ids(job)
    current = _read_crontab()
    cleaned = current
    present = False
    for jid in job_ids:
        spec = get_job(jid)
        if _block_present(cleaned, spec.cron_begin):
            present = True
        cleaned = strip_job_block(cleaned, jid)
    if dry_run:
        return {"backend": "cron", "dry_run": True, "crontab": cleaned, "jobs": list(job_ids)}
    if not present:
        return {"backend": "cron", "removed": False, "jobs": list(job_ids)}
    if cleaned.strip():
        _write_crontab(cleaned if cleaned.endswith("\n") else cleaned + "\n")
    else:
        subprocess.run(["crontab", "-r"], capture_output=True, check=False)
    return {"backend": "cron", "removed": True, "jobs": list(job_ids)}


def cron_status(*, job: str | JobSpec | None = None) -> dict[str, object]:
    spec = get_job(job)
    current = _read_crontab()
    if not _block_present(current, spec.cron_begin):
        return {"installed": False, "backend": "cron", "job": spec.id}
    line = None
    in_block = False
    for raw in current.splitlines():
        s = raw.strip()
        if s == spec.cron_begin:
            in_block = True
            continue
        if s == spec.cron_end:
            break
        if in_block and s and not s.startswith("#"):
            line = s
            break
    return {"installed": True, "backend": "cron", "job": spec.id, "line": line}


def _block_present(crontab: str, begin: str) -> bool:
    return any(line.strip() == begin for line in crontab.splitlines())


def _read_crontab() -> str:
    proc = subprocess.run(
        ["crontab", "-l"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return ""
    return proc.stdout or ""


def _write_crontab(content: str) -> None:
    proc = subprocess.run(
        ["crontab", "-"],
        input=content,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"crontab install failed: {err}")
