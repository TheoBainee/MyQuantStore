"""Installation / statut d'une entrée crontab utilisateur (bloc marqué)."""

from __future__ import annotations

import subprocess
from pathlib import Path

from myquantstore.schedule.common import (
    CRON_BEGIN,
    CRON_END,
    DEFAULT_CRON,
    resolve_binary,
    shell_join_exec,
)


def render_cron_line(
    *,
    schedule: str = DEFAULT_CRON,
    binary: str | None = None,
    fetch_args: str = "",
    log_path: Path | None = None,
) -> str:
    bin_path = binary or resolve_binary()
    extra = ["schedule", "run"]
    if fetch_args.strip():
        # cron: passer les args après run ; shlex déjà dans runner via --fetch-args
        extra.extend(["--fetch-args", fetch_args.strip()])
    cmd = shell_join_exec(bin_path, *extra)
    log = log_path or (Path.home() / ".local" / "share" / "myquantstore" / "logs" / "schedule.log")
    # PATH minimal → binary absolu déjà résolu
    return f"{schedule} {cmd} >> {log} 2>&1"


def render_cron_block(
    *,
    schedule: str = DEFAULT_CRON,
    binary: str | None = None,
    fetch_args: str = "",
) -> str:
    line = render_cron_line(schedule=schedule, binary=binary, fetch_args=fetch_args)
    return f"{CRON_BEGIN}\n{line}\n{CRON_END}\n"


def merge_crontab(existing: str, block: str) -> str:
    """Remplace le bloc MYQUANTSTORE s'il existe, sinon l'ajoute."""
    cleaned = strip_myquantstore_block(existing).rstrip()
    if cleaned and not cleaned.endswith("\n"):
        cleaned += "\n"
    if cleaned:
        cleaned += "\n"
    return cleaned + block


def strip_myquantstore_block(crontab: str) -> str:
    lines = crontab.splitlines(keepends=True)
    out: list[str] = []
    skipping = False
    for line in lines:
        stripped = line.strip()
        if stripped == CRON_BEGIN:
            skipping = True
            continue
        if stripped == CRON_END:
            skipping = False
            continue
        if not skipping:
            out.append(line)
    return "".join(out)


def install_cron(
    *,
    schedule: str = DEFAULT_CRON,
    fetch_args: str = "",
    binary: str | None = None,
    dry_run: bool = False,
) -> dict[str, object]:
    block = render_cron_block(schedule=schedule, binary=binary, fetch_args=fetch_args)
    current = _read_crontab()
    merged = merge_crontab(current, block)
    if dry_run:
        return {"backend": "cron", "dry_run": True, "crontab": merged, "block": block}
    _write_crontab(merged)
    return {"backend": "cron", "schedule": schedule, "block": block}


def uninstall_cron(*, dry_run: bool = False) -> dict[str, object]:
    current = _read_crontab()
    cleaned = strip_myquantstore_block(current)
    if dry_run:
        return {"backend": "cron", "dry_run": True, "crontab": cleaned}
    if cleaned.strip() == current.strip() and CRON_BEGIN not in current:
        return {"backend": "cron", "removed": False}
    if cleaned.strip():
        _write_crontab(cleaned if cleaned.endswith("\n") else cleaned + "\n")
    else:
        # crontab vide : supprimer
        subprocess.run(["crontab", "-r"], capture_output=True, check=False)
    return {"backend": "cron", "removed": True}


def cron_status() -> dict[str, object]:
    current = _read_crontab()
    if CRON_BEGIN not in current:
        return {"installed": False, "backend": "cron"}
    line = None
    in_block = False
    for raw in current.splitlines():
        s = raw.strip()
        if s == CRON_BEGIN:
            in_block = True
            continue
        if s == CRON_END:
            break
        if in_block and s and not s.startswith("#"):
            line = s
            break
    return {"installed": True, "backend": "cron", "line": line}


def _read_crontab() -> str:
    proc = subprocess.run(
        ["crontab", "-l"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        # no crontab for user
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
