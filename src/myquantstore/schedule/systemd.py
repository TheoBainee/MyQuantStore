"""Installation / statut d'un timer systemd --user."""

from __future__ import annotations

import contextlib
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


def user_unit_dir() -> Path:
    return Path.home() / ".config" / "systemd" / "user"


def render_service_unit(
    *,
    job: str | JobSpec | None = None,
    binary: str | None = None,
    fetch_args: str = "",
) -> str:
    spec = get_job(job)
    bin_path = binary or resolve_binary()
    extra = list(spec.run_cli)
    if fetch_args.strip():
        extra.extend(["--fetch-args", fetch_args.strip()])
    exec_start = shell_join_exec(bin_path, *extra)
    return (
        "[Unit]\n"
        f"Description={spec.unit_description}\n"
        "After=network-online.target\n"
        "Wants=network-online.target\n"
        "\n"
        "[Service]\n"
        "Type=oneshot\n"
        f"ExecStart={exec_start}\n"
        "Nice=10\n"
        "\n"
        "[Install]\n"
        "WantedBy=default.target\n"
    )


def render_timer_unit(
    *,
    job: str | JobSpec | None = None,
    on_calendar: str | None = None,
) -> str:
    spec = get_job(job)
    calendar = on_calendar or spec.default_on_calendar
    return (
        "[Unit]\n"
        f"Description={spec.timer_description}\n"
        "\n"
        "[Timer]\n"
        f"OnCalendar={calendar}\n"
        "Persistent=true\n"
        f"Unit={spec.service_name}.service\n"
        "\n"
        "[Install]\n"
        "WantedBy=timers.target\n"
    )


def install_systemd(
    *,
    job: str | JobSpec | None = None,
    on_calendar: str | None = None,
    fetch_args: str = "",
    binary: str | None = None,
    dry_run: bool = False,
) -> dict[str, object]:
    spec = get_job(job)
    calendar = on_calendar or spec.default_on_calendar
    unit_dir = user_unit_dir()
    service_path = unit_dir / f"{spec.service_name}.service"
    timer_path = unit_dir / spec.timer_name
    service_body = render_service_unit(job=spec, binary=binary, fetch_args=fetch_args)
    timer_body = render_timer_unit(job=spec, on_calendar=calendar)

    if dry_run:
        return {
            "backend": "systemd",
            "job": spec.id,
            "dry_run": True,
            "service_path": service_path,
            "timer_path": timer_path,
            "service": service_body,
            "timer": timer_body,
        }

    unit_dir.mkdir(parents=True, exist_ok=True)
    service_path.write_text(service_body, encoding="utf-8")
    timer_path.write_text(timer_body, encoding="utf-8")

    _systemctl("daemon-reload")
    _systemctl("enable", "--now", spec.timer_name)

    return {
        "backend": "systemd",
        "job": spec.id,
        "service_path": service_path,
        "timer_path": timer_path,
        "on_calendar": calendar,
        "enabled": True,
    }


def uninstall_systemd(
    *,
    job: str | None = JOB_FETCH,
    dry_run: bool = False,
) -> dict[str, object]:
    unit_dir = user_unit_dir()
    removed: list[Path] = []
    job_ids = resolve_job_ids(job)
    if dry_run:
        paths: list[Path] = []
        for jid in job_ids:
            spec = get_job(jid)
            paths.extend((unit_dir / spec.timer_name, unit_dir / f"{spec.service_name}.service"))
        return {"backend": "systemd", "dry_run": True, "removed": paths, "jobs": list(job_ids)}

    for jid in job_ids:
        spec = get_job(jid)
        service_path = unit_dir / f"{spec.service_name}.service"
        timer_path = unit_dir / spec.timer_name
        with contextlib.suppress(RuntimeError):
            _systemctl("disable", "--now", spec.timer_name)
        for p in (timer_path, service_path):
            if p.exists():
                p.unlink()
                removed.append(p)
    with contextlib.suppress(RuntimeError):
        _systemctl("daemon-reload")
    return {"backend": "systemd", "removed": removed, "jobs": list(job_ids)}


def systemd_status(*, job: str | JobSpec | None = None) -> dict[str, object]:
    spec = get_job(job)
    unit_dir = user_unit_dir()
    service_path = unit_dir / f"{spec.service_name}.service"
    timer_path = unit_dir / spec.timer_name
    installed = service_path.exists() and timer_path.exists()
    if not installed:
        return {"installed": False, "backend": "systemd", "job": spec.id}

    enabled = _systemctl_bool("is-enabled", spec.timer_name)
    active = _systemctl_bool("is-active", spec.timer_name)
    next_run = _timer_next(spec.timer_name)
    return {
        "installed": True,
        "backend": "systemd",
        "job": spec.id,
        "enabled": enabled,
        "active": active,
        "next": next_run,
        "service_path": str(service_path),
        "timer_path": str(timer_path),
    }


def _systemctl(*args: str) -> str:
    import shutil

    if shutil.which("systemctl") is None:
        raise RuntimeError("systemctl introuvable")
    proc = subprocess.run(
        ["systemctl", "--user", *args],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if proc.returncode != 0 and args[0] not in ("is-enabled", "is-active", "disable"):
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"systemctl --user {' '.join(args)} failed: {err}")
    return (proc.stdout or "").strip()


def _systemctl_bool(*args: str) -> bool:
    try:
        proc = subprocess.run(
            ["systemctl", "--user", *args],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        return proc.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def _timer_next(timer_name: str) -> str | None:
    try:
        out = _systemctl("list-timers", timer_name, "--no-pager")
    except RuntimeError:
        return None
    lines = [ln for ln in out.splitlines() if timer_name in ln]
    if not lines:
        return None
    return " ".join(lines[0].split()[:2]) if lines[0].split() else lines[0].strip()
