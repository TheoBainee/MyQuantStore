"""Installation / statut d'un timer systemd --user."""

from __future__ import annotations

import contextlib
import subprocess
from pathlib import Path

from myquantstore.schedule.common import (
    DEFAULT_ON_CALENDAR,
    SERVICE_NAME,
    TIMER_NAME,
    resolve_binary,
    shell_join_exec,
)


def user_unit_dir() -> Path:
    return Path.home() / ".config" / "systemd" / "user"


def render_service_unit(
    *,
    binary: str | None = None,
    fetch_args: str = "",
) -> str:
    bin_path = binary or resolve_binary()
    extra = ["schedule", "run"]
    if fetch_args.strip():
        extra.extend(["--fetch-args", fetch_args.strip()])
    exec_start = shell_join_exec(bin_path, *extra)
    return (
        "[Unit]\n"
        "Description=MyQuantStore OHLCV fetch + aggregate + health check\n"
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


def render_timer_unit(*, on_calendar: str = DEFAULT_ON_CALENDAR) -> str:
    return (
        "[Unit]\n"
        "Description=MyQuantStore periodic historisation timer\n"
        "\n"
        "[Timer]\n"
        f"OnCalendar={on_calendar}\n"
        "Persistent=true\n"
        "Unit=myquantstore-fetch.service\n"
        "\n"
        "[Install]\n"
        "WantedBy=timers.target\n"
    )


def install_systemd(
    *,
    on_calendar: str = DEFAULT_ON_CALENDAR,
    fetch_args: str = "",
    binary: str | None = None,
    dry_run: bool = False,
) -> dict[str, object]:
    unit_dir = user_unit_dir()
    service_path = unit_dir / f"{SERVICE_NAME}.service"
    timer_path = unit_dir / TIMER_NAME
    service_body = render_service_unit(binary=binary, fetch_args=fetch_args)
    timer_body = render_timer_unit(on_calendar=on_calendar)

    if dry_run:
        return {
            "backend": "systemd",
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
    _systemctl("enable", "--now", TIMER_NAME)

    return {
        "backend": "systemd",
        "service_path": service_path,
        "timer_path": timer_path,
        "on_calendar": on_calendar,
        "enabled": True,
    }


def uninstall_systemd(*, dry_run: bool = False) -> dict[str, object]:
    unit_dir = user_unit_dir()
    service_path = unit_dir / f"{SERVICE_NAME}.service"
    timer_path = unit_dir / TIMER_NAME
    if dry_run:
        return {"backend": "systemd", "dry_run": True, "removed": [service_path, timer_path]}

    with contextlib.suppress(RuntimeError):
        _systemctl("disable", "--now", TIMER_NAME)
    removed: list[Path] = []
    for p in (timer_path, service_path):
        if p.exists():
            p.unlink()
            removed.append(p)
    with contextlib.suppress(RuntimeError):
        _systemctl("daemon-reload")
    return {"backend": "systemd", "removed": removed}


def systemd_status() -> dict[str, object]:
    unit_dir = user_unit_dir()
    service_path = unit_dir / f"{SERVICE_NAME}.service"
    timer_path = unit_dir / TIMER_NAME
    installed = service_path.exists() and timer_path.exists()
    if not installed:
        return {"installed": False, "backend": "systemd"}

    enabled = _systemctl_bool("is-enabled", TIMER_NAME)
    active = _systemctl_bool("is-active", TIMER_NAME)
    next_run = _timer_next()
    return {
        "installed": True,
        "backend": "systemd",
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


def _timer_next() -> str | None:
    try:
        out = _systemctl("list-timers", TIMER_NAME, "--no-pager")
    except RuntimeError:
        return None
    lines = [ln for ln in out.splitlines() if TIMER_NAME in ln]
    if not lines:
        return None
    # First columns: NEXT LEFT LAST PASSED UNIT ACTIVATES
    return " ".join(lines[0].split()[:2]) if lines[0].split() else lines[0].strip()
