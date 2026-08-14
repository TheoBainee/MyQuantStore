"""Utilitaires communs schedule (binary, backends, defaults)."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

# Samedi 01:00 heure locale
DEFAULT_ON_CALENDAR = "Sat *-*-* 01:00:00"
DEFAULT_CRON = "0 1 * * 6"

SERVICE_NAME = "myquantstore-fetch"
TIMER_NAME = "myquantstore-fetch.timer"
CRON_BEGIN = "# BEGIN MYQUANTSTORE"
CRON_END = "# END MYQUANTSTORE"


def resolve_binary() -> str:
    """Chemin absolu du binaire ``myquantstore`` (PATH) ou fallback module.

    Préfère le script installé (pipx/venv) pour un PATH minimal (cron).
    """
    which = shutil.which("myquantstore")
    if which:
        return str(Path(which).resolve())
    # Fallback: python -m myquantstore (même interpréteur)
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
    if any(c in token for c in ' \t"\'$&|;<>()'):
        return "'" + token.replace("'", "'\\''") + "'"
    return token
