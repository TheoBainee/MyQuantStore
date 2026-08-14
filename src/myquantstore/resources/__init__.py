"""Templates embarqués (config, .env) accessibles hors clone via importlib.resources."""

from __future__ import annotations

from importlib import resources
from pathlib import Path

_PKG = "myquantstore.resources"


def read_resource_text(name: str) -> str:
    """Lit un fichier texte du package resources.

    :param name: Nom du fichier (ex: ``config.minimal.toml``).
    :raises FileNotFoundError: Si la resource est absente.
    """
    root = resources.files(_PKG)
    target = root.joinpath(name)
    if not target.is_file():
        raise FileNotFoundError(f"Resource introuvable: {_PKG}/{name}")
    return target.read_text(encoding="utf-8")


def resource_path(name: str) -> Path:
    """Chemin filesystem d'une resource (context manager as_file si zip).

    Pour usage simple lecture seule, préférer :func:`read_resource_text`.
    """
    root = resources.files(_PKG)
    target = root.joinpath(name)
    if not target.is_file():
        raise FileNotFoundError(f"Resource introuvable: {_PKG}/{name}")
    # Traversable may be in zip; as_file materializes if needed.
    with resources.as_file(target) as path:
        return Path(path)
