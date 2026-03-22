"""Packaging wrapper for the blobodbc SQLite extension."""

import pathlib

_HERE = pathlib.Path(__file__).parent


def extension_path() -> str:
    """Return the absolute path to the blobodbc SQLite extension (without suffix).

    SQLite's .load command does not want the file extension:
        .load <path>
    """
    base = _HERE / "blobodbc"
    for suffix in (".so", ".dylib", ".dll"):
        if (base.parent / f"blobodbc{suffix}").exists():
            return str(base)
    raise FileNotFoundError(f"Extension not found at {base}.*")
