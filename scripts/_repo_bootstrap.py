"""Bind repository CLI scripts to the source tree in this checkout.

Developer machines can retain editable-install ``.pth`` files for older clones.
Every direct repository script calls :func:`bootstrap_repo_src` before importing
``self_healthy_kafka`` so those global paths cannot silently select stale code.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import runpy
import sys
from pathlib import Path
from types import ModuleType

PACKAGE_NAME = "self_healthy_kafka"
ROOT = Path(__file__).resolve().parents[1]
SRC = (ROOT / "src").resolve()


class RepoImportError(RuntimeError):
    """Raised when a repository CLI cannot guarantee local source isolation."""


def _resolved_path(value: str | os.PathLike[str]) -> Path:
    return Path(value).resolve(strict=False)


def _is_within(path: Path, parent: Path) -> bool:
    path_key = os.path.normcase(str(path))
    parent_key = os.path.normcase(str(parent))
    return path_key == parent_key or parent_key in {
        os.path.normcase(str(candidate)) for candidate in path.parents
    }


def _module_locations(module: ModuleType) -> tuple[Path, ...]:
    locations: list[Path] = []
    raw_origin = getattr(module, "__file__", None)
    if raw_origin:
        locations.append(_resolved_path(raw_origin))
    for raw_path in getattr(module, "__path__", ()):
        locations.append(_resolved_path(raw_path))
    return tuple(locations)


def bootstrap_repo_src() -> Path:
    """Put this checkout's ``src`` first and verify package resolution.

    If another checkout's package was imported before this function ran, Python
    cannot safely replace all of its already-loaded submodules. Failing with an
    actionable error is safer than running a mixed checkout.
    """

    expected_package = SRC / PACKAGE_NAME / "__init__.py"
    if not expected_package.is_file():
        raise RepoImportError(f"Repository package is missing: {expected_package}")

    package_prefix = PACKAGE_NAME + "."
    for module_name, loaded in tuple(sys.modules.items()):
        if loaded is None or not (
            module_name == PACKAGE_NAME or module_name.startswith(package_prefix)
        ):
            continue
        locations = _module_locations(loaded)
        if not locations or any(not _is_within(location, SRC) for location in locations):
            rendered_locations = ", ".join(str(location) for location in locations)
            raise RepoImportError(
                f"{module_name!r} was already imported from "
                f"{rendered_locations or 'an unknown location'}; "
                f"expected this checkout under {SRC}. Start a fresh Python process."
            )

    retained_paths: list[str] = []
    for entry in sys.path:
        candidate = entry or os.getcwd()
        try:
            if _resolved_path(candidate) == SRC:
                continue
        except (OSError, RuntimeError, ValueError):
            pass
        retained_paths.append(entry)
    sys.path[:] = [str(SRC), *retained_paths]
    importlib.invalidate_caches()

    spec = importlib.util.find_spec(PACKAGE_NAME)
    origin = _resolved_path(spec.origin) if spec is not None and spec.origin else None
    if origin is None or not _is_within(origin, SRC):
        raise RepoImportError(
            f"Unable to bind {PACKAGE_NAME!r} to {SRC}; resolver selected "
            f"{origin or 'no package'}."
        )
    return ROOT


def verify_repo_package() -> Path:
    """Import and return the verified package origin for launcher diagnostics."""

    bootstrap_repo_src()
    module = importlib.import_module(PACKAGE_NAME)
    locations = _module_locations(module)
    if not locations or any(not _is_within(location, SRC) for location in locations):
        rendered_locations = ", ".join(str(location) for location in locations)
        raise RepoImportError(
            f"Imported {PACKAGE_NAME!r} from {rendered_locations or 'an unknown location'}; "
            f"expected this checkout under {SRC}."
        )
    if sys.version_info < (3, 12):
        raise RepoImportError(
            f"Python 3.12+ is required; found {sys.version.split()[0]} at {sys.executable}."
        )
    return locations[0]


def _main() -> int:
    arguments = sys.argv[1:]
    if arguments == ["--check"]:
        print(verify_repo_package())
        return 0
    if len(arguments) >= 2 and arguments[0] == "--run-module":
        module_name = arguments[1]
        if not module_name.startswith(PACKAGE_NAME + "."):
            raise RepoImportError(
                f"Refusing to run module outside {PACKAGE_NAME!r}: {module_name!r}."
            )
        verify_repo_package()
        sys.argv = [module_name, *arguments[2:]]
        runpy.run_module(module_name, run_name="__main__", alter_sys=True)
        return 0
    print(
        f"Usage: {Path(__file__).name} --check | --run-module {PACKAGE_NAME}.MODULE [ARG ...]",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(_main())
