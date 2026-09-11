from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

from scripts._repo_bootstrap import RepoImportError, bootstrap_repo_src

ROOT = Path(__file__).resolve().parents[2]
CLI_ENTRYPOINTS = (
    Path("scripts/delete_qdrant_test_collection.py"),
    Path("scripts/evaluate_runbook_rag.py"),
    Path("scripts/evaluate_runbook_retrieval.py"),
    Path("scripts/index_runbooks.py"),
    Path("scripts/manage_qdrant_alias.py"),
    Path("scripts/migrate_qdrant_payload_indexes.py"),
    Path("scripts/promote_qdrant_collection.py"),
    Path("scripts/retrieve_runbooks.py"),
    Path("scripts/seed_healing_samples.py"),
    Path("notebooks/evaluation/evaluate_multi_turn_context.py"),
)


@pytest.mark.parametrize("entrypoint", CLI_ENTRYPOINTS, ids=str)
def test_cli_prefers_current_checkout_over_stale_pythonpath(
    tmp_path: Path, entrypoint: Path
) -> None:
    stale_root = tmp_path / "stale-checkout" / "src"
    stale_package = stale_root / "self_healthy_kafka"
    stale_package.mkdir(parents=True)
    (stale_package / "__init__.py").write_text(
        'raise RuntimeError("stale checkout imported")\n', encoding="utf-8"
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(stale_root), env.get("PYTHONPATH", "")) if part
    )

    completed = subprocess.run(
        [sys.executable, str(ROOT / entrypoint), "--help"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "stale checkout imported" not in completed.stderr


def test_module_entrypoint_prefers_current_checkout_over_stale_pythonpath(tmp_path: Path) -> None:
    stale_root = tmp_path / "stale-checkout" / "src"
    stale_package = stale_root / "self_healthy_kafka"
    stale_package.mkdir(parents=True)
    (stale_package / "__init__.py").write_text(
        'raise RuntimeError("stale checkout imported")\n', encoding="utf-8"
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = str(stale_root)

    completed = subprocess.run(
        [sys.executable, "-m", "scripts.retrieve_runbooks", "--help"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "stale checkout imported" not in completed.stderr


def test_bootstrap_runs_repo_module_in_same_isolated_process(tmp_path: Path) -> None:
    env = _stale_environment(tmp_path)

    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "_repo_bootstrap.py"),
            "--run-module",
            "self_healthy_kafka.config",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "stale checkout imported" not in completed.stderr


def test_bootstrap_rejects_a_package_preloaded_from_another_checkout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    stale_module = ModuleType("self_healthy_kafka")
    stale_module.__file__ = str(tmp_path / "old" / "self_healthy_kafka" / "__init__.py")
    monkeypatch.setitem(sys.modules, "self_healthy_kafka", stale_module)

    with pytest.raises(RepoImportError, match="already imported"):
        bootstrap_repo_src()


def test_bootstrap_rejects_a_child_module_preloaded_from_another_checkout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    stale_module = ModuleType("self_healthy_kafka.config")
    stale_module.__file__ = str(tmp_path / "old" / "self_healthy_kafka" / "config.py")
    monkeypatch.setitem(sys.modules, "self_healthy_kafka.config", stale_module)

    with pytest.raises(RepoImportError, match="self_healthy_kafka.config.*already imported"):
        bootstrap_repo_src()


@pytest.mark.skipif(shutil.which("bash") is None, reason="Bash is not installed")
def test_bash_launcher_check_only_prefers_current_checkout(tmp_path: Path) -> None:
    env = _stale_environment(tmp_path)
    env["SELF_HEALTHY_KAFKA_ENV_FILE"] = "env/dev.env.example"
    env["SELF_HEALTHY_KAFKA_PYTHON"] = sys.executable

    completed = subprocess.run(
        [shutil.which("bash") or "bash", "scripts/run.sh", "dev", "--check-only"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "Runtime identity check passed." in completed.stdout
    assert "stale-checkout" not in completed.stdout


@pytest.mark.skipif(os.name != "nt", reason="PowerShell launcher is Windows-specific")
def test_powershell_launcher_check_only_prefers_current_checkout(tmp_path: Path) -> None:
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if powershell is None:
        pytest.skip("PowerShell is not installed")
    env = _stale_environment(tmp_path)
    env["SELF_HEALTHY_KAFKA_ENV_FILE"] = "env/dev.env.example"

    completed = subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ROOT / "scripts" / "run.ps1"),
            "dev",
            "-PythonExecutable",
            sys.executable,
            "-CheckOnly",
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "Runtime identity check passed." in completed.stdout
    assert "stale-checkout" not in completed.stdout


def _stale_environment(tmp_path: Path) -> dict[str, str]:
    stale_root = tmp_path / "stale-checkout" / "src"
    stale_package = stale_root / "self_healthy_kafka"
    stale_package.mkdir(parents=True)
    (stale_package / "__init__.py").write_text(
        'raise RuntimeError("stale checkout imported")\n', encoding="utf-8"
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(stale_root), env.get("PYTHONPATH", "")) if part
    )
    return env
