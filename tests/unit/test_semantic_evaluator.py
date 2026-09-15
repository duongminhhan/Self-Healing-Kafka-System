from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "evaluate_semantic_chat.py"
DATASET = ROOT / "runbooks" / "evaluation" / "semantic_chat_cases.jsonl"


def _module():
    spec = importlib.util.spec_from_file_location("evaluate_semantic_chat", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_offline_semantic_evaluator_validates_independent_plan_contracts_only():
    module = _module()

    cases = module._load_cases(DATASET)
    report = module.evaluate_offline(cases)

    assert len(cases) <= 30
    assert report["summary"]["offline_contract_passed"] == len(cases)
    assert report["summary"]["semantic_plan_accuracy"] is None
    assert "no model inference" in report["summary"]["not_measured"][0].lower()
    assert any(item["expected_route"] == "combined" for item in report["records"])
