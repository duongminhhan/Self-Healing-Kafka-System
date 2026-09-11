from notebooks.evaluation.evaluate_multi_turn_context import evaluate


def test_all_structured_multi_turn_gold_cases_pass():
    report = evaluate()

    assert report["kind"] == "mocked_structured_context_contract"
    assert report["live_model"] is False
    assert report["live_database"] is False
    assert report["pass_count"] == report["case_count"]
