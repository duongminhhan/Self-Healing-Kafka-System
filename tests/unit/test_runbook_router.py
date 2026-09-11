import pytest

from self_healthy_kafka.rag.models import Route
from self_healthy_kafka.rag.router import RunbookRouter, validate_route


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        ("Connector nào thường xuyên gặp sự cố nhất?", Route.ANALYTICS),
        ("Quy trình xử lý khi healing đã retry hết là gì?", Route.RUNBOOK),
        ("ORA-01017 là lỗi gì và tôi nên xử lý thế nào?", Route.RUNBOOK),
        ("Connector orders-source đang lỗi; dựa trên runbook cần làm gì?", Route.COMBINED),
        ("Thời gian phục hồi trung bình tuần này là bao nhiêu?", Route.ANALYTICS),
        ("Nội dung lỗi đầy đủ của mã lỗi ORA-01013 là gì?", Route.ANALYTICS),
        ("Schema Registry trả về HTTP-401, tôi nên xử lý thế nào?", Route.COMBINED),
    ],
)
def test_router_understands_non_schema_wording(question, expected):
    assert RunbookRouter().route(question).route is expected


def test_router_preserves_exact_identifiers():
    decision = RunbookRouter().route("Connector orders-source lỗi ORA-01017 và SINK_WRITER_TIMEOUT")

    assert decision.connector_name == "orders-source"
    assert decision.error_codes == ("ORA-01017", "SINK_WRITER_TIMEOUT")


def test_invalid_model_router_output_is_rejected_locally():
    with pytest.raises(ValueError, match="analytics, runbook or combined"):
        validate_route({"route": "write_sql", "error_codes": []})


def test_error_remediation_does_not_depend_on_analytics_planning():
    decision = RunbookRouter().route("Cách xử lý khi gặp lỗi ORA-01013 là gì?")

    assert decision.route is Route.RUNBOOK
    assert decision.error_codes == ("ORA-01013",)


def test_current_connector_remediation_still_uses_combined_route():
    decision = RunbookRouter().route(
        "Connector sample-oracle-orders đang lỗi ORA-01013, xử lý thế nào?"
    )

    assert decision.route is Route.COMBINED


def test_multi_domain_question_does_not_filter_to_the_first_named_class():
    decision = RunbookRouter().route(
        "Runbook nào áp dụng cho lỗi xác thực của Oracle hay Schema Registry?"
    )

    assert decision.route is Route.RUNBOOK
    assert decision.connector_class is None
