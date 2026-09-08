import pytest

from self_healthy_kafka.rag.models import Route
from self_healthy_kafka.rag.router import RunbookRouter, validate_route


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        ("Connector nào thường xuyên gặp sự cố nhất?", Route.ANALYTICS),
        ("Quy trình xử lý khi healing đã retry hết là gì?", Route.RUNBOOK),
        ("ORA-01017 là lỗi gì và tôi nên xử lý thế nào?", Route.COMBINED),
        ("Connector orders-source đang lỗi; dựa trên runbook cần làm gì?", Route.COMBINED),
        ("Thời gian phục hồi trung bình tuần này là bao nhiêu?", Route.ANALYTICS),
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
