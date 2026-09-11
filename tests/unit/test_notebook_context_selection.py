from notebooks.shared.context_selection import normalize_text, select_context_for_question
from notebooks.shared.few_shot import select_few_shot_messages


def _context(table_count=6):
    tables = {
        "ConnectorHealingQueue": [
            {"name": "QueueId", "primary_key_position": 1},
            {"name": "RootConnectorName", "primary_key_position": 0},
            {"name": "QueueStatus", "primary_key_position": 0},
            {"name": "ReceivedAt", "primary_key_position": 0},
        ],
        "ConnectorHealingLogs": [
            {"name": "Id", "primary_key_position": 1},
            {"name": "QueueId", "primary_key_position": 0},
            {"name": "EventType", "primary_key_position": 0},
            {"name": "CreatedAt", "primary_key_position": 0},
        ],
    }
    for index in range(max(0, table_count - 2)):
        tables[f"Unrelated{index}"] = [
            {"name": "Id", "primary_key_position": 1},
            {"name": "Payload", "primary_key_position": 0},
        ]
    return {
        "dialect": "sqlite",
        "tables": tables,
        "relationships": [
            {
                "from_table": "ConnectorHealingLogs",
                "from_column": "QueueId",
                "to_table": "ConnectorHealingQueue",
                "to_column": "QueueId",
            }
        ],
        "business_definitions": {
            "ConnectorHealingQueue": {"grain": "one incident"},
            "ConnectorHealingLogs": {"grain": "one healing event"},
        },
        "semantic_catalog": {
            "entities": {
                "ConnectorHealingQueue": {
                    "dimensions": ["RootConnectorName", "QueueStatus"],
                    "timestamps": {"ReceivedAt": "receipt"},
                },
                "ConnectorHealingLogs": {
                    "dimensions": ["EventType"],
                    "timestamps": {"CreatedAt": "event"},
                },
            }
        },
        "categorical_observations": {
            "ConnectorHealingQueue.QueueStatus": {"observed_values": ["COMPLETED"]},
            "Unrelated0.Payload": {"observed_values": ["SECRET-LIKE-NOISE"]},
        },
        "snapshot": {"file_name": "fixture.db"},
    }


def test_normalization_handles_plain_vietnamese_and_accents():
    assert normalize_text("Sự cố của kết nối") == "su co cua ket noi"


def test_schema_selection_excludes_unrelated_tables_and_keeps_join_keys():
    selected = select_context_for_question(
        _context(),
        "Connector nào có nhiều healing log nhất?",
        max_tables=2,
        max_columns_per_table=3,
    )

    assert set(selected["tables"]) == {
        "ConnectorHealingQueue",
        "ConnectorHealingLogs",
    }
    assert all(
        any(column["name"] == "QueueId" for column in columns)
        for columns in selected["tables"].values()
    )
    assert "Unrelated0.Payload" not in selected["categorical_observations"]
    assert selected["context_selection"]["strategy"] == (
        "deterministic_catalog_lexical_v1"
    )


def test_dynamic_few_shot_selection_is_bounded_and_intent_driven():
    messages, example_ids = select_few_shot_messages(
        "plan",
        "Thời gian phục hồi trung bình mất bao lâu?",
        max_examples=2,
    )

    assert len(messages) == 4
    assert example_ids[0] == "plan_duration"
    assert len(example_ids) == len(set(example_ids)) == 2


def test_response_examples_select_null_shape_without_reading_gold_answers():
    _, example_ids = select_few_shot_messages(
        "response",
        "Trạng thái hiện tại là gì?",
        result={"rows": [{"queue_status": None}]},
        max_examples=1,
    )

    assert example_ids == ["response_null_value"]
