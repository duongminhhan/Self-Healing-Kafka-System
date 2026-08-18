from self_healthy_kafka.healing.offset_recovery import (
    base_connector_name,
    build_replacement_connector,
    extract_last_scn,
    has_offsets,
    is_versioned_connector_name,
    next_versioned_connector_name,
)


def test_next_versioned_connector_name_preserves_suffix_width():
    assert next_versioned_connector_name("conA") == "conA.001"
    assert next_versioned_connector_name("conA.009") == "conA.010"


def test_base_connector_name_strips_only_version_suffix():
    assert base_connector_name("conA") == "conA"
    assert base_connector_name("conA.001") == "conA"
    assert base_connector_name("natis-ora-connector_v2.001") == "natis-ora-connector_v2"
    assert base_connector_name("natis-ora-connector_v2") == "natis-ora-connector_v2"


def test_is_versioned_connector_name_requires_numeric_suffix():
    assert is_versioned_connector_name("conA.001") is True
    assert is_versioned_connector_name("conA.v2") is False
    assert is_versioned_connector_name("natis-ora-connector_v2") is False


def test_extract_last_scn_prefers_highest_scn_like_value():
    offsets = {
        "offsets": [
            {
                "partition": {"server": "oracle_cdc"},
                "offset": {
                    "scn": "123",
                    "commit_scn": "124:1:abc",
                    "snapshot_scn": "120",
                },
            }
        ]
    }

    assert extract_last_scn(offsets) == ("124", "124:1:abc")


def test_has_offsets_requires_non_empty_offsets_payload():
    assert has_offsets({"offsets": [{"offset": {"scn": "1"}}]}) is True
    assert has_offsets({"offsets": []}) is False
    assert has_offsets(None) is False


def test_build_replacement_connector_overrides_name_and_schema_history():
    connector_name, config = build_replacement_connector(
        {
            "connector_name": "conA.001",
            "active_config": {
                "connector.class": "X",
                "name": "conA.001",
                "schema.history.internal.kafka.topic": "old-history",
            },
        },
        scn="123",
    )

    assert connector_name == "conA.002"
    assert config["name"] == "conA.002"
    assert config["schema.history.internal.kafka.topic"] == "schema-history.conA.002"
    assert config["snapshot.scn"] == "123"


def test_build_replacement_connector_advances_past_failed_attempt_name():
    connector_name, config = build_replacement_connector(
        {
            "connector_name": "conA.001",
            "latest_event_details": {"new_connector_name": "conA.002"},
            "active_config": {
                "connector.class": "X",
                "name": "conA.001",
                "schema.history.internal.kafka.topic": "schema-history.conA.001",
            },
        },
        scn=None,
    )

    assert connector_name == "conA.003"
    assert config["name"] == "conA.003"
    assert config["schema.history.internal.kafka.topic"] == "schema-history.conA.003"


def test_build_replacement_connector_preserves_existing_schema_history():
    connector_name, config = build_replacement_connector(
        {
            "connector_name": "conA.001",
            "active_config": {
                "connector.class": "X",
                "name": "conA.001",
                "schema.history.internal.kafka.topic": "schema-history.conA.001",
            },
        },
        scn="123",
        preserve_schema_history=True,
    )

    assert connector_name == "conA.002"
    assert config["name"] == "conA.002"
    assert config["schema.history.internal.kafka.topic"] == "schema-history.conA.001"


def test_preserving_schema_history_requires_existing_topic_config():
    try:
        build_replacement_connector(
            {
                "connector_name": "conA.001",
                "active_config": {
                    "connector.class": "X",
                    "name": "conA.001",
                },
            },
            scn="123",
            preserve_schema_history=True,
        )
    except ValueError as exc:
        assert "Existing schema history topic is required" in str(exc)
    else:
        raise AssertionError("Expected missing schema history topic to fail")
