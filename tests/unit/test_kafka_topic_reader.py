from types import SimpleNamespace
from unittest.mock import MagicMock

from kafka import TopicPartition

from self_healthy_kafka.health.kafka_topic_reader import KafkaTopicReader


def test_latest_records_assigns_all_partitions_before_single_poll():
    reader = object.__new__(KafkaTopicReader)
    consumer = MagicMock()
    reader._consumer = consumer
    first = TopicPartition("topic-a", 0)
    second = TopicPartition("topic-a", 1)
    records = {
        first: [SimpleNamespace(partition=0, offset=9)],
        second: [SimpleNamespace(partition=1, offset=19)],
    }
    consumer.partitions_for_topic.return_value = {0, 1}
    consumer.end_offsets.return_value = {first: 10, second: 20}
    consumer.poll.return_value = records

    result = reader.latest_records_by_topic(["topic-a"])

    consumer.assign.assert_called_once()
    assert set(consumer.assign.call_args.args[0]) == {first, second}
    assert consumer.seek.call_count == 2
    consumer.poll.assert_called_once()
    assert {record.offset for record in result["topic-a"]} == {9, 19}


def test_end_offsets_are_fetched_in_one_batch_for_all_topics():
    reader = object.__new__(KafkaTopicReader)
    consumer = MagicMock()
    reader._consumer = consumer
    partitions = {
        "topic-a": {0, 1},
        "topic-b": {0},
    }
    consumer.partitions_for_topic.side_effect = partitions.get
    offsets = {
        TopicPartition("topic-a", 0): 10,
        TopicPartition("topic-a", 1): 20,
        TopicPartition("topic-b", 0): 7,
    }
    consumer.end_offsets.return_value = offsets

    result = reader.end_offsets_by_topic(["topic-a", "topic-b"])

    consumer.end_offsets.assert_called_once()
    assert result == {"topic-a": 30, "topic-b": 7}
