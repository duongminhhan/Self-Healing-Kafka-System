from __future__ import annotations

import time

from kafka import KafkaConsumer, TopicPartition


class KafkaTopicReader:
    """Small Kafka adapter used by topic monitoring domain logic."""

    def __init__(self, bootstrap_servers: list[str]):
        self._consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=None,
            enable_auto_commit=False,
            client_id="self-healthy-kafka-db-topic-lag-probe",
            api_version_auto_timeout_ms=10000,
            request_timeout_ms=15000,
            consumer_timeout_ms=1000,
        )

    def close(self) -> None:
        self._consumer.close()

    def end_offset_total(self, topic: str) -> int:
        partitions = self._consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError("topic has no partitions (does it exist?)")
        partitions_to_read = [TopicPartition(topic, item) for item in partitions]
        return sum(self._consumer.end_offsets(partitions_to_read).values())

    def end_offsets_by_topic(self, topics: list[str]) -> dict[str, int]:
        partitions_by_topic: dict[str, list[TopicPartition]] = {}
        for topic in topics:
            partitions = self._consumer.partitions_for_topic(topic)
            if partitions:
                partitions_by_topic[topic] = [
                    TopicPartition(topic, partition) for partition in partitions
                ]
        all_partitions = [
            partition
            for partitions in partitions_by_topic.values()
            for partition in partitions
        ]
        end_offsets = self._consumer.end_offsets(all_partitions) if all_partitions else {}
        return {
            topic: sum(end_offsets.get(partition, 0) for partition in partitions)
            for topic, partitions in partitions_by_topic.items()
        }

    def latest_records_by_partition(self, topic: str):
        return self.latest_records_by_topic([topic]).get(topic, [])

    def latest_records_by_topic(self, topics: list[str]) -> dict[str, list]:
        partitions_by_topic: dict[str, list[TopicPartition]] = {}
        for topic in topics:
            partitions = self._consumer.partitions_for_topic(topic)
            if partitions:
                partitions_by_topic[topic] = [
                    TopicPartition(topic, partition) for partition in partitions
                ]
        all_partitions = [
            partition
            for partitions in partitions_by_topic.values()
            for partition in partitions
        ]
        result = {topic: [] for topic in topics}
        if not all_partitions:
            return result

        end_offsets = self._consumer.end_offsets(all_partitions)
        readable = [
            partition
            for partition in all_partitions
            if end_offsets.get(partition, 0) > 0
        ]
        if not readable:
            return result

        self._consumer.assign(readable)
        for partition in readable:
            self._consumer.seek(partition, end_offsets[partition] - 1)

        pending = set(readable)
        deadline = time.monotonic() + 1.0
        while pending:
            remaining_ms = max(0, int((deadline - time.monotonic()) * 1000))
            if remaining_ms <= 0:
                break
            records = self._consumer.poll(
                timeout_ms=remaining_ms,
                max_records=len(pending),
            )
            for partition, candidates in records.items():
                if partition not in pending or not candidates:
                    continue
                topic_name = (
                    partition.topic
                    if hasattr(partition, "topic")
                    else str(partition[0])
                )
                result[topic_name].append(candidates[-1])
                pending.remove(partition)
        return result
