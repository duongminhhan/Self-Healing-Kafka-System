from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from kafka.errors import KafkaError

from self_healthy_kafka.health.kafka_topic_reader import KafkaTopicReader
from self_healthy_kafka.monitoring.metrics import (
    record_topic_lag_state,
    sync_active_topic_labels,
)
from self_healthy_kafka.storage.mssql import HealingRepository

logger = logging.getLogger(__name__)


class DbTopicLagProbe:
    """Checks active topic_lag_jobs and records threshold transitions."""

    def __init__(
        self,
        *,
        bootstrap_servers: list[str],
        db: HealingRepository,
        default_idle_threshold_seconds: int,
        event_capture_lag_threshold_ms: int,
        topic_idle_enabled: bool = True,
        event_capture_enabled: bool = False,
    ):
        self._bootstrap = bootstrap_servers
        self._db = db
        self._default_threshold = default_idle_threshold_seconds
        self._topic_idle_enabled = topic_idle_enabled
        self._event_capture_enabled = event_capture_enabled
        self._event_capture_lag_threshold_ms = event_capture_lag_threshold_ms
        self._reader: Optional[KafkaTopicReader] = None
        self._condition_over_threshold: dict[tuple[str, str, str], bool] = {}
        self._latest_records: dict[str, list[Any]] = {}

    def enabled(self) -> bool:
        return bool(
            self._bootstrap
            and (self._topic_idle_enabled or self._event_capture_enabled)
        )

    def tick(
        self,
        *,
        source_server: str | None = None,
        check_event_latency: bool = False,
    ) -> None:
        if not self.enabled():
            return
        topics = self._db.list_monitored_topics(self._default_threshold)
        now = datetime.now(timezone.utc)
        if source_server is None:
            sync_active_topic_labels(topics, self._enabled_conditions(check_event_latency))
            for item in topics:
                _sync_topic_lag_state_from_db(
                    item,
                    now,
                    conditions=self._enabled_conditions(check_event_latency),
                )
        if source_server:
            topics = [item for item in topics if _matches_source_server(item, source_server)]
        if not topics:
            return

        try:
            self._ensure_consumer()
        except KafkaError as e:
            logger.warning(
                f"topic lag probe cannot reach Kafka: {e}",
                extra={"event": "topic_probe_failed", "reason": str(e)},
            )
            return

        totals = self._end_offsets_for_topics(topics)
        topics_to_read = [
            str(item["topic_name"])
            for item in topics
            if str(item["topic_name"]) not in self._latest_records
            or item.get("last_end_offset") is None
            or totals.get(str(item["topic_name"]), -1)
            > int(item.get("last_end_offset") or 0)
        ]
        if topics_to_read:
            try:
                self._latest_records.update(
                    self._latest_records_for_topics(topics_to_read)
                )
            except Exception as e:
                logger.warning(
                    f"latest topic record batch fetch failed: {e}",
                    extra={"event": "topic_probe_failed", "reason": str(e)},
                )

        for item in topics:
            topic = item["topic_name"]
            total = totals.get(topic)
            if total is None:
                logger.warning(
                    f"[{topic}] end-offset fetch failed: topic has no partitions",
                    extra={
                        "event": "topic_probe_failed",
                        "topic": topic,
                        "reason": "topic has no partitions",
                    },
                )
                continue
            records = self._latest_records.get(topic)
            latest_message_at = None
            last_offset = item.get("last_end_offset")
            if last_offset is None or total > int(last_offset):
                try:
                    latest_message_at = self._message_at_from_records(records or [])
                except Exception as e:
                    logger.warning(
                        f"[{topic}] latest record timestamp fetch failed: {e}",
                        extra={
                            "event": "topic_probe_failed",
                            "topic": topic,
                            "reason": str(e),
                        },
                    )
                    continue
            self._evaluate_topic_lag_state(
                item,
                total,
                now,
                latest_message_at=latest_message_at,
                latest_records=records,
                check_event_latency=check_event_latency,
            )

    def close(self) -> None:
        if self._reader is not None:
            try:
                self._reader.close()
            except Exception:
                pass
            self._reader = None

    def _ensure_consumer(self) -> None:
        if self._reader is not None:
            return
        self._reader = KafkaTopicReader(self._bootstrap)

    def _end_offset_total(self, topic: str) -> int:
        assert self._reader is not None
        return self._reader.end_offset_total(topic)

    def _end_offsets_for_topics(
        self,
        topics: list[dict[str, Any]],
    ) -> dict[str, int]:
        if "_end_offset_total" in self.__dict__:
            result: dict[str, int] = {}
            for item in topics:
                topic = str(item["topic_name"])
                try:
                    result[topic] = self._end_offset_total(topic)
                except Exception as e:
                    logger.warning(
                        f"[{topic}] end-offset fetch failed: {e}",
                        extra={
                            "event": "topic_probe_failed",
                            "topic": topic,
                            "reason": str(e),
                        },
                    )
            return result
        assert self._reader is not None
        return self._reader.end_offsets_by_topic(
            [str(item["topic_name"]) for item in topics]
        )

    def _latest_records_for_topics(self, topics: list[str]) -> dict[str, list[Any]]:
        assert self._reader is not None
        return self._reader.latest_records_by_topic(topics)

    def _enabled_conditions(
        self,
        check_event_latency: bool = False,
    ) -> tuple[str, ...]:
        conditions: list[str] = []
        if self._topic_idle_enabled:
            conditions.append("topic_idle")
        if self._event_capture_enabled or check_event_latency:
            conditions.append("event_capture_time")
        return tuple(conditions)

    @staticmethod
    def _message_at_from_records(records: list[Any]) -> datetime | None:
        timestamps = [
            timestamp
            for timestamp in (_record_message_timestamp_ms(record) for record in records)
            if timestamp is not None
        ]
        if not timestamps:
            return None
        return datetime.fromtimestamp(max(timestamps) / 1000, tz=timezone.utc)

    def _evaluate_topic_idle(
        self,
        item: dict[str, Any],
        total: int,
        now: datetime,
        *,
        latest_message_at: datetime | None = None,
    ) -> None:
        self._evaluate_topic_lag_state(
            item,
            total,
            now,
            latest_message_at=latest_message_at,
        )

    def _evaluate_topic_lag_state(
        self,
        item: dict[str, Any],
        total: int,
        now: datetime,
        *,
        latest_message_at: datetime | None = None,
        latest_records: list[Any] | None = None,
        check_event_latency: bool = False,
    ) -> None:
        last_offset = item.get("last_end_offset")
        last_message_at = _parse_dt(item.get("last_message_at")) or now
        last_observed_at = _parse_dt(item.get("updated_at")) or last_message_at
        threshold = int(item["idle_threshold_seconds"])
        offset_increased = last_offset is None or total > int(last_offset)
        was_over_threshold = bool(item.get("is_over_threshold"))
        confirmed_new_message = offset_increased and latest_message_at is not None
        stale_over_threshold_message = (
            was_over_threshold
            and confirmed_new_message
            and latest_message_at <= last_observed_at
        )
        message_advances_idle_state = (
            confirmed_new_message and not stale_over_threshold_message
        )
        current_message_at = latest_message_at if confirmed_new_message else last_message_at
        next_last_message_at = (
            current_message_at if message_advances_idle_state else last_message_at
        )
        evaluated_last_message_at = (
            current_message_at if message_advances_idle_state else last_message_at
        )
        messages_per_second = _messages_per_second(
            previous_offset=last_offset,
            current_offset=total,
            previous_observed_at=last_observed_at,
            current_observed_at=now,
        )
        condition_states: dict[str, dict[str, Any]] = {}
        snapshot_record_info = (
            self._record_info_from_records(latest_records)
            if latest_records is not None
            else None
        )

        if self._topic_idle_enabled:
            condition_states["topic_idle"] = self._evaluate_topic_idle_condition(
                item,
                total,
                now,
                offset_increased=offset_increased,
                last_message_at=evaluated_last_message_at,
                threshold=threshold,
            )

        if self._event_capture_enabled or check_event_latency:
            try:
                condition_states["event_capture_time"] = (
                    self._evaluate_event_capture_condition(
                        item,
                        total,
                        record_info=snapshot_record_info,
                    )
                )
            except Exception as e:
                logger.warning(
                    f"[{item['topic_name']}] event-capture lag evaluation failed: {e}",
                    extra={
                        "event": "topic_probe_failed",
                        "topic": item["topic_name"],
                        "reason": str(e),
                        "lag_condition": "event_capture_time",
                    },
                )

        source_lag_minutes = self._source_lag_minutes(
            item["topic_name"],
            total,
            condition_states,
            record_info=snapshot_record_info,
        )
        for condition, state in condition_states.items():
            record_topic_lag_state(
                connector_name=item["connector_name"],
                topic_name=item["topic_name"],
                over_threshold=bool(state["over_threshold"]),
                lag_seconds=state["lag_seconds"],
                messages_per_second=messages_per_second,
                records_lag=source_lag_minutes,
                condition=condition,
            )

        current_over_threshold = any(
            bool(state["over_threshold"]) for state in condition_states.values()
        )
        topic_idle_over_threshold = bool(
            condition_states.get("topic_idle", {}).get("over_threshold")
        )
        condition_transitions = self._condition_transitions(
            item,
            condition_states,
            previous_topic_idle_over_threshold=was_over_threshold,
        )
        if topic_idle_over_threshold:
            condition_transitions = {
                condition: changed if condition == "topic_idle" else False
                for condition, changed in condition_transitions.items()
            }
        recovered_by_new_message = (
            was_over_threshold
            and message_advances_idle_state
            and current_message_at > last_message_at
            and current_message_at > last_observed_at
        )
        next_topic_idle_over_threshold = topic_idle_over_threshold
        if was_over_threshold and not recovered_by_new_message:
            next_topic_idle_over_threshold = True

        if confirmed_new_message or next_topic_idle_over_threshold != was_over_threshold:
            self._db.update_topic_lag_state(
                topic_lag_job_id=item["topic_lag_job_id"],
                last_end_offset=total,
                last_message_at=next_last_message_at,
                is_over_threshold=next_topic_idle_over_threshold,
            )

        if recovered_by_new_message:
            previous_idle_seconds = max(
                0,
                int((current_message_at - last_message_at).total_seconds()),
            )
            self._record_topic_lag(
                item,
                event_status="RECOVERED",
                message=f"Topic {item['topic_name']} recovered",
                end_offset=total,
                details={
                    **_transition_details(
                        _cleared_condition_states(condition_states),
                        event_status="RECOVERED",
                        previous_last_end_offset=last_offset,
                        current_last_end_offset=total,
                        messages_per_second=messages_per_second,
                        records_lag=source_lag_minutes,
                    ),
                    "idle_seconds": previous_idle_seconds,
                    "lag_seconds": previous_idle_seconds,
                    "previous_message_at": last_message_at.isoformat(),
                    "current_message_at": current_message_at.isoformat(),
                    "lag_time_source": (
                        "kafka_record_timestamp"
                        if latest_message_at is not None
                        else "poll_observed_at"
                    ),
                },
            )

        if (
            any(condition_transitions.values())
            or (recovered_by_new_message and current_over_threshold)
        ):
            over_threshold_states = _filter_condition_states(
                condition_states,
                [
                    condition
                    for condition, changed in condition_transitions.items()
                    if changed
                ],
            )
            if not over_threshold_states and recovered_by_new_message:
                over_threshold_states = condition_states
            self._record_topic_lag(
                item,
                event_status="OVER_THRESHOLD",
                message=_over_threshold_message(item, over_threshold_states),
                end_offset=total,
                details=_transition_details(
                    over_threshold_states,
                    event_status="OVER_THRESHOLD",
                    previous_last_end_offset=last_offset,
                    current_last_end_offset=total,
                    messages_per_second=messages_per_second,
                    records_lag=source_lag_minutes,
                ),
            )
            _log_topic_lag_warning(item, total, over_threshold_states)

        aggregate_lag_seconds = max(
            [float(state["lag_seconds"]) for state in condition_states.values()] or [0]
        )
        _log_topic_lag_state(
            item,
            total=total,
            idle_seconds=int(aggregate_lag_seconds),
            threshold=threshold,
            over_threshold=current_over_threshold,
            messages_per_second=messages_per_second,
        )

    def _evaluate_topic_idle_condition(
        self,
        item: dict[str, Any],
        total: int,
        now: datetime,
        *,
        offset_increased: bool,
        last_message_at: datetime,
        threshold: int,
    ) -> dict[str, Any]:
        idle_seconds = int((now - last_message_at).total_seconds())
        if offset_increased and idle_seconds < threshold:
            return {
                "over_threshold": False,
                "lag_seconds": 0,
                "threshold_seconds": threshold,
                "message": "topic received new messages",
            }

        return {
            "over_threshold": idle_seconds >= threshold,
            "lag_seconds": max(0, idle_seconds),
            "threshold_seconds": threshold,
            "milliseconds_since_last_event": max(0, idle_seconds) * 1000,
            "message": f"no new messages for {idle_seconds}s",
        }

    def _evaluate_event_capture_condition(
        self,
        item: dict[str, Any],
        total: int,
        *,
        record_info: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if total <= 0:
            return {
                "over_threshold": False,
                "lag_seconds": 0,
                "threshold_ms": self._event_capture_lag_threshold_ms,
                "message": "topic has no records",
            }
        record_info = record_info or self._latest_partition_record_info(
            item["topic_name"]
        )
        lag_ms = record_info.get("event_capture_lag_ms")
        over_threshold = (
            lag_ms is not None
            and lag_ms >= self._event_capture_lag_threshold_ms
        )
        return {
            **record_info,
            "over_threshold": over_threshold,
            "lag_seconds": (lag_ms or 0) / 1000,
            "threshold_ms": self._event_capture_lag_threshold_ms,
            "message": (
                f"event capture lag is {lag_ms}ms"
                if lag_ms is not None
                else str(record_info.get("reason") or "event timestamp unavailable")
            ),
        }

    def _source_lag_minutes(
        self,
        topic: str,
        total: int,
        condition_states: dict[str, dict[str, Any]],
        *,
        record_info: dict[str, Any] | None = None,
    ) -> float:
        for state in condition_states.values():
            lag_ms = state.get("event_capture_lag_ms")
            if lag_ms is not None:
                return _milliseconds_to_minutes(lag_ms)
        if total <= 0 or self._reader is None:
            return 0.0
        try:
            record_info = record_info or self._latest_partition_record_info(topic)
        except Exception as e:
            logger.warning(
                f"[{topic}] source lag metric evaluation failed: {e}",
                extra={
                    "event": "topic_probe_failed",
                    "topic": topic,
                    "reason": str(e),
                    "lag_metric": "records_lag",
                },
            )
            return 0.0
        return _milliseconds_to_minutes(record_info.get("event_capture_lag_ms"))

    def _condition_transitions(
        self,
        item: dict[str, Any],
        condition_states: dict[str, dict[str, Any]],
        *,
        previous_topic_idle_over_threshold: bool,
    ) -> dict[str, bool]:
        transitions: dict[str, bool] = {}
        connector_name = str(item.get("connector_name") or "unknown")
        topic_name = str(item["topic_name"])
        for condition, state in condition_states.items():
            current = bool(state.get("over_threshold"))
            key = (connector_name, topic_name, condition)
            previous = (
                previous_topic_idle_over_threshold
                if condition == "topic_idle"
                else self._condition_over_threshold.get(key, False)
            )
            transitions[condition] = current and not previous
            self._condition_over_threshold[key] = current
        return transitions

    def _latest_partition_record_info(self, topic: str) -> dict[str, Any]:
        assert self._reader is not None
        records = self._reader.latest_records_by_partition(topic)
        return self._record_info_from_records(records)

    @staticmethod
    def _record_info_from_records(records: list[Any] | None) -> dict[str, Any]:
        records = records or []
        infos = [
            _record_event_capture_info(record)
            for record in records
            if record is not None
        ]
        with_lag = [
            info for info in infos if info.get("event_capture_lag_ms") is not None
        ]
        if with_lag:
            selected = max(
                with_lag,
                key=lambda info: int(info.get("event_capture_lag_ms") or 0),
            )
            return {
                **selected,
                "partition_count_checked": len(records),
            }
        if infos:
            selected = max(
                infos,
                key=lambda info: int(info.get("kafka_timestamp_ms") or 0),
            )
            return {
                **selected,
                "partition_count_checked": len(records),
            }
        return {
            "event_capture_lag_ms": None,
            "partition_count_checked": 0,
            "reason": "topic has no readable records",
        }

    def _record_topic_lag(
        self,
        item: dict[str, Any],
        *,
        event_status: str,
        message: str,
        end_offset: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self._db.record_topic_lag_log(
            topic_lag_job_id=item.get("topic_lag_job_id"),
            connector_id=item.get("connector_id"),
            job_name=item.get("job_name") or "",
            connector_name=item.get("connector_name"),
            topic_name=item["topic_name"],
            end_offset=end_offset,
            event_status=event_status,
            message=message,
            details=details or {},
        )


DbTopicIdleProbe = DbTopicLagProbe


def _parse_dt(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _decode_json(value: bytes | None) -> dict[str, Any] | None:
    if not value:
        return None
    if value[:1] == b"\x00":
        return None
    try:
        decoded = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return decoded if isinstance(decoded, dict) else None


def _extract_source_ts_ms(payload: dict[str, Any] | None) -> int | None:
    if not payload:
        return None
    candidates = [
        payload.get("payload", {}).get("source", {}).get("ts_ms"),
        payload.get("source", {}).get("ts_ms"),
    ]
    return _first_int(candidates)


def _extract_capture_ts_ms(payload: dict[str, Any] | None) -> int | None:
    if not payload:
        return None
    candidates = [
        payload.get("payload", {}).get("ts_ms"),
        payload.get("ts_ms"),
    ]
    return _first_int(candidates)


def _record_event_capture_info(record) -> dict[str, Any]:
    decoded = _decode_json(record.value)
    source_ts_ms = _extract_source_ts_ms(decoded)
    capture_ts_ms = _extract_capture_ts_ms(decoded) or record.timestamp
    base = {
        "partition": getattr(record, "partition", None),
        "offset": getattr(record, "offset", None),
        "kafka_timestamp_ms": getattr(record, "timestamp", None),
        "decode_supported": decoded is not None,
    }
    if source_ts_ms is None or capture_ts_ms is None:
        return {
            **base,
            "event_capture_lag_ms": None,
            "reason": "source or capture timestamp not available",
        }
    return {
        **base,
        "db_event_at_ms": source_ts_ms,
        "kafka_captured_at_ms": capture_ts_ms,
        "event_capture_lag_ms": max(0, int(capture_ts_ms) - int(source_ts_ms)),
    }


def _record_message_timestamp_ms(record) -> int | None:
    decoded = _decode_json(record.value)
    return _extract_capture_ts_ms(decoded) or getattr(record, "timestamp", None)


def _transition_details(
    condition_states: dict[str, dict[str, Any]],
    *,
    event_status: str,
    previous_last_end_offset: Any,
    current_last_end_offset: int,
    messages_per_second: float = 0,
    records_lag: int = 0,
) -> dict[str, Any]:
    active_conditions = [
        condition
        for condition, state in condition_states.items()
        if state.get("over_threshold")
    ]
    lag_seconds_by_condition = {
        condition: state.get("lag_seconds", 0)
        for condition, state in condition_states.items()
    }
    condition_details = {
        condition: {
            key: value
            for key, value in state.items()
            if key != "over_threshold"
        }
        for condition, state in condition_states.items()
    }
    details = {
        "lag_condition": _event_condition(active_conditions, event_status),
        "active_conditions": active_conditions,
        "lag_seconds": max(
            [float(value or 0) for value in lag_seconds_by_condition.values()] or [0],
        ),
        "lag_seconds_by_condition": lag_seconds_by_condition,
        "condition_details": condition_details,
        "previous_last_end_offset": previous_last_end_offset,
        "current_last_end_offset": current_last_end_offset,
        "records_lag": records_lag,
        "messages_per_second": messages_per_second,
    }
    if len(active_conditions) == 1:
        details.update(condition_details.get(active_conditions[0], {}))
    return details


def _cleared_condition_states(
    condition_states: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {
        condition: {
            **state,
            "over_threshold": False,
            "lag_seconds": 0,
            "message": "topic received new messages",
        }
        for condition, state in condition_states.items()
    }


def _filter_condition_states(
    condition_states: dict[str, dict[str, Any]],
    conditions: list[str],
) -> dict[str, dict[str, Any]]:
    selected = set(conditions)
    return {
        condition: state
        for condition, state in condition_states.items()
        if condition in selected
    }


def _event_condition(active_conditions: list[str], event_status: str) -> str:
    if len(active_conditions) == 1:
        return active_conditions[0]
    if len(active_conditions) > 1:
        return "mixed"
    return "topic_lag" if event_status == "RECOVERED" else "unknown"


def _over_threshold_message(
    item: dict[str, Any],
    condition_states: dict[str, dict[str, Any]],
) -> str:
    active = [
        f"{condition}: {state.get('message')}"
        for condition, state in condition_states.items()
        if state.get("over_threshold")
    ]
    suffix = "; ".join(active) if active else "unknown lag condition"
    return f"Topic {item['topic_name']} is over threshold ({suffix})"


def _log_topic_lag_warning(
    item: dict[str, Any],
    total: int,
    condition_states: dict[str, dict[str, Any]],
) -> None:
    active_conditions = [
        condition
        for condition, state in condition_states.items()
        if state.get("over_threshold")
    ]
    logger.warning(
        f"[{item['topic_name']}] OVER_THRESHOLD - conditions={active_conditions}",
        extra={
            "event": "TOPIC_IDLE_WARNING",
            "connector_name": item["connector_name"],
            "topic": item["topic_name"],
            "active_conditions": active_conditions,
            "condition_details": condition_states,
            "end_offset": total,
        },
    )


def _first_int(values: list[Any]) -> int | None:
    for value in values:
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _messages_per_second(
    *,
    previous_offset: Any,
    current_offset: int,
    previous_observed_at: datetime | None,
    current_observed_at: datetime,
) -> float:
    if previous_offset is None or previous_observed_at is None:
        return 0.0
    try:
        offset_delta = current_offset - int(previous_offset)
    except (TypeError, ValueError):
        return 0.0
    if offset_delta <= 0:
        return 0.0
    elapsed_seconds = (current_observed_at - previous_observed_at).total_seconds()
    if elapsed_seconds <= 0:
        return 0.0
    return offset_delta / elapsed_seconds


def _milliseconds_to_minutes(milliseconds: Any) -> float:
    try:
        return max(0.0, float(milliseconds or 0) / 60000)
    except (TypeError, ValueError):
        return 0.0


def _matches_source_server(item: dict[str, Any], source_server: str) -> bool:
    config = item.get("config_template") or {}
    if isinstance(config, dict) and isinstance(config.get("config"), dict):
        config = config["config"]
    topic_prefix = config.get("topic.prefix") if isinstance(config, dict) else None
    return bool(
        source_server
        and (
            source_server == topic_prefix
            or str(item.get("topic_name") or "").startswith(f"{source_server}.")
        )
    )


def _log_topic_lag_state(
    item: dict[str, Any],
    *,
    total: int,
    idle_seconds: int,
    threshold: int,
    over_threshold: bool,
    messages_per_second: float,
) -> None:
    logger.debug(
        "topic lag state",
        extra={
            "event": "topic_lag_state",
            "connector_name": item["connector_name"],
            "topic": item["topic_name"],
            "over_threshold": 1 if over_threshold else 0,
            "idle_seconds": idle_seconds,
            "threshold_seconds": threshold,
            "end_offset": total,
            "messages_per_second": messages_per_second,
        },
    )


def _sync_topic_lag_state_from_db(
    item: dict[str, Any],
    now: datetime,
    *,
    conditions: tuple[str, ...] = ("topic_idle",),
) -> None:
    last_message_at = _parse_dt(item.get("last_message_at"))
    idle_seconds = int((now - last_message_at).total_seconds()) if last_message_at else 0
    aggregate_condition = conditions[0] if conditions else "topic_idle"
    for condition in conditions:
        record_topic_lag_state(
            connector_name=item.get("connector_name"),
            topic_name=str(item["topic_name"]),
            over_threshold=(
                bool(item.get("is_over_threshold"))
                and condition == aggregate_condition
            ),
            lag_seconds=max(0, idle_seconds) if condition == "topic_idle" else 0,
            messages_per_second=0,
            condition=condition,
        )
