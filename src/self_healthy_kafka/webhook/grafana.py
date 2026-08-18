from __future__ import annotations

import hashlib
import json
import logging
import queue
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, urlsplit

from self_healthy_kafka.config import GrafanaWebhookConfig
from self_healthy_kafka.webhook.metrics import render_prometheus_metrics
from self_healthy_kafka.webhook.security import (
    EventDeduplicator,
    WebhookAuthenticator,
)

logger = logging.getLogger(__name__)

MAX_BODY_BYTES = 1024 * 1024
CLIENT_DISCONNECT_ERRORS = (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)


@dataclass(frozen=True)
class GrafanaAlertEvent:
    connector_name: str
    status: str
    fingerprint: str
    starts_at: str
    alert_name: str
    failure_confirmed: bool = False

    @property
    def dedupe_key(self) -> str:
        return ":".join(
            (
                self.fingerprint or self.connector_name,
                self.status,
                self.starts_at,
            )
        )


def parse_grafana_alerts(payload: dict[str, Any]) -> list[GrafanaAlertEvent]:
    events: list[GrafanaAlertEvent] = []
    group_status = str(payload.get("status") or "").lower()
    for alert in payload.get("alerts") or []:
        labels = alert.get("labels") or {}
        connector_name = (
            labels.get("connector_name")
            or labels.get("connector")
            or labels.get("server")
        )
        status = str(alert.get("status") or group_status).lower()
        if not connector_name or status not in {"firing", "resolved"}:
            continue
        events.append(
            GrafanaAlertEvent(
                connector_name=str(connector_name),
                status=status,
                fingerprint=str(alert.get("fingerprint") or ""),
                starts_at=str(alert.get("startsAt") or ""),
                alert_name=str(labels.get("alertname") or ""),
                failure_confirmed=status == "firing",
            )
        )
    return events


class GrafanaWebhookService:
    """Receives Grafana alerts and runs connector recovery outside HTTP threads."""

    def __init__(
        self,
        config: GrafanaWebhookConfig,
        process_connector: Callable[[str, bool], str | None],
        connector_activity: Callable[[], list[dict[str, Any]]] | None = None,
        topic_lag_metrics: Callable[[], list[dict[str, Any]]] | None = None,
        monitoring_inventory: Callable[
            [], dict[str, list[dict[str, Any]]]
        ] | None = None,
        monitoring_start: Callable[[], None] | None = None,
        monitoring_close: Callable[[], None] | None = None,
    ):
        self._config = config
        self._process_connector = process_connector
        self._connector_activity = connector_activity or (lambda: [])
        self._topic_lag_metrics = topic_lag_metrics or (lambda: [])
        self._monitoring_inventory = monitoring_inventory or (
            lambda: {"connectors": [], "topics": []}
        )
        self._monitoring_start = monitoring_start or (lambda: None)
        self._monitoring_close = monitoring_close or (lambda: None)
        self._queue: queue.Queue[GrafanaAlertEvent | None] = queue.Queue(
            maxsize=config.queue_size
        )
        self._authenticator = WebhookAuthenticator(config)
        self._deduplicator = EventDeduplicator(config.dedupe_ttl_seconds)
        self._followup_lock = threading.Lock()
        self._followup_timers: dict[str, threading.Timer] = {}
        self._server: ThreadingHTTPServer | None = None
        self._server_thread: threading.Thread | None = None
        self._worker_threads: list[threading.Thread] = []
        self._stopping = threading.Event()

    def start(self) -> None:
        if not self._config.enabled:
            return
        self._validate_config()
        self._monitoring_start()
        handler = self._handler_class()
        self._server = ThreadingHTTPServer(
            (self._config.host, self._config.port),
            handler,
        )
        self._server.daemon_threads = True
        self._worker_threads = [
            threading.Thread(
                target=self._run_worker,
                name=f"grafana-webhook-worker-{index + 1}",
                daemon=True,
            )
            for index in range(self._config.worker_count)
        ]
        self._server_thread = threading.Thread(
            target=self._server.serve_forever,
            name="grafana-webhook-http",
            daemon=True,
        )
        for worker in self._worker_threads:
            worker.start()
        self._server_thread.start()
        logger.info(
            "Grafana webhook receiver started",
            extra={
                "event": "grafana_webhook_started",
                "host": self._config.host,
                "port": self._config.port,
                "path": self._config.path,
                "auth_mode": self._config.auth_mode,
                "worker_count": self._config.worker_count,
            },
        )

    def close(self) -> None:
        if not self._config.enabled:
            return
        self._stopping.set()
        self._monitoring_close()
        with self._followup_lock:
            timers = list(self._followup_timers.values())
            self._followup_timers.clear()
        for timer in timers:
            timer.cancel()
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        for _worker in self._worker_threads:
            try:
                self._queue.put_nowait(None)
            except queue.Full:
                break
        if self._server_thread is not None:
            self._server_thread.join(timeout=5)
        for worker in self._worker_threads:
            worker.join(timeout=5)
        self._worker_threads.clear()

    def submit(self, payload: dict[str, Any]) -> dict[str, int]:
        accepted = 0
        duplicate = 0
        events = parse_grafana_alerts(payload)
        ignored = max(0, len(payload.get("alerts") or []) - len(events))
        for event in events:
            if not self._claim_event(event):
                duplicate += 1
                continue
            try:
                self._queue.put_nowait(event)
                accepted += 1
            except queue.Full:
                self._release_event(event)
                logger.error(
                    "Grafana webhook queue is full",
                    extra={
                        "event": "grafana_webhook_queue_full",
                        "connector_name": event.connector_name,
                    },
                )
                ignored += 1
        return {"accepted": accepted, "duplicate": duplicate, "ignored": ignored}

    def verify_request(
        self,
        headers,
        body: bytes,
        *,
        query_token: str = "",
    ) -> bool:
        return self._authenticator.verify(
            headers,
            body,
            query_token=query_token,
        )

    def _validate_config(self) -> None:
        if not self._config.path.startswith("/"):
            raise ValueError("GRAFANA_WEBHOOK_PATH must start with '/'")
        if not self._config.secret:
            raise ValueError("GRAFANA_WEBHOOK_SECRET is required when webhook is enabled")
        if self._config.auth_mode.strip().lower() not in {"bearer", "hmac"}:
            raise ValueError("GRAFANA_WEBHOOK_AUTH_MODE must be 'bearer' or 'hmac'")
        if self._config.worker_count < 1:
            raise ValueError("GRAFANA_WEBHOOK_WORKER_COUNT must be at least 1")

    def _claim_event(self, event: GrafanaAlertEvent) -> bool:
        return self._deduplicator.claim(event.dedupe_key)

    def _release_event(self, event: GrafanaAlertEvent) -> None:
        self._deduplicator.release(event.dedupe_key)

    def _run_worker(self) -> None:
        while not self._stopping.is_set():
            try:
                alert = self._queue.get(timeout=1)
            except queue.Empty:
                continue
            if alert is None:
                return
            self._clear_followup(alert.connector_name)
            try:
                logger.info(
                    "Processing Grafana connector alert",
                    extra={
                        "event": "grafana_webhook_processing",
                        "connector_name": alert.connector_name,
                        "alert_status": alert.status,
                        "alert_name": alert.alert_name,
                    },
                )
                followup_connector = self._process_connector(
                    alert.connector_name,
                    alert.failure_confirmed,
                )
                if followup_connector:
                    self._schedule_followup(
                        followup_connector,
                        failure_confirmed=alert.failure_confirmed,
                    )
            except Exception:
                logger.exception(
                    "Grafana-triggered connector processing failed",
                    extra={
                        "event": "grafana_webhook_processing_failed",
                        "connector_name": alert.connector_name,
                    },
                )
            finally:
                self._queue.task_done()

    def _schedule_followup(
        self,
        connector_name: str,
        *,
        failure_confirmed: bool = False,
    ) -> None:
        with self._followup_lock:
            if connector_name in self._followup_timers or self._stopping.is_set():
                return
            timer = threading.Timer(
                self._config.recovery_followup_seconds,
                self._enqueue_followup,
                args=(connector_name, failure_confirmed),
            )
            timer.daemon = True
            self._followup_timers[connector_name] = timer
            timer.start()

    def schedule_recovery_followup(self, connector_name: str) -> None:
        """Schedule a targeted recovery check from webhook or reconciliation."""
        self._schedule_followup(connector_name)

    def _enqueue_followup(
        self,
        connector_name: str,
        failure_confirmed: bool = False,
    ) -> None:
        self._clear_followup(connector_name)
        if self._stopping.is_set():
            return
        event = GrafanaAlertEvent(
            connector_name=connector_name,
            status="followup",
            fingerprint="internal",
            starts_at=str(time.time_ns()),
            alert_name="SELF-HEALTHY-KAFKA recovery follow-up",
            failure_confirmed=failure_confirmed,
        )
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            logger.error(
                "Recovery follow-up queue is full",
                extra={
                    "event": "grafana_webhook_queue_full",
                    "connector_name": connector_name,
                },
            )

    def _clear_followup(self, connector_name: str) -> None:
        with self._followup_lock:
            timer = self._followup_timers.pop(connector_name, None)
        if timer is not None and timer is not threading.current_thread():
            timer.cancel()

    def _handler_class(self):
        service = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == "/health":
                    self._json_response(HTTPStatus.OK, {"status": "ok"})
                    return
                if self.path == "/metrics":
                    self._metrics_response()
                    return
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "not found"})

            def do_POST(self) -> None:
                request_url = urlsplit(self.path)
                if request_url.path != service._config.path:
                    self._json_response(HTTPStatus.NOT_FOUND, {"error": "not found"})
                    return
                length = int(self.headers.get("Content-Length") or 0)
                if length <= 0 or length > MAX_BODY_BYTES:
                    self._json_response(
                        HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                        {"error": "invalid body size"},
                    )
                    return
                body = self.rfile.read(length)
                query_token = (parse_qs(request_url.query).get("token") or [""])[0]
                if not service.verify_request(
                    self.headers,
                    body,
                    query_token=query_token,
                ):
                    authorization = self.headers.get("Authorization", "")
                    custom_token = self.headers.get("X-SELF-HEALTHY-KAFKA-Token", "")
                    logger.warning(
                        "Rejected Grafana webhook authentication",
                        extra={
                            "event": "grafana_webhook_auth_rejected",
                            "authorization_present": bool(authorization),
                            "authorization_length": len(authorization),
                            "custom_token_present": bool(custom_token),
                            "custom_token_length": len(custom_token),
                            "custom_token_fingerprint": (
                                hashlib.sha256(custom_token.encode("utf-8"))
                                .hexdigest()[:12]
                                if custom_token
                                else None
                            ),
                        },
                    )
                    self._json_response(
                        HTTPStatus.UNAUTHORIZED,
                        {"error": "invalid webhook authentication"},
                    )
                    return
                try:
                    payload = json.loads(body)
                    if not isinstance(payload, dict):
                        raise ValueError("payload must be an object")
                except (json.JSONDecodeError, ValueError) as exc:
                    self._json_response(
                        HTTPStatus.BAD_REQUEST,
                        {"error": str(exc)},
                    )
                    return
                result = service.submit(payload)
                self._json_response(HTTPStatus.ACCEPTED, result)

            def log_message(self, format: str, *args) -> None:
                logger.debug("Grafana webhook HTTP: " + format, *args)

            def _json_response(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
                body = json.dumps(payload).encode("utf-8")
                self._write_response(
                    status,
                    "application/json",
                    body,
                )

            def _metrics_response(self) -> None:
                try:
                    inventory = service._monitoring_inventory()
                    connector_rows = inventory.get("connector_activity")
                    if connector_rows is None:
                        connector_rows = service._connector_activity()
                    topic_rows = inventory.get("topic_lag_metrics")
                    if topic_rows is None:
                        topic_rows = service._topic_lag_metrics()
                    body = render_prometheus_metrics(
                        connector_rows,
                        topic_rows,
                        inventory,
                    )
                except Exception:
                    logger.exception(
                        "Failed to load SELF-HEALTHY-KAFKA metrics",
                        extra={"event": "kc_shs_metrics_failed"},
                    )
                    self._json_response(
                        HTTPStatus.INTERNAL_SERVER_ERROR,
                        {"error": "metrics unavailable"},
                    )
                    return

                self._write_response(
                    HTTPStatus.OK,
                    "text/plain; version=0.0.4; charset=utf-8",
                    body,
                )

            def _write_response(
                self,
                status: HTTPStatus,
                content_type: str,
                body: bytes,
            ) -> None:
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", content_type)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except CLIENT_DISCONNECT_ERRORS:
                    logger.debug(
                        "HTTP client disconnected before response was fully written",
                        extra={
                            "event": "grafana_webhook_client_disconnected",
                            "path": self.path,
                        },
                    )

        return Handler
