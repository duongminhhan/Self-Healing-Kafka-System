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

from self_healthy_kafka.config import (
    AnalyticsChatConfig,
    ChatApiConfig,
    GrafanaWebhookConfig,
    OllamaChatConfig,
)
from self_healthy_kafka.webhook.analytics_chat import AnalyticsChatService
from self_healthy_kafka.webhook.chat_api import ChatReadApi
from self_healthy_kafka.webhook.chat_ui import page as chat_ui_page
from self_healthy_kafka.webhook.ollama_chat import OllamaChatService
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
        *,
        chat_api_config: ChatApiConfig | None = None,
        queue_lookup: Callable[[str | None, str | None], list[Any]] | None = None,
        healing_logs: Callable[..., list[dict[str, Any]]] | None = None,
        log_search: Callable[[str, int], list[dict[str, Any]]] | None = None,
        ollama_chat_config: OllamaChatConfig | None = None,
        failure_ranking: Callable[..., list[dict[str, Any]]] | None = None,
        analytics_chat_config: AnalyticsChatConfig | None = None,
        incident_facts: Callable[..., list[dict[str, Any]]] | None = None,
    ):
        self._config = config
        self._process_connector = process_connector
        self._queue: queue.Queue[GrafanaAlertEvent | None] = queue.Queue(
            maxsize=config.queue_size
        )
        self._authenticator = WebhookAuthenticator(config)
        self._chat_api = (
            ChatReadApi(
                chat_api_config,
                queue_lookup=queue_lookup or (lambda _queue_id, _connector_name: []),
                healing_logs=healing_logs or (lambda **_kwargs: []),
                log_search=log_search,
            )
            if chat_api_config is not None
            else None
        )
        self._ollama_chat = (
            OllamaChatService(
                ollama_chat_config,
                read_api=self._chat_api,
                failure_ranking=failure_ranking or (lambda **_kwargs: []),
            )
            if ollama_chat_config is not None and self._chat_api is not None
            else None
        )
        self._analytics_chat = (
            AnalyticsChatService(
                analytics_chat_config,
                incident_facts=incident_facts or (lambda **_kwargs: []),
            )
            if analytics_chat_config is not None
            else None
        )
        self._deduplicator = EventDeduplicator(config.dedupe_ttl_seconds)
        self._followup_lock = threading.Lock()
        self._followup_timers: dict[str, threading.Timer] = {}
        self._server: ThreadingHTTPServer | None = None
        self._server_thread: threading.Thread | None = None
        self._worker_threads: list[threading.Thread] = []
        self._stopping = threading.Event()

    def start(self) -> None:
        if not self._config.enabled and not (self._chat_api and self._chat_api.enabled):
            return
        self._validate_config()
        handler = self._handler_class()
        self._server = ThreadingHTTPServer(
            (self._config.host, self._config.port),
            handler,
        )
        self._server.daemon_threads = True
        if self._config.enabled:
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
            "HTTP receiver started",
            extra={
                "event": "http_receiver_started",
                "host": self._config.host,
                "port": self._config.port,
                "webhook_path": self._config.path if self._config.enabled else None,
                "chat_api_path_prefix": (
                    self._chat_api.path_prefix if self._chat_api and self._chat_api.enabled else None
                ),
                "ollama_chat_path": (
                    self._ollama_chat.path if self._ollama_chat and self._ollama_chat.enabled else None
                ),
                "auth_mode": self._config.auth_mode,
                "worker_count": self._config.worker_count,
            },
        )

    def close(self) -> None:
        if not self._config.enabled and not (self._chat_api and self._chat_api.enabled):
            return
        self._stopping.set()
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
        if self._config.enabled:
            if not self._config.path.startswith("/"):
                raise ValueError("GRAFANA_WEBHOOK_PATH must start with '/'")
            if not self._config.secret:
                raise ValueError("GRAFANA_WEBHOOK_SECRET is required when webhook is enabled")
            if self._config.auth_mode.strip().lower() not in {"bearer", "hmac"}:
                raise ValueError("GRAFANA_WEBHOOK_AUTH_MODE must be 'bearer' or 'hmac'")
            if self._config.worker_count < 1:
                raise ValueError("GRAFANA_WEBHOOK_WORKER_COUNT must be at least 1")
        if self._chat_api and self._chat_api.enabled:
            self._chat_api.validate()
        if self._ollama_chat and self._ollama_chat.enabled:
            if not self._chat_api or not self._chat_api.enabled:
                raise ValueError("CHAT_API_ENABLED must be true when Ollama chat is enabled")
            self._ollama_chat.validate()
        if self._analytics_chat and self._analytics_chat.enabled:
            if not self._chat_api or not self._chat_api.enabled:
                raise ValueError("CHAT_API_ENABLED must be true when analytics chat is enabled")
            self._analytics_chat.validate()

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
                request_url = urlsplit(self.path)
                if request_url.path == "/health":
                    self._json_response(HTTPStatus.OK, {"status": "ok"})
                    return
                if request_url.path in {"/", "/chat"}:
                    self._write_response(
                        HTTPStatus.OK,
                        "text/html; charset=utf-8",
                        chat_ui_page(),
                    )
                    return
                if service._chat_api and service._chat_api.enabled:
                    if not service._chat_api.is_authorized(
                        self.headers.get("Authorization", "")
                    ):
                        self._json_response(HTTPStatus.UNAUTHORIZED, {"error": "invalid chat API authentication"})
                        return
                    try:
                        status, payload = service._chat_api.handle_get(
                            request_url.path,
                            parse_qs(request_url.query),
                        )
                    except ValueError as exc:
                        self._json_response(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                        return
                    self._json_response(HTTPStatus(status), payload)
                    return
                self._json_response(HTTPStatus.NOT_FOUND, {"error": "not found"})

            def do_POST(self) -> None:
                request_url = urlsplit(self.path)
                if (
                    service._analytics_chat
                    and service._analytics_chat.enabled
                    and request_url.path == service._analytics_chat.path
                ):
                    if not service._chat_api or not service._chat_api.is_authorized(
                        self.headers.get("Authorization", "")
                    ):
                        self._json_response(HTTPStatus.UNAUTHORIZED, {"error": "invalid chat API authentication"})
                        return
                    payload = self._read_json_body()
                    if payload is None:
                        return
                    question = payload.get("question")
                    if not isinstance(question, str):
                        self._json_response(HTTPStatus.BAD_REQUEST, {"error": "question must be a string"})
                        return
                    try:
                        result = service._analytics_chat.ask(question)
                    except ValueError as exc:
                        self._json_response(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                        return
                    except Exception:
                        logger.exception("Analytics chat request failed", extra={"event": "analytics_chat_failed"})
                        self._json_response(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "analytics chat is unavailable"})
                        return
                    self._json_response(HTTPStatus.OK, result)
                    return
                if (
                    service._ollama_chat
                    and service._ollama_chat.enabled
                    and request_url.path == service._ollama_chat.path
                ):
                    if not service._chat_api or not service._chat_api.is_authorized(
                        self.headers.get("Authorization", "")
                    ):
                        self._json_response(
                            HTTPStatus.UNAUTHORIZED,
                            {"error": "invalid chat API authentication"},
                        )
                        return
                    payload = self._read_json_body()
                    if payload is None:
                        return
                    question = payload.get("question")
                    if not isinstance(question, str):
                        self._json_response(
                            HTTPStatus.BAD_REQUEST,
                            {"error": "question must be a string"},
                        )
                        return
                    try:
                        result = service._ollama_chat.ask(question)
                    except ValueError as exc:
                        self._json_response(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                        return
                    except Exception:
                        logger.exception("Ollama chat request failed", extra={"event": "ollama_chat_failed"})
                        self._json_response(
                            HTTPStatus.SERVICE_UNAVAILABLE,
                            {"error": "chat model is unavailable"},
                        )
                        return
                    self._json_response(HTTPStatus.OK, result)
                    return
                if not service._config.enabled:
                    self._json_response(HTTPStatus.NOT_FOUND, {"error": "not found"})
                    return
                if request_url.path != service._config.path:
                    self._json_response(HTTPStatus.NOT_FOUND, {"error": "not found"})
                    return
                body = self._read_body()
                if body is None:
                    return
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

            def _read_json_body(self) -> dict[str, Any] | None:
                body = self._read_body()
                if body is None:
                    return None
                try:
                    payload = json.loads(body)
                    if not isinstance(payload, dict):
                        raise ValueError("payload must be an object")
                except (json.JSONDecodeError, ValueError) as exc:
                    self._json_response(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                    return None
                return payload

            def _read_body(self) -> bytes | None:
                length = int(self.headers.get("Content-Length") or 0)
                if length <= 0 or length > MAX_BODY_BYTES:
                    self._json_response(
                        HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                        {"error": "invalid body size"},
                    )
                    return None
                return self.rfile.read(length)

            def log_message(self, format: str, *args) -> None:
                logger.debug("Grafana webhook HTTP: " + format, *args)

            def _json_response(self, status: HTTPStatus, payload: Any) -> None:
                body = json.dumps(payload).encode("utf-8")
                self._write_response(
                    status,
                    "application/json",
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
