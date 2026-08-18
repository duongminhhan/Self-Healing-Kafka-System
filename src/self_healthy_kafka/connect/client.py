# kafka_connect_client.py — HTTP client cho Kafka Connect REST API

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Optional

import httpx

from self_healthy_kafka.domain.models import ConnectorStatus

logger = logging.getLogger(__name__)

class KafkaConnectCircuitOpen(RuntimeError):
    pass


class KafkaConnectClient:
    """
    Wrapper cho Kafka Connect REST API.
    Tất cả HTTP call đi qua đây — dễ mock trong test.
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 5.0,
        tls_verify: bool = True,
        circuit_breaker_cooldown_seconds: int = 30,
    ):
        self._base = base_url.rstrip("/")
        self._circuit_cooldown = circuit_breaker_cooldown_seconds
        self._circuit_open_until = 0.0
        self._circuit_half_open = False
        self._circuit_lock = threading.Lock()
        self._client = httpx.Client(
            timeout=timeout,
            verify=tls_verify,
            headers={"Content-Type": "application/json"},
        )
        if not tls_verify:
            logger.warning(
                "Kafka Connect TLS certificate verification is disabled",
                extra={
                    "event": "kafka_connect_tls_verify_disabled",
                    "url": self._base,
                },
            )

    def _request(
        self,
        method: str,
        path: str,
        *,
        connector_name: str | None = None,
        **kwargs,
    ) -> httpx.Response:
        url = f"{self._base}{path}"
        started = time.perf_counter()
        status_code = None
        outcome = "error"
        try:
            resp = self._client.request(method, url, **kwargs)
            status_code = resp.status_code
            outcome = "ok" if resp.status_code < 400 else "http_error"
            return resp
        except httpx.RequestError:
            outcome = "request_error"
            raise
        finally:
            latency_ms = round((time.perf_counter() - started) * 1000, 2)
            logger.info(
                "Kafka Connect REST request",
                extra={
                    "event": "kafka_connect_request",
                    "connector_name": connector_name,
                    "method": method.upper(),
                    "path": path,
                    "status_code": status_code,
                    "outcome": outcome,
                    "latency_ms": latency_ms,
                },
            )

    # ─── Status ──────────────────────────────────────────────────────────────

    def get_status(self, name: str) -> Optional[ConnectorStatus]:
        """
        GET /connectors/{name}/status
        Trả về None nếu connector không tồn tại (404).
        """
        self._before_status_request()
        try:
            resp = self._request("GET", f"/connectors/{name}/status", connector_name=name)
            self._close_status_circuit()
            if resp.status_code == 404:
                logger.error(f"[{name}] connector not found (404)")
                return None
            resp.raise_for_status()
            return ConnectorStatus.from_api_response(resp.json())
        except httpx.HTTPStatusError as e:
            self._close_status_circuit()
            logger.error(f"[{name}] HTTP error: {e.response.status_code}")
            raise
        except httpx.RequestError as e:
            self._open_status_circuit()
            logger.error(f"[{name}] Kafka Connect unreachable: {e}")
            raise

    @property
    def status_circuit_open(self) -> bool:
        with self._circuit_lock:
            return time.monotonic() < self._circuit_open_until

    def _before_status_request(self) -> None:
        with self._circuit_lock:
            now = time.monotonic()
            if now < self._circuit_open_until:
                raise KafkaConnectCircuitOpen(
                    "Kafka Connect status circuit is open until cooldown expires"
                )
            if self._circuit_open_until and self._circuit_half_open:
                raise KafkaConnectCircuitOpen(
                    "Kafka Connect status circuit half-open probe is in progress"
                )
            if self._circuit_open_until:
                self._circuit_half_open = True

    def _open_status_circuit(self) -> None:
        with self._circuit_lock:
            self._circuit_open_until = time.monotonic() + self._circuit_cooldown
            self._circuit_half_open = False

    def _close_status_circuit(self) -> None:
        with self._circuit_lock:
            self._circuit_open_until = 0.0
            self._circuit_half_open = False

    def list_connectors(self) -> list[str]:
        """GET /connectors — liệt kê tất cả connectors."""
        resp = self._request("GET", "/connectors")
        resp.raise_for_status()
        return resp.json()

    # ─── Restart Actions ─────────────────────────────────────────────────────

    def restart_connector(self, name: str, only_failed: bool = True) -> None:
        """POST /connectors/{name}/restart"""
        params = {"includeTasks": "true", "onlyFailed": str(only_failed).lower()}
        resp = self._request(
            "POST",
            f"/connectors/{name}/restart",
            connector_name=name,
            params=params,
        )
        resp.raise_for_status()
        logger.info(f"[{name}] restart sent (onlyFailed={only_failed})")

    # ─── Connector Lifecycle ─────────────────────────────────────────────────

    def delete_connector(self, name: str) -> None:
        """DELETE /connectors/{name}"""
        resp = self._request("DELETE", f"/connectors/{name}", connector_name=name)
        if resp.status_code not in (200, 204, 404):
            resp.raise_for_status()
        logger.info(f"[{name}] deleted")

    def create_connector(
        self,
        name: str,
        config: dict[str, Any],
        *,
        initial_state: str | None = None,
    ) -> dict[str, Any]:
        """POST /connectors"""
        payload: dict[str, Any] = {"name": name, "config": config}
        if initial_state:
            payload["initial_state"] = initial_state
        resp = self._request(
            "POST",
            "/connectors",
            connector_name=name,
            json=payload,
        )
        resp.raise_for_status()
        logger.info(f"Connector [{name}] created")
        return resp.json()

    def connector_exists(self, name: str) -> bool:
        try:
            status = self.get_status(name)
            return status is not None
        except Exception:
            return False

    # ─── Offsets API (Kafka Connect 3.5+) ─────────────────────────────────────

    def get_offsets(self, name: str) -> Optional[dict[str, Any]]:
        """
        GET /connectors/{name}/offsets — Kafka Connect 3.5+.

        Returns the offsets payload (`{"offsets": [...]}` shape) if the
        cluster supports it, or None on 404 / 405 (older clusters).
        Other HTTP errors propagate.
        """
        try:
            resp = self._request("GET", f"/connectors/{name}/offsets", connector_name=name)
            if resp.status_code in (404, 405):
                logger.warning(
                    f"[{name}] offsets API not available (status {resp.status_code}) - "
                    "older Kafka Connect cluster?"
                )
                return None
            resp.raise_for_status()
            return resp.json()
        except httpx.RequestError as e:
            logger.error(f"[{name}] failed to fetch offsets: {e}")
            return None

    def patch_offsets(self, name: str, offsets: dict[str, Any]) -> bool:
        """
        PATCH /connectors/{name}/offsets — Kafka Connect 3.5+.

        Reapplies a previously captured offsets payload onto a (newly created)
        connector. Returns True on success, False if the API is unavailable
        or the call fails.

        Note: the connector must typically be STOPPED for this to apply.
        """
        try:
            resp = self._request(
                "PATCH",
                f"/connectors/{name}/offsets",
                connector_name=name,
                json=offsets,
            )
            if resp.status_code in (404, 405):
                logger.warning(
                    f"[{name}] offsets API not available for PATCH (status {resp.status_code})"
                )
                return False
            resp.raise_for_status()
            logger.info(f"[{name}] offsets reapplied")
            return True
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            logger.error(f"[{name}] failed to PATCH offsets: {e}")
            return False

    def stop_connector(self, name: str) -> bool:
        """
        PUT /connectors/{name}/stop — Kafka Connect 3.5+.

        Required precondition for PATCH /offsets on most cluster versions.
        Returns True if accepted, False otherwise.
        """
        try:
            resp = self._request("PUT", f"/connectors/{name}/stop", connector_name=name)
            if resp.status_code == 404:
                logger.info(f"[{name}] connector already absent; stop treated as complete")
                return True
            if resp.status_code == 405:
                return False
            resp.raise_for_status()
            return True
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            logger.warning(f"[{name}] stop_connector failed: {e}")
            return False

    def resume_connector(self, name: str) -> bool:
        """PUT /connectors/{name}/resume — resume a STOPPED/PAUSED connector."""
        try:
            resp = self._request("PUT", f"/connectors/{name}/resume", connector_name=name)
            if resp.status_code in (404, 405):
                return False
            resp.raise_for_status()
            return True
        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            logger.warning(f"[{name}] resume_connector failed: {e}")
            return False

    def close(self) -> None:
        self._client.close()

