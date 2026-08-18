#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urljoin
from urllib.request import Request, urlopen

DEFAULT_RULES = [
    "kafka-connect-task-error-trigger",
    "self-healthy-kafka-healing-failed",
    "self-healthy-kafka-healing-success",
    "self-healthy-kafka-topic-idle-over-threshold",
    "self-healthy-kafka-event-capture-lag-over-threshold",
    "self-healthy-kafka-topic-recovered",
]


@dataclass
class CheckResult:
    passed: int = 0
    warned: int = 0
    failed: int = 0

    def pass_(self, message: str) -> None:
        self.passed += 1
        print(f"[PASS] {message}")

    def warn(self, message: str) -> None:
        self.warned += 1
        print(f"[WARN] {message}")

    def fail(self, message: str) -> None:
        self.failed += 1
        print(f"[FAIL] {message}")


class GrafanaClient:
    def __init__(self, base_url: str, token: str = "", basic_auth: str = "", timeout: int = 10):
        self.base_url = base_url.rstrip("/") + "/"
        self.timeout = timeout
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
        elif basic_auth:
            encoded = base64.b64encode(basic_auth.encode("utf-8")).decode("ascii")
            self.headers["Authorization"] = f"Basic {encoded}"

    def get_json(self, path: str) -> Any:
        return self._request_json("GET", path)

    def _request_json(self, method: str, path: str, body: bytes | None = None) -> Any:
        url = urljoin(self.base_url, path.lstrip("/"))
        req = Request(url, data=body, headers=self.headers, method=method)
        try:
            with urlopen(req, timeout=self.timeout) as response:
                payload = response.read()
        except HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"HTTP {exc.code} {url}: {details}") from exc
        except URLError as exc:
            raise RuntimeError(f"{url}: {exc.reason}") from exc
        if not payload:
            return None
        try:
            return json.loads(payload)
        except json.JSONDecodeError as exc:
            preview = payload.decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"non-JSON response from {url}: {preview}") from exc


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check Grafana alert rules and their datasource queries.",
    )
    parser.add_argument(
        "--grafana-url",
        default=os.getenv("GRAFANA_URL", ""),
        help="Grafana base URL, e.g. http://grafana-stg.snp.com.vn:3000",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("GRAFANA_API_TOKEN", ""),
        help="Grafana service account token. Env: GRAFANA_API_TOKEN",
    )
    parser.add_argument(
        "--basic-auth",
        default=os.getenv("GRAFANA_BASIC_AUTH", ""),
        help="Basic auth as user:password. Env: GRAFANA_BASIC_AUTH",
    )
    parser.add_argument(
        "--rule",
        action="append",
        default=[],
        help="Rule title/UID to check. Repeatable. Defaults to SELF-HEALTHY-KAFKA rules.",
    )
    parser.add_argument(
        "--timeout",
        default=int(os.getenv("GRAFANA_CHECK_TIMEOUT_SECONDS", "10")),
        type=int,
    )
    parser.add_argument(
        "--skip-query",
        action="store_true",
        help="Only check rule metadata; do not evaluate datasource queries.",
    )
    args = parser.parse_args()

    if not args.grafana_url:
        print("Missing --grafana-url or GRAFANA_URL", file=sys.stderr)
        return 2

    client = GrafanaClient(
        args.grafana_url,
        token=args.token,
        basic_auth=args.basic_auth,
        timeout=args.timeout,
    )
    result = CheckResult()

    print("== Grafana API ==")
    try:
        health = client.get_json("/api/health")
    except RuntimeError as exc:
        result.fail(f"Grafana health failed: {exc}")
        print_summary(result)
        return 1
    result.pass_(f"Grafana reachable: {health.get('version', 'unknown version')}")

    print("\n== Alert Rules ==")
    try:
        rules = client.get_json("/api/v1/provisioning/alert-rules")
    except RuntimeError as exc:
        result.fail(f"cannot read alert rules: {exc}")
        result.warn("token needs alert rule read permission; try a Grafana service account token")
        print_summary(result)
        return 1

    targets = args.rule or DEFAULT_RULES
    by_key = {}
    for rule in rules or []:
        by_key[str(rule.get("title") or "")] = rule
        by_key[str(rule.get("uid") or "")] = rule

    for target in targets:
        rule = by_key.get(target)
        if not rule:
            result.fail(f"rule not found: {target}")
            continue
        check_rule_metadata(rule, result)
        if not args.skip_query:
            check_rule_queries(client, rule, result)

    print_summary(result)
    return 1 if result.failed else 0


def check_rule_metadata(rule: dict[str, Any], result: CheckResult) -> None:
    title = rule.get("title") or rule.get("uid")
    labels = rule.get("labels") or {}
    annotations = rule.get("annotations") or {}
    data = rule.get("data") or []

    result.pass_(
        f"{title}: uid={rule.get('uid')} for={rule.get('for', '0s')} "
        f"noDataState={rule.get('noDataState')} execErrState={rule.get('execErrState')}"
    )

    if labels.get("recovery_trigger") == "true" and rule.get("execErrState") != "OK":
        result.warn(
            f"{title}: recovery trigger should use execErrState=OK to avoid DatasourceError recovery alerts"
        )
    if rule.get("noDataState") != "OK":
        result.warn(f"{title}: noDataState should be OK to avoid NoData notification noise")
    if labels.get("service") != "self-healthy-kafka":
        result.warn(f"{title}: missing service=self-healthy-kafka label")
    if not labels.get("alert_group"):
        result.warn(f"{title}: missing alert_group label")
    if not labels.get("event"):
        result.warn(f"{title}: missing event label")
    if labels.get("recovery_trigger") == "true" and "connector_name" not in str(
        annotations
    ):
        result.warn(f"{title}: recovery annotation does not reference connector_name")
    if "healing" in str(title) and labels.get("alert_group") != "healing":
        result.warn(f"{title}: healing rule should use alert_group=healing")
    if "topic" in str(title) and labels.get("alert_group") != "topic":
        result.warn(f"{title}: topic rule should use alert_group=topic")
    if not data:
        result.fail(f"{title}: rule has no data queries")


def check_rule_queries(client: GrafanaClient, rule: dict[str, Any], result: CheckResult) -> None:
    title = str(rule.get("title") or rule.get("uid"))
    for query in rule.get("data") or []:
        datasource_uid = query.get("datasourceUid")
        model = query.get("model") or {}
        expr = model.get("expr")
        ref_id = query.get("refId") or model.get("refId")

        if not expr or datasource_uid == "__expr__":
            continue

        try:
            datasource = client.get_json(f"/api/datasources/uid/{quote(str(datasource_uid))}")
        except RuntimeError as exc:
            result.fail(f"{title} {ref_id}: datasource {datasource_uid} lookup failed: {exc}")
            continue

        ds_type = str(datasource.get("type") or model.get("datasource", {}).get("type") or "")
        result.pass_(
            f"{title} {ref_id}: datasource uid={datasource_uid} "
            f"name={datasource.get('name')} type={ds_type}"
        )

        if ds_type == "prometheus":
            check_prometheus_query(client, title, ref_id, datasource_uid, expr, result)
        elif ds_type == "loki":
            check_loki_query(client, title, ref_id, datasource_uid, expr, result)
        else:
            result.warn(f"{title} {ref_id}: unsupported datasource type for query test: {ds_type}")


def check_prometheus_query(
    client: GrafanaClient,
    title: str,
    ref_id: str,
    datasource_uid: str,
    expr: str,
    result: CheckResult,
) -> None:
    path = (
        f"/api/datasources/proxy/uid/{quote(datasource_uid)}"
        f"/api/v1/query?query={quote(expr, safe='')}&time={int(time.time())}"
    )
    try:
        payload = client.get_json(path)
    except RuntimeError as exc:
        result.fail(f"{title} {ref_id}: Prometheus query failed: {exc}")
        return
    if payload.get("status") != "success":
        result.fail(f"{title} {ref_id}: Prometheus status={payload.get('status')}: {payload}")
        return
    series = ((payload.get("data") or {}).get("result") or [])
    result.pass_(f"{title} {ref_id}: Prometheus query OK, series={len(series)}")
    check_expected_labels(title, ref_id, series, result)


def check_loki_query(
    client: GrafanaClient,
    title: str,
    ref_id: str,
    datasource_uid: str,
    expr: str,
    result: CheckResult,
) -> None:
    path = (
        f"/api/datasources/proxy/uid/{quote(datasource_uid)}"
        f"/loki/api/v1/query?query={quote(expr, safe='')}&time={int(time.time() * 1_000_000_000)}"
    )
    try:
        payload = client.get_json(path)
    except RuntimeError as exc:
        result.fail(f"{title} {ref_id}: Loki query failed: {exc}")
        return
    if payload.get("status") != "success":
        result.fail(f"{title} {ref_id}: Loki status={payload.get('status')}: {payload}")
        return
    series = ((payload.get("data") or {}).get("result") or [])
    result.pass_(f"{title} {ref_id}: Loki query OK, series={len(series)}")
    check_expected_labels(title, ref_id, series, result)


def check_expected_labels(
    title: str,
    ref_id: str,
    series: list[dict[str, Any]],
    result: CheckResult,
) -> None:
    if not series:
        result.warn(f"{title} {ref_id}: query returned no series right now")
        return
    labels = series[0].get("metric") or {}
    if "connector" in title or "healing" in title:
        if "connector_name" not in labels and "connector" not in labels and "server" not in labels:
            result.warn(f"{title} {ref_id}: first series has no connector label: {labels}")
    if "topic" in title and "topic" not in labels:
        result.warn(f"{title} {ref_id}: first series has no topic label: {labels}")


def print_summary(result: CheckResult) -> None:
    print("\n== Summary ==")
    print(f"PASS={result.passed} WARN={result.warned} FAIL={result.failed}")


if __name__ == "__main__":
    raise SystemExit(main())
