# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List, Optional

from .client import (
    FeatureFilter,
    FeatureFilterOp,
    FeaturePoint,
    RiskPrecision,
    SequenceFeatureRow,
    TemporalStoreError,
    WindowUnit,
)


@dataclass
class ProxyOptions:
    endpoint: str
    namespace_name: str
    table_name: str
    api_key: str = ""
    timeout_seconds: float = 5.0


class ProxyClient:
    def __init__(self, options: ProxyOptions):
        self.options = options
        self.endpoint = options.endpoint.rstrip("/")

    def put_string(self, key: str, value: str, ttl_ms: Optional[int] = None) -> None:
        body: Dict[str, Any] = self._key_body(key)
        body["value"] = _bytes_value(value)
        if ttl_ms is not None:
            body["ttl_ms"] = ttl_ms
            self._post("/ProxyService/SetEx", body)
            return
        self._post("/ProxyService/Set", body)

    def get_string(self, key: str) -> str:
        data = self._post("/ProxyService/Get", self._key_body(key))
        return str(data.get("value", ""))

    def delete_object(self, key: str) -> None:
        self._post("/ProxyService/Delete", self._key_body(key))

    def expire(self, key: str, ttl_ms: int) -> None:
        body = self._key_body(key)
        body["ttl_ms"] = ttl_ms
        self._post("/ProxyService/Expire", body)

    def ttl(self, key: str) -> int:
        data = self._post("/ProxyService/Ttl", self._key_body(key))
        return int(data.get("ttl_ms", 0))

    def hset(self, key: str, field: str, value: str) -> None:
        body = self._key_body(key)
        body.update({"field": field, "value": _bytes_value(value)})
        self._post("/ProxyService/HSet", body)

    def batch_hset(self, entries: Iterable[Dict[str, Any]]) -> None:
        grouped: Dict[str, List[List[Any]]] = {}
        for entry in entries:
            key = str(entry["key"])
            grouped.setdefault(key, []).append(
                [str(entry["field"]), _bytes_value(str(entry.get("value", "")))]
            )
        for key, kv in grouped.items():
            body = self._key_body(key)
            body["entries"] = kv
            self._post("/ProxyService/HMSet", body)

    def hget(self, key: str, field: str) -> str:
        body = self._key_body(key)
        body["field"] = field
        data = self._post("/ProxyService/HGet", body)
        return str(data.get("value", ""))

    def hdel(self, key: str, field: str) -> None:
        body = self._key_body(key)
        body["field"] = field
        self._post("/ProxyService/HDel", body)

    def sadd(self, key: str, member: str) -> None:
        body = self._key_body(key)
        body["member"] = _bytes_value(member)
        self._post("/ProxyService/SAdd", body)

    def smembers(self, key: str) -> List[str]:
        data = self._post("/ProxyService/SMembers", self._key_body(key))
        return [str(item) for item in data.get("members", [])]

    def add_feature_points(self, key: str, points: Iterable[FeaturePoint]) -> None:
        body = self._key_body(key)
        body["points"] = [_feature_point_body(point) for point in points]
        self._post("/ProxyService/FeatureAdd", body)

    def query_feature_points(
        self,
        key: str,
        start_ts: int,
        end_ts: int,
        count: int,
        filters: Iterable[FeatureFilter] = (),
    ) -> List[FeaturePoint]:
        body = self._query_body(key, start_ts, end_ts, count, filters)
        data = self._post("/ProxyService/FeatureQuery", body)
        return [
            FeaturePoint(int(row.get("timestamp", row.get("timestamp_ms", 0))), _string_value(row.get("value", "")))
            for row in data.get("points", [])
        ]

    def add_sequence_feature_rows(self, key: str, rows: Iterable[SequenceFeatureRow]) -> None:
        body = self._key_body(key)
        body["command"] = {"kind": "sequence_add", "key": key, "rows": [_sequence_row_body(row) for row in rows]}
        self._post("/ProxyService/ExecuteTableCmd", body)

    def query_sequence_feature_rows(
        self,
        key: str,
        start_ts: int,
        end_ts: int,
        count: int,
        filters: Iterable[FeatureFilter] = (),
    ) -> List[SequenceFeatureRow]:
        body = self._key_body(key)
        body["command"] = {
            "kind": "sequence_query",
            "key": key,
            "start_ms": start_ts,
            "end_ms": end_ts,
            "count": int(count),
            "filters": [_feature_filter_body(item) for item in filters],
        }
        data = self._post("/ProxyService/ExecuteTableCmd", body)
        return [
            SequenceFeatureRow(
                int(row.get("timestamp", row.get("timestamp_ms", 0))),
                int(row["gid"]),
                int(row["action_type"]),
                int(row["duration"]),
                int(row["author_id"]),
            )
            for row in data.get("rows", [])
        ]



    def risk_increment(
        self,
        key: str,
        amount: int = 1,
        ttl_seconds: int = 24 * 3600,
        precision: RiskPrecision = RiskPrecision.ONE_MINUTE,
        uuid: str = "",
        occur_time_seconds: int = 0,
    ) -> None:
        body = self._key_body(key)
        body.update(
            {
                "amount": amount,
                "ttl_seconds": ttl_seconds,
                "precision": int(precision),
                "uuid": uuid,
                "occur_time_seconds": occur_time_seconds,
            }
        )
        body["command"] = {
            "kind": "control_state_increment",
            "key": key,
            "timestamp_ms": int(occur_time_seconds * 1000) if occur_time_seconds else 0,
            "amount": int(amount),
        }
        self._post("/ProxyService/ExecuteTableCmd", body)

    def risk_count(
        self,
        key: str,
        precision: RiskPrecision = RiskPrecision.ONE_MINUTE,
        window_start: int = -1,
        window_end: int = 0,
        window_unit: WindowUnit = WindowUnit.HOUR,
    ) -> int:
        body = self._key_body(key)
        body.update(
            {
                "precision": int(precision),
                "window_start": window_start,
                "window_end": window_end,
                "window_unit": int(window_unit),
            }
        )
        body["command"] = {"kind": "control_state_count", "key": key, "start_ms": int(window_start), "end_ms": int(window_end)}
        data = self._post("/ProxyService/ExecuteTableCmd", body)
        return int(data.get("count", 0))

    def feature_agg_query(
        self,
        key: str,
        start_ts: int,
        end_ts: int,
        aggregator: str = "count",
        count: Optional[int] = None,
    ) -> int:
        """Exact serving-time aggregate (count/sum/min/max/avg/first/last)."""
        body = self._key_body(key)
        body.update({"start_ms": int(start_ts), "end_ms": int(end_ts), "aggregator": str(aggregator)})
        if count is not None:
            body["count"] = int(count)
        data = self._post("/ProxyService/FeatureAggQuery", body)
        return int(data.get("value", data.get("count", 0)))

    def control_state_increment(
        self,
        key: str,
        amount: int = 1,
        ttl_seconds: int = 24 * 3600,
        precision: RiskPrecision = RiskPrecision.ONE_MINUTE,
        uuid: str = "",
        occur_time_seconds: int = 0,
    ) -> None:
        """Canonical name for the renamed Control State counter increment."""
        self.risk_increment(key, amount, ttl_seconds, precision, uuid, occur_time_seconds)

    def control_state_count(
        self,
        key: str,
        precision: RiskPrecision = RiskPrecision.ONE_MINUTE,
        window_start: int = -1,
        window_end: int = 0,
        window_unit: WindowUnit = WindowUnit.HOUR,
    ) -> int:
        """Canonical name for the renamed Control State windowed count."""
        return self.risk_count(key, precision, window_start, window_end, window_unit)

    def _key_body(self, key: str) -> Dict[str, Any]:
        return {
            "namespace": self.options.namespace_name,
            "table": self.options.table_name,
            "table_name": self.options.table_name,
            "key": key,
        }

    def _query_body(
        self,
        key: str,
        start_ts: int,
        end_ts: int,
        count: int,
        filters: Iterable[FeatureFilter],
    ) -> Dict[str, Any]:
        body = self._key_body(key)
        body.update(
            {
                "start_ts": start_ts,
                "end_ts": end_ts,
                "start_ms": start_ts,
                "end_ms": end_ts,
                "count": count,
                "filters": [_feature_filter_body(item) for item in filters],
            }
        )
        return body

    def _post(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        request = urllib.request.Request(
            self.endpoint + path,
            data=json.dumps(body).encode("utf-8"),
            headers=self._headers(),
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.options.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            message = exc.read().decode("utf-8", errors="replace")
            raise TemporalStoreError(exc.code, message) from exc
        except urllib.error.URLError as exc:
            raise TemporalStoreError(0, str(exc)) from exc

        return _unwrap_proxy_response(payload)

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.options.api_key:
            headers["Authorization"] = "Bearer " + self.options.api_key
        return headers


def _bytes_value(value: str) -> List[int]:
    return list(value.encode("utf-8"))


def _string_value(value: Any) -> str:
    if isinstance(value, list):
        return bytes(int(item) & 0xFF for item in value).decode("utf-8", errors="replace")
    return str(value)


def _feature_filter_body(value: FeatureFilter) -> Dict[str, Any]:
    names = {
        FeatureFilterOp.EQUAL: "equal",
        FeatureFilterOp.NOT_EQUAL: "not_equal",
        FeatureFilterOp.GREATER_THAN: "greater_than",
        FeatureFilterOp.LESS_THAN: "less_than",
    }
    return {"field": value.field, "op": names.get(value.op, "equal"), "value": int(value.value)}


def _feature_point_body(value: FeaturePoint) -> Dict[str, Any]:
    return {"timestamp_ms": int(value.timestamp), "value": _bytes_value(value.value)}


def _sequence_row_body(value: SequenceFeatureRow) -> Dict[str, Any]:
    return {
        "timestamp_ms": int(value.timestamp),
        "gid": int(value.gid),
        "action_type": int(value.action_type),
        "duration": int(value.duration),
        "author_id": int(value.author_id),
    }


def _unwrap_proxy_response(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    if "ok" in payload:
        if not payload.get("ok", False):
            code = payload.get("code", 0)
            raise TemporalStoreError(int(code) if isinstance(code, int) else 0, str(payload.get("message", "")))
        data = payload.get("data", {})
        return data if isinstance(data, dict) else {}
    if "status" in payload and "response" in payload:
        status = payload.get("status") or {}
        if not status.get("ok", False):
            raise TemporalStoreError(0, str(status.get("message", status.get("code", ""))))
        return _flatten_command_response(payload.get("response", {}))
    return payload


def _flatten_command_response(response: Any) -> Dict[str, Any]:
    if not isinstance(response, dict):
        return {}
    kind = response.get("kind")
    if kind == "empty":
        return {}
    if kind == "bytes":
        return {"value": _string_value(response.get("value", []))}
    if kind in {"integer", "aggregate"}:
        value = int(response.get("value", 0))
        return {"value": value, "count": value, "ttl_ms": value}
    if kind == "members":
        return {"members": [_string_value(item) for item in response.get("members", [])]}
    if kind == "feature_points":
        return {"points": [_feature_point_result(item) for item in response.get("points", [])]}
    if kind == "sequence_rows":
        return {"rows": [_sequence_row_result(item) for item in response.get("rows", [])]}
    return response


def _feature_point_result(value: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "timestamp": int(value.get("timestamp", value.get("timestamp_ms", 0))),
        "value": _string_value(value.get("value", "")),
    }


def _sequence_row_result(value: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(value)
    out["timestamp"] = int(out.get("timestamp", out.get("timestamp_ms", 0)))
    return out
