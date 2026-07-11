from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List, Optional

from .client import (
    FeatureFilter,
    FeatureFilterOp,
    FeaturePoint,
    FeatureWritePolicy,
    IpsFeatureStat,
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
        for entry in entries:
            self.hset(str(entry["key"]), str(entry["field"]), str(entry.get("value", "")))

    def matrixark_batch_append_records(
        self,
        entries: Iterable[Dict[str, Any]],
        *,
        count_key: Optional[str] = None,
        count_value: Optional[str] = None,
        append_options: Optional[Dict[str, Any]] = None,
    ) -> None:
        body: Dict[str, Any] = {
            "namespace": self.options.namespace_name,
            "table": self.options.table_name,
            "table_name": self.options.table_name,
            "entries": list(entries),
        }
        if count_key is not None:
            body["count_key"] = count_key
        if count_value is not None:
            body["count_value"] = count_value
        if append_options is not None:
            body["append_options_json"] = json.dumps(
                append_options, sort_keys=True, separators=(",", ":")
            )
        self._post("/matrixark/append_records", body)

    def matrixark_append_records(
        self,
        entries: Iterable[Dict[str, Any]],
        *,
        count_key: Optional[str] = None,
        count_value: Optional[str] = None,
        append_options: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.matrixark_batch_append_records(
            entries,
            count_key=count_key,
            count_value=count_value,
            append_options=append_options,
        )

    def matrixark_scan_candidates(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        scope: Dict[str, Any],
        record_types: List[str],
        secondary_index_groups: List[List[str]],
        selected_node_hashes: List[int],
    ) -> Dict[str, Any]:
        body = {
            "namespace": self.options.namespace_name,
            "table": self.options.table_name,
            "table_name": self.options.table_name,
            "count_key": count_key,
            "record_hash_key": record_hash_key,
            "shard_size": int(shard_size),
            "scope": scope,
            "record_types": record_types,
            "secondary_index_groups": secondary_index_groups,
            "selected_node_hashes": selected_node_hashes,
        }
        return self._post("/matrixark/scan_candidates", body)

    def matrixark_retrieve_context_pack(
        self,
        *,
        count_key: str,
        record_hash_key: str,
        shard_size: int,
        request: Dict[str, Any],
    ) -> Dict[str, Any]:
        body = {
            "namespace": self.options.namespace_name,
            "table": self.options.table_name,
            "table_name": self.options.table_name,
            "count_key": count_key,
            "record_hash_key": record_hash_key,
            "shard_size": int(shard_size),
            "request": request,
        }
        return self._post("/matrixark/retrieve_context_pack", body)

    def hget(self, key: str, field: str) -> str:
        body = self._key_body(key)
        body["field"] = field
        data = self._post("/ProxyService/HGet", body)
        return _string_value(data.get("value", ""))

    def hmset(self, key: str, entries: Dict[str, str]) -> None:
        body = self._key_body(key)
        body["entries"] = [[field, _bytes_value(value)] for field, value in entries.items()]
        self._post("/ProxyService/HMSet", body)

    def hmget(self, key: str, fields: Iterable[str]) -> List[Optional[str]]:
        body = self._key_body(key)
        body["fields"] = list(fields)
        data = self._post("/ProxyService/HMGet", body)
        return [None if value is None else _string_value(value) for value in data.get("values", [])]

    def hgetall(self, key: str) -> Dict[str, str]:
        data = self._post("/ProxyService/HGetAll", self._key_body(key))
        if "fields" in data and "values" in data:
            return {
                str(field): _string_value(value)
                for field, value in zip(data.get("fields", []), data.get("values", []))
            }
        return {
            str(entry[0]): _string_value(entry[1])
            for entry in data.get("entries", [])
            if isinstance(entry, list) and len(entry) >= 2
        }

    def hlen(self, key: str) -> int:
        data = self._post("/ProxyService/HLen", self._key_body(key))
        return int(data.get("len", data.get("value", 0)))

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

    def srem(self, key: str, member: str) -> None:
        body = self._key_body(key)
        body["member"] = _bytes_value(member)
        self._post("/ProxyService/SRem", body)

    def exists(self, key: str) -> bool:
        data = self._post("/ProxyService/Exists", self._key_body(key))
        return int(data.get("value", 0)) != 0

    def add_feature_points(self, key: str, points: Iterable[FeaturePoint]) -> None:
        self.add_feature_points_with_policy(key, points, FeatureWritePolicy.UPSERT)

    def add_feature_points_with_policy(
        self,
        key: str,
        points: Iterable[FeaturePoint],
        policy: FeatureWritePolicy = FeatureWritePolicy.UPSERT,
    ) -> None:
        body = self._key_body(key)
        body["points"] = [_feature_point_body(point) for point in points]
        body["policy"] = int(policy)
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

    def replace_feature_points(
        self,
        key: str,
        start_ts: int,
        end_ts: int,
        points: Iterable[FeaturePoint],
    ) -> None:
        body = self._key_body(key)
        body.update(
            {
                "start_ms": start_ts,
                "end_ms": end_ts,
                "points": [_feature_point_body(point) for point in points],
            }
        )
        self._post("/ProxyService/FeatureReplace", body)

    def delete_feature_points(self, key: str) -> None:
        self._post("/ProxyService/FeatureDelete", self._key_body(key))

    def aggregate_feature_points(
        self,
        key: str,
        start_ts: int,
        end_ts: int,
        aggregator: str,
        count: Optional[int] = None,
    ) -> int:
        body = self._key_body(key)
        body.update(
            {
                "start_ms": start_ts,
                "end_ms": end_ts,
                "aggregator": aggregator,
                "count": count,
            }
        )
        data = self._post("/ProxyService/FeatureAggQuery", body)
        return int(data.get("value", 0))

    def add_sequence_feature_rows(self, key: str, rows: Iterable[SequenceFeatureRow]) -> None:
        self.add_sequence_feature_rows_with_policy(key, rows, FeatureWritePolicy.UPSERT)

    def add_sequence_feature_rows_with_policy(
        self,
        key: str,
        rows: Iterable[SequenceFeatureRow],
        policy: FeatureWritePolicy = FeatureWritePolicy.UPSERT,
    ) -> None:
        body = self._key_body(key)
        body["rows"] = [_sequence_row_body(row) for row in rows]
        body["policy"] = int(policy)
        self._post("/ProxyService/SequenceAdd", body)

    def query_sequence_feature_rows(
        self,
        key: str,
        start_ts: int,
        end_ts: int,
        count: int,
        filters: Iterable[FeatureFilter] = (),
    ) -> List[SequenceFeatureRow]:
        body = self._query_body(key, start_ts, end_ts, count, filters)
        data = self._post("/ProxyService/SequenceQuery", body)
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

    def add_ips_instance(
        self,
        table: str,
        uid: int,
        timestamp_us: int,
        action_type: int,
        logical_table: int,
        features: Iterable[IpsFeatureStat],
    ) -> None:
        body = {
            "namespace": self.options.namespace_name,
            "table": self.options.table_name,
            "table_name": self.options.table_name,
            "ips_table": table,
            "key": table,
            "uid": uid,
            "timestamp_us": timestamp_us,
            "timestamp_ms": int(timestamp_us // 1000),
            "action_type": action_type,
            "table_id": int(logical_table),
            "logical_table": logical_table,
            "features": [asdict(feature) for feature in features],
        }
        body["instance"] = _bytes_value(json.dumps(body["features"], separators=(",", ":")))
        self._post("/ProxyService/IpsAdd", body)

    def query_ips_last_instances(
        self,
        table: str,
        uid: int,
        action_type: int,
        logical_table: int,
        slot: int,
        top_k: int = 20,
        last_instances: int = 10,
    ) -> List[IpsFeatureStat]:
        body = {
            "namespace": self.options.namespace_name,
            "table": self.options.table_name,
            "table_name": self.options.table_name,
            "ips_table": table,
            "key": table,
            "uid": uid,
            "action_type": action_type,
            "logical_table": logical_table,
            "slot": slot,
            "top_k": top_k,
            "last_instances": last_instances,
            "count": int(last_instances),
        }
        data = self._post("/ProxyService/IpsQueryLast", body)
        return [
            IpsFeatureStat(
                int(row.get("id", 0)),
                int(row.get("slot", slot)),
                bool(row.get("has_slot", True)),
                int(row.get("type", 0)),
                int(row.get("v1", 0)),
                int(row.get("v2", 0)),
            )
            for row in data.get("features", [])
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
                "ttl_ms": ttl_seconds * 1000,
                "precision": int(precision),
                "precision_ms": _risk_precision_ms(precision),
                "uuid": uuid,
                "occur_time_seconds": occur_time_seconds,
                "timestamp_ms": _proxy_timestamp_ms(occur_time_seconds),
            }
        )
        self._post("/ProxyService/RiskIncrement", body)

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
        start_ms, end_ms = _risk_window_ms(window_start, window_end, window_unit)
        body["start_ms"] = start_ms
        body["end_ms"] = end_ms
        data = self._post("/ProxyService/RiskCount", body)
        return int(data.get("count", 0))

    def risk_hset(self, key: str, timestamp_ms: int, amount: int) -> None:
        body = self._key_body(key)
        body.update({"timestamp_ms": timestamp_ms, "amount": amount})
        self._post("/ProxyService/RiskHset", body)

    def risk_hset_with_options(
        self,
        key: str,
        value: str,
        ttl_seconds: int,
        htype: str = "count",
        occur_time_seconds: int = 0,
        precision: RiskPrecision = RiskPrecision.ONE_MINUTE,
    ) -> None:
        body = self._key_body(key)
        body.update(
            {
                "value": value,
                "ttl": int(ttl_seconds),
                "ttl_ms": int(ttl_seconds) * 1000,
                "htype": htype,
                "occur_time": int(occur_time_seconds),
                "timestamp_ms": _proxy_timestamp_ms(occur_time_seconds),
                "precision_ms": _risk_precision_ms(precision),
            }
        )
        self._post("/ProxyService/RiskHset", body)

    def risk_hquery(
        self,
        key: str,
        precision: RiskPrecision = RiskPrecision.ONE_MINUTE,
        window_start: int = -1,
        window_end: int = 0,
        window_unit: WindowUnit = WindowUnit.HOUR,
        aggregator: str = "sum",
    ) -> List[int]:
        body = self._key_body(key)
        start_ms, end_ms = _risk_window_ms(window_start, window_end, window_unit)
        body.update(
            {
                "start_ms": start_ms,
                "end_ms": end_ms,
                "precision_ms": _risk_precision_ms(precision),
                "aggregator": aggregator,
            }
        )
        data = self._post("/ProxyService/RiskHquery", body)
        return [int(item.get("result", 0)) for item in data.get("result_list", [])]

    def risk_cpc_set(
        self,
        key: str,
        values: Iterable[str],
        timestamp_ms: Optional[int] = None,
        ttl_ms: int = 0,
        precision: RiskPrecision = RiskPrecision.ONE_MINUTE,
        dont_upgrade_cpc: bool = False,
    ) -> None:
        body = self._key_body(key)
        body.update(
            {
                "values": list(values),
                "timestamp_ms": int(timestamp_ms if timestamp_ms is not None else time.time() * 1000),
                "ttl_ms": ttl_ms,
                "precision_ms": _risk_precision_ms(precision),
                "dont_upgrade_cpc": dont_upgrade_cpc,
            }
        )
        self._post("/ProxyService/RiskCPCSet", body)

    def risk_cpc_query(
        self,
        key: str,
        precision: RiskPrecision = RiskPrecision.ONE_MINUTE,
        window_start: int = -1,
        window_end: int = 0,
        window_unit: WindowUnit = WindowUnit.HOUR,
    ) -> List[int]:
        body = self._key_body(key)
        start_ms, end_ms = _risk_window_ms(window_start, window_end, window_unit)
        body.update(
            {
                "start_ms": start_ms,
                "end_ms": end_ms,
                "precision_ms": _risk_precision_ms(precision),
            }
        )
        data = self._post("/ProxyService/RiskCPCQuery", body)
        return [int(value) for value in data.get("count_list", [])]

    def risk_fol_set(
        self,
        key: str,
        value: str,
        occur_time_ms: int,
        ttl_ms: int,
        fol_type: str = "last",
    ) -> None:
        body = self._key_body(key)
        body.update(
            {
                "value": value,
                "value_bytes": _bytes_value(value),
                "occur_time": int(occur_time_ms) // 1000,
                "occur_time_ms": occur_time_ms,
                "ttl": int(ttl_ms) // 1000,
                "ttl_ms": ttl_ms,
                "fol_type": fol_type,
            }
        )
        self._post("/ProxyService/RiskFolSet", body)

    def risk_fol_query(self, key: str) -> str:
        data = self._post("/ProxyService/RiskFolQuery", self._key_body(key))
        return str(data.get("result", _string_value(data.get("value", ""))))

    def risk_manager(self, key: str) -> Dict[str, str]:
        data = self._post("/ProxyService/RiskManager", self._key_body(key))
        return dict(data.get("entries", {}))

    def risk_manager_with_options(
        self,
        key: str,
        op_type: str,
        field_list: Optional[Iterable[Dict[str, str]]] = None,
        start_offset: str = "",
        end_offset: str = "",
        is_cpc: bool = False,
    ) -> Dict[str, str]:
        body = self._key_body(key)
        body.update(
            {
                "op_type": op_type,
                "field_list": list(field_list or []),
                "start_offset": start_offset,
                "end_offset": end_offset,
                "is_cpc": is_cpc,
            }
        )
        data = self._post("/ProxyService/RiskManager", body)
        return dict(data.get("entries", {}))

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
        FeatureFilterOp.GREATER_OR_EQUAL: "greater_or_equal",
        FeatureFilterOp.LESS_OR_EQUAL: "less_or_equal",
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
    if kind == "values":
        values = list(response.get("values", []))
        exists = response.get("exists")
        if isinstance(exists, list):
            return {
                "values": [
                    _string_value(value) if index < len(exists) and bool(exists[index]) else None
                    for index, value in enumerate(values)
                ],
                "exists": [bool(item) for item in exists],
            }
        return {
            "values": [
                None if item is None else _string_value(item)
                for item in values
            ]
        }
    if kind == "hash_entries":
        entries: Dict[str, str] = {}
        for item in response.get("entries", []):
            if isinstance(item, list) and len(item) >= 2:
                entries[str(item[0])] = _string_value(item[1])
        return {"entries": entries}
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


def _proxy_timestamp_ms(occur_time_seconds: int) -> int:
    if occur_time_seconds:
        return int(occur_time_seconds) * 1000
    return int(time.time() * 1000)


def _risk_precision_ms(precision: RiskPrecision) -> int:
    values = {
        RiskPrecision.ONE_SECOND: 1000,
        RiskPrecision.FIVE_SECONDS: 5000,
        RiskPrecision.TEN_SECONDS: 10000,
        RiskPrecision.ONE_MINUTE: 60000,
        RiskPrecision.FIVE_MINUTES: 5 * 60000,
        RiskPrecision.TEN_MINUTES: 10 * 60000,
        RiskPrecision.ONE_HOUR: 60 * 60000,
        RiskPrecision.ONE_DAY: 24 * 60 * 60000,
        RiskPrecision.ONE_MONTH: 30 * 24 * 60 * 60000,
    }
    return values.get(precision, 60000)


def _risk_window_ms(window_start: int, window_end: int, unit: WindowUnit) -> tuple[int, int]:
    end = int(window_end) if window_end > 0 else int(time.time() * 1000)
    start = int(window_start)
    if start < 0:
        start = end - _window_unit_ms(unit)
    return max(start, 0), end


def _window_unit_ms(unit: WindowUnit) -> int:
    values = {
        WindowUnit.SECOND: 1000,
        WindowUnit.MINUTE: 60000,
        WindowUnit.HOUR: 60 * 60000,
        WindowUnit.DAY: 24 * 60 * 60000,
    }
    return values.get(unit, 60 * 60000)
