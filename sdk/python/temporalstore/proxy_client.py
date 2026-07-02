from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List, Optional

from .client import (
    FeatureFilter,
    FeaturePoint,
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
        body["value"] = value
        if ttl_ms is not None:
            body["ttl_ms"] = ttl_ms
        self._post("/v1/string/put", body)

    def get_string(self, key: str) -> str:
        data = self._post("/v1/string/get", self._key_body(key))
        return str(data.get("value", ""))

    def delete_object(self, key: str) -> None:
        self._post("/v1/common/delete", self._key_body(key))

    def expire(self, key: str, ttl_ms: int) -> None:
        body = self._key_body(key)
        body["ttl_ms"] = ttl_ms
        self._post("/v1/common/expire", body)

    def ttl(self, key: str) -> int:
        data = self._post("/v1/common/ttl", self._key_body(key))
        return int(data.get("ttl_ms", 0))

    def hset(self, key: str, field: str, value: str) -> None:
        body = self._key_body(key)
        body.update({"field": field, "value": value})
        self._post("/v1/hash/hset", body)

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
        self.batch_hset(entries)
        if count_key is not None and count_value is not None:
            self.put_string(count_key, count_value)

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

    def hget(self, key: str, field: str) -> str:
        body = self._key_body(key)
        body["field"] = field
        data = self._post("/v1/hash/hget", body)
        return str(data.get("value", ""))

    def hdel(self, key: str, field: str) -> None:
        body = self._key_body(key)
        body["field"] = field
        self._post("/v1/hash/hdel", body)

    def sadd(self, key: str, member: str) -> None:
        body = self._key_body(key)
        body["member"] = member
        self._post("/v1/set/sadd", body)

    def smembers(self, key: str) -> List[str]:
        data = self._post("/v1/set/smembers", self._key_body(key))
        return [str(item) for item in data.get("members", [])]

    def add_feature_points(self, key: str, points: Iterable[FeaturePoint]) -> None:
        body = self._key_body(key)
        body["points"] = [asdict(point) for point in points]
        self._post("/v1/feature/add", body)

    def query_feature_points(
        self,
        key: str,
        start_ts: int,
        end_ts: int,
        count: int,
        filters: Iterable[FeatureFilter] = (),
    ) -> List[FeaturePoint]:
        body = self._query_body(key, start_ts, end_ts, count, filters)
        data = self._post("/v1/feature/query", body)
        return [FeaturePoint(int(row["timestamp"]), str(row["value"])) for row in data.get("points", [])]

    def add_sequence_feature_rows(self, key: str, rows: Iterable[SequenceFeatureRow]) -> None:
        body = self._key_body(key)
        body["rows"] = [asdict(row) for row in rows]
        self._post("/v1/sequence/add", body)

    def query_sequence_feature_rows(
        self,
        key: str,
        start_ts: int,
        end_ts: int,
        count: int,
        filters: Iterable[FeatureFilter] = (),
    ) -> List[SequenceFeatureRow]:
        body = self._query_body(key, start_ts, end_ts, count, filters)
        data = self._post("/v1/sequence/query", body)
        return [
            SequenceFeatureRow(
                int(row["timestamp"]),
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
            "ips_table": table,
            "uid": uid,
            "timestamp_us": timestamp_us,
            "action_type": action_type,
            "logical_table": logical_table,
            "features": [asdict(feature) for feature in features],
        }
        self._post("/v1/ips/add", body)

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
            "ips_table": table,
            "uid": uid,
            "action_type": action_type,
            "logical_table": logical_table,
            "slot": slot,
            "top_k": top_k,
            "last_instances": last_instances,
        }
        data = self._post("/v1/ips/query_last", body)
        return [
            IpsFeatureStat(
                int(row["id"]),
                int(row["slot"]),
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
                "precision": int(precision),
                "uuid": uuid,
                "occur_time_seconds": occur_time_seconds,
            }
        )
        self._post("/v1/risk/increment", body)

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
        data = self._post("/v1/risk/count", body)
        return int(data.get("count", 0))

    def _key_body(self, key: str) -> Dict[str, Any]:
        return {
            "namespace": self.options.namespace_name,
            "table": self.options.table_name,
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
                "count": count,
                "filters": [asdict(item) for item in filters],
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

        if not payload.get("ok", False):
            raise TemporalStoreError(int(payload.get("code", 0)), str(payload.get("message", "")))
        data = payload.get("data", {})
        return data if isinstance(data, dict) else {}

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.options.api_key:
            headers["Authorization"] = "Bearer " + self.options.api_key
        return headers
