# SPDX-License-Identifier: Apache-2.0
"""Namespace and table topology from the TemporalStore metaserver."""

from dataclasses import dataclass
from http.client import HTTPConnection, HTTPSConnection
import json
from typing import Callable, Optional
from urllib.parse import urlsplit


def _crc_entry(byte: int) -> int:
    crc = byte
    for _ in range(8):
        crc = (crc >> 1) ^ (0x95AC9329AC4BC9B5 if crc & 1 else 0)
    return crc


_CRC_TABLE = tuple(_crc_entry(byte) for byte in range(256))


def _crc64(key: str) -> int:
    crc = 0
    for byte in key.encode("utf-8"):
        crc = _CRC_TABLE[(crc ^ byte) & 255] ^ (crc >> 8)
    return crc


def _bucket_for_key(key: str) -> int:
    return _crc64(key) >> 34


class MetadataError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


class RaftPrimaryUnavailable(MetadataError):
    """A table has elected no primary yet, and no command was sent."""

    def __init__(self, shard_id: int):
        super().__init__("no_grpc_primary", f"shard {shard_id} has no elected gRPC primary")


@dataclass(frozen=True)
class Shard:
    shard_id: int
    primary: Optional[str]
    replicas: tuple[str, ...]
    load_version: int
    primary_grpc: Optional[str] = None
    replica_grpc: tuple[str, ...] = ()
    uses_raft_route: bool = False


@dataclass(frozen=True)
class TableTopology:
    namespace: str
    name: str
    version: int
    first_shard_id: int
    shard_count: int
    shards: tuple[Shard, ...]
    table_id: Optional[int] = None

    def shard_for_key(self, key: str) -> Shard:
        if self.shard_count <= 0:
            raise MetadataError("invalid_topology", "table has no shards")
        position = 0 if self.shard_count == 1 else _bucket_for_key(key) % self.shard_count
        shard_id = self.first_shard_id + position
        if position >= len(self.shards) or self.shards[position].shard_id != shard_id:
            raise MetadataError("invalid_topology", f"table is missing shard {shard_id}")
        return self.shards[position]


class MetadataClient:
    """Resolve a table using the metaserver's control-plane HTTP API."""

    def __init__(
        self,
        url: str,
        *,
        credential_provider: Optional[Callable[[], Optional[str]]] = None,
        timeout_seconds: float = 5.0,
    ):
        endpoint = urlsplit(url)
        if (endpoint.scheme not in ("http", "https") or not endpoint.hostname
                or endpoint.path not in ("", "/") or endpoint.username is not None
                or endpoint.query or endpoint.fragment):
            raise ValueError("expected a metaserver base URL")
        self._connect = HTTPSConnection if endpoint.scheme == "https" else HTTPConnection
        self._host = endpoint.hostname
        self._port = endpoint.port
        self._timeout = timeout_seconds
        self._credentials = credential_provider

    def table_topology(
        self,
        namespace: str,
        table: str,
        *,
        previous: Optional[TableTopology] = None,
        location: str = "",
    ) -> TableTopology:
        if not namespace or not table:
            raise ValueError("namespace and table are required")
        if previous is not None and (namespace, table) != (previous.namespace, previous.name):
            raise ValueError("previous topology is for a different table")
        request = {
            "namespace": namespace, "table_name": table, "client_location": location,
            "old_topology_version": previous.version if previous else 0,
        }
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        token = self._credentials() if self._credentials is not None else None
        if token:
            headers["Authorization"] = f"Bearer {token}"
        connection = self._connect(self._host, self._port, timeout=self._timeout)
        try:
            connection.request("POST", "/tables/topology", json.dumps(request), headers)
            response = connection.getresponse()
            if response.status != 200:
                raise MetadataError(f"http_{response.status}", response.reason)
            body = json.load(response)
        finally:
            connection.close()
        status = body["status"]
        if not status["ok"]:
            raise MetadataError(status["code"], status["message"])
        if body["unchanged"] and previous is not None:
            return previous
        info = body["table"]
        if not info or (info["namespace"], info["table_name"]) != (namespace, table):
            raise MetadataError("invalid_topology", "metaserver returned a different table")
        return TableTopology(
            namespace, table, info["topology_version"], info["first_shard_id"], info["shard_count"],
            tuple(
                self._decode_shard(row)
                for row in sorted(body["shards"], key=lambda item: item["shard_id"])
            ),
            table_id=info.get("table_id"),
        )

    @staticmethod
    def _decode_shard(row) -> Shard:
        raft_route = row.get("raft_route")
        if raft_route is None:
            endpoint = row.get("primary_endpoint") or {}
            primary = row["primary"]
            version = row.get("load_version", 0)
        else:
            endpoint = raft_route.get("endpoint") or {}
            primary = endpoint.get("server_addr")
            version = raft_route["load_version"]
        return Shard(
            row["shard_id"], primary, tuple(row["replicas"]), version, endpoint.get("grpc_addr"),
            tuple(endpoint["grpc_addr"] for endpoint in row.get("replica_endpoints", ())
                  if endpoint.get("grpc_addr")),
            uses_raft_route=raft_route is not None,
        )
