# SPDX-License-Identifier: Apache-2.0
"""Typed clients for TemporalStore's protobuf command service."""

from temporalstore.v1 import temporalstore_pb2 as commands

from .client import CommandClient
from .metadata import MetadataClient, MetadataError, RaftPrimaryUnavailable, Shard, TableTopology
from .proxy import ProxyCommandClient, ProxyTableClient
from .table import TableClient
from .values import decode_hash_entries, decode_integer, decode_value, decode_values

__all__ = [
    "CommandClient", "MetadataClient", "MetadataError", "RaftPrimaryUnavailable",
    "ProxyCommandClient", "ProxyTableClient", "Shard", "TableClient", "TableTopology", "commands", "decode_hash_entries",
    "decode_integer", "decode_value", "decode_values",
]
