# SPDX-License-Identifier: Apache-2.0
"""Typed clients for TemporalStore's protobuf command service."""

from temporalstore.v1 import temporalstore_pb2 as commands

from .client import CommandClient

__all__ = ["CommandClient", "commands"]
