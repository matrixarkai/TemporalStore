# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum


class TemporalStoreError(RuntimeError):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code


class FeatureFilterOp(IntEnum):
    EQUAL = 0
    NOT_EQUAL = 1
    GREATER_THAN = 2
    LESS_THAN = 3


class RiskPrecision(IntEnum):
    ONE_SECOND = 0
    FIVE_SECONDS = 1
    TEN_SECONDS = 2
    ONE_MINUTE = 3
    FIVE_MINUTES = 4
    TEN_MINUTES = 5
    ONE_HOUR = 6
    ONE_DAY = 7
    ONE_MONTH = 8


class WindowUnit(IntEnum):
    SECOND = 0
    MINUTE = 1
    HOUR = 2
    DAY = 3


@dataclass
class Options:
    metaserver_addr: str
    namespace_name: str
    table_name: str
    idc: str = "vdc1"
    metaserver_consul: str = ""
    host: str = "127.0.0.1"
    psm: str = "temporalstore.python.client"
    log_dir: str = "./"
    log_level: int = 3
    io_timeout_ms: int = 1000
    connect_timeout_ms: int = 1000
    request_timeout_ms: int = 5000
    max_read_retries: int = 1
    max_write_retries: int = 0
    retry_backoff_ms: int = 2
    max_feature_points_per_request: int = 1000
    max_feature_query_count: int = 5000
    max_key_bytes: int = 4096
    max_value_bytes: int = 16 * 1024 * 1024
    pin_primary: bool = True


@dataclass
class FeatureFilter:
    field: str
    op: FeatureFilterOp = FeatureFilterOp.EQUAL
    value: int = 0


@dataclass
class FeaturePoint:
    timestamp: int
    value: str


@dataclass
class SequenceFeatureRow:
    timestamp: int
    gid: int
    action_type: int
    duration: int
    author_id: int
