from __future__ import annotations

import ctypes
import ctypes.util
import json
import os
from dataclasses import dataclass
from enum import IntEnum
from typing import Iterable, List, Optional


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


@dataclass
class IpsFeatureStat:
    id: int
    slot: int
    has_slot: bool = True
    type: int = 0
    v1: int = 0
    v2: int = 0


class _COptions(ctypes.Structure):
    _fields_ = [
        ("metaserver_addr", ctypes.c_char_p),
        ("metaserver_consul", ctypes.c_char_p),
        ("namespace_name", ctypes.c_char_p),
        ("table_name", ctypes.c_char_p),
        ("idc", ctypes.c_char_p),
        ("host", ctypes.c_char_p),
        ("psm", ctypes.c_char_p),
        ("log_dir", ctypes.c_char_p),
        ("log_level", ctypes.c_int),
        ("io_timeout_ms", ctypes.c_int),
        ("connect_timeout_ms", ctypes.c_int),
        ("request_timeout_ms", ctypes.c_int),
        ("max_read_retries", ctypes.c_int),
        ("max_write_retries", ctypes.c_int),
        ("retry_backoff_ms", ctypes.c_int),
        ("max_feature_points_per_request", ctypes.c_int),
        ("max_feature_query_count", ctypes.c_uint64),
        ("max_key_bytes", ctypes.c_uint64),
        ("max_value_bytes", ctypes.c_uint64),
        ("pin_primary", ctypes.c_int),
    ]


class _CStringArray(ctypes.Structure):
    _fields_ = [
        ("count", ctypes.c_size_t),
        ("values", ctypes.POINTER(ctypes.c_char_p)),
    ]


class _CHashEntry(ctypes.Structure):
    _fields_ = [
        ("key", ctypes.c_char_p),
        ("field", ctypes.c_char_p),
        ("value", ctypes.c_char_p),
        ("route_json", ctypes.c_char_p),
    ]


class _CFeaturePoint(ctypes.Structure):
    _fields_ = [
        ("timestamp", ctypes.c_uint64),
        ("value", ctypes.c_char_p),
    ]


class _CFeaturePointArray(ctypes.Structure):
    _fields_ = [
        ("count", ctypes.c_size_t),
        ("points", ctypes.POINTER(_CFeaturePoint)),
    ]


class _CFeatureFilter(ctypes.Structure):
    _fields_ = [
        ("field", ctypes.c_char_p),
        ("op", ctypes.c_int),
        ("value", ctypes.c_uint64),
    ]


class _CSequenceFeatureRow(ctypes.Structure):
    _fields_ = [
        ("timestamp", ctypes.c_uint64),
        ("gid", ctypes.c_uint64),
        ("action_type", ctypes.c_uint32),
        ("duration", ctypes.c_uint32),
        ("author_id", ctypes.c_uint64),
    ]


class _CSequenceFeatureRowArray(ctypes.Structure):
    _fields_ = [
        ("count", ctypes.c_size_t),
        ("rows", ctypes.POINTER(_CSequenceFeatureRow)),
    ]


class _CIpsFeatureStat(ctypes.Structure):
    _fields_ = [
        ("id", ctypes.c_int64),
        ("slot", ctypes.c_int32),
        ("has_slot", ctypes.c_int),
        ("type", ctypes.c_int32),
        ("v1", ctypes.c_int32),
        ("v2", ctypes.c_int32),
    ]


class _CIpsFeatureArray(ctypes.Structure):
    _fields_ = [
        ("count", ctypes.c_size_t),
        ("features", ctypes.POINTER(_CIpsFeatureStat)),
    ]


def _encode(value: str) -> bytes:
    return value.encode("utf-8")


def _load_library(path: Optional[str] = None) -> ctypes.CDLL:
    candidates = []
    if path:
        candidates.append(path)
    env_path = os.environ.get("TEMPORALSTORE_LIB")
    if env_path:
        candidates.append(env_path)
    candidates.extend(["libbcache2.so", "bcache2.dll", "libbcache2.dylib"])

    load_mode = getattr(ctypes, "RTLD_GLOBAL", None)
    preload_errors = []
    for dependency in ("crypto", "ssl", "fmt", "leveldb", "rocksdb", "snappy", "lz4", "zstd", "bz2"):
        dependency_path = ctypes.util.find_library(dependency)
        if not dependency_path:
            continue
        try:
            if load_mode is None:
                ctypes.CDLL(dependency_path)
            else:
                ctypes.CDLL(dependency_path, mode=load_mode)
        except OSError as exc:
            preload_errors.append(f"{dependency_path}: {exc}")

    errors = []
    for candidate in candidates:
        try:
            if load_mode is None:
                return ctypes.CDLL(candidate)
            return ctypes.CDLL(candidate, mode=load_mode)
        except OSError as exc:
            errors.append(f"{candidate}: {exc}")
    if preload_errors:
        errors.extend(f"preload {error}" for error in preload_errors)
    raise TemporalStoreError(0, "failed to load TemporalStore library: " + "; ".join(errors))


def _optional_pointer(values, ctype):
    if len(values) == 0:
        return None
    return ctypes.cast(values, ctypes.POINTER(ctype))


class _Native:
    def __init__(self, lib: ctypes.CDLL):
        self.lib = lib
        self._bind()

    def _bind(self) -> None:
        lib = self.lib
        lib.temporalstore_options_init.argtypes = [ctypes.POINTER(_COptions)]
        lib.temporalstore_options_init.restype = None
        lib.temporalstore_free_string.argtypes = [ctypes.c_void_p]
        lib.temporalstore_free_string.restype = None
        lib.temporalstore_string_array_free.argtypes = [ctypes.POINTER(_CStringArray)]
        lib.temporalstore_string_array_free.restype = None
        lib.temporalstore_feature_point_array_free.argtypes = [ctypes.POINTER(_CFeaturePointArray)]
        lib.temporalstore_feature_point_array_free.restype = None
        lib.temporalstore_sequence_feature_row_array_free.argtypes = [
            ctypes.POINTER(_CSequenceFeatureRowArray)
        ]
        lib.temporalstore_sequence_feature_row_array_free.restype = None
        lib.temporalstore_ips_feature_array_free.argtypes = [ctypes.POINTER(_CIpsFeatureArray)]
        lib.temporalstore_ips_feature_array_free.restype = None

        lib.temporalstore_connect.argtypes = [
            ctypes.POINTER(_COptions),
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_connect.restype = ctypes.c_int
        lib.temporalstore_close.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
        lib.temporalstore_close.restype = ctypes.c_int
        lib.temporalstore_put_string.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_put_string.restype = ctypes.c_int
        lib.temporalstore_put_string_with_ttl.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_put_string_with_ttl.restype = ctypes.c_int
        lib.temporalstore_get_string.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_get_string.restype = ctypes.c_int
        lib.temporalstore_delete_object.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_delete_object.restype = ctypes.c_int
        lib.temporalstore_expire.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_expire.restype = ctypes.c_int
        lib.temporalstore_ttl.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_ttl.restype = ctypes.c_int
        lib.temporalstore_hset.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_hset.restype = ctypes.c_int
        lib.temporalstore_hget.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_hget.restype = ctypes.c_int
        lib.temporalstore_hdel.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_hdel.restype = ctypes.c_int
        lib.temporalstore_sadd.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_sadd.restype = ctypes.c_int
        lib.temporalstore_smembers.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.POINTER(_CStringArray),
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_smembers.restype = ctypes.c_int

        self.has_matrixark_batch_append_records = hasattr(
            lib, "temporalstore_matrixark_batch_append_records"
        )
        if self.has_matrixark_batch_append_records:
            lib.temporalstore_matrixark_batch_append_records.argtypes = [
                ctypes.c_void_p,
                ctypes.POINTER(_CHashEntry),
                ctypes.c_size_t,
                ctypes.c_char_p,
                ctypes.c_char_p,
                ctypes.POINTER(ctypes.c_void_p),
            ]
            lib.temporalstore_matrixark_batch_append_records.restype = ctypes.c_int
        self.has_matrixark_retrieve_context_pack = hasattr(
            lib, "temporalstore_matrixark_retrieve_context_pack"
        )
        if self.has_matrixark_retrieve_context_pack:
            lib.temporalstore_matrixark_retrieve_context_pack.argtypes = [
                ctypes.c_void_p,
                ctypes.c_char_p,
                ctypes.POINTER(ctypes.c_void_p),
                ctypes.POINTER(ctypes.c_void_p),
            ]
            lib.temporalstore_matrixark_retrieve_context_pack.restype = ctypes.c_int
        lib.temporalstore_add_feature_points.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.POINTER(_CFeaturePoint),
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_add_feature_points.restype = ctypes.c_int
        lib.temporalstore_query_feature_points_with_filters.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.POINTER(_CFeatureFilter),
            ctypes.c_size_t,
            ctypes.POINTER(_CFeaturePointArray),
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_query_feature_points_with_filters.restype = ctypes.c_int
        lib.temporalstore_add_sequence_feature_rows.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.POINTER(_CSequenceFeatureRow),
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_add_sequence_feature_rows.restype = ctypes.c_int
        lib.temporalstore_query_sequence_feature_rows.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.POINTER(_CFeatureFilter),
            ctypes.c_size_t,
            ctypes.POINTER(_CSequenceFeatureRowArray),
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_query_sequence_feature_rows.restype = ctypes.c_int
        lib.temporalstore_add_ips_instance.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_int64,
            ctypes.c_int64,
            ctypes.c_int32,
            ctypes.c_int32,
            ctypes.POINTER(_CIpsFeatureStat),
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_add_ips_instance.restype = ctypes.c_int
        lib.temporalstore_query_ips_last_instances.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_int64,
            ctypes.c_int32,
            ctypes.c_int32,
            ctypes.c_int32,
            ctypes.c_int32,
            ctypes.c_int64,
            ctypes.POINTER(_CIpsFeatureArray),
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_query_ips_last_instances.restype = ctypes.c_int
        lib.temporalstore_risk_increment.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_int64,
            ctypes.c_uint64,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint64,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_risk_increment.restype = ctypes.c_int
        lib.temporalstore_risk_count.argtypes = [
            ctypes.c_void_p,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_int64,
            ctypes.c_int64,
            ctypes.c_int,
            ctypes.POINTER(ctypes.c_int64),
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.temporalstore_risk_count.restype = ctypes.c_int

    def check(self, code: int, error: ctypes.c_void_p) -> None:
        if code == 0:
            return
        message = "unknown TemporalStore error"
        if error:
            message = ctypes.cast(error, ctypes.c_char_p).value.decode("utf-8", errors="replace")
            self.lib.temporalstore_free_string(error)
        raise TemporalStoreError(code, message)


class Client:
    def __init__(self, options: Options, library_path: Optional[str] = None):
        self._native = _Native(_load_library(library_path))
        self._handle = ctypes.c_void_p()
        self._owned_strings: List[bytes] = []

        c_options = _COptions()
        self._native.lib.temporalstore_options_init(ctypes.byref(c_options))
        self._set_options(c_options, options)

        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_connect(
            ctypes.byref(c_options), ctypes.byref(self._handle), ctypes.byref(error)
        )
        self._native.check(code, error)

    def _bytes(self, value: str) -> bytes:
        encoded = _encode(value)
        self._owned_strings.append(encoded)
        return encoded

    def _set_options(self, c_options: _COptions, options: Options) -> None:
        c_options.metaserver_addr = self._bytes(options.metaserver_addr)
        c_options.metaserver_consul = self._bytes(options.metaserver_consul)
        c_options.namespace_name = self._bytes(options.namespace_name)
        c_options.table_name = self._bytes(options.table_name)
        c_options.idc = self._bytes(options.idc)
        c_options.host = self._bytes(options.host)
        c_options.psm = self._bytes(options.psm)
        c_options.log_dir = self._bytes(options.log_dir)
        c_options.log_level = options.log_level
        c_options.io_timeout_ms = options.io_timeout_ms
        c_options.connect_timeout_ms = options.connect_timeout_ms
        c_options.request_timeout_ms = options.request_timeout_ms
        c_options.max_read_retries = options.max_read_retries
        c_options.max_write_retries = options.max_write_retries
        c_options.retry_backoff_ms = options.retry_backoff_ms
        c_options.max_feature_points_per_request = options.max_feature_points_per_request
        c_options.max_feature_query_count = options.max_feature_query_count
        c_options.max_key_bytes = options.max_key_bytes
        c_options.max_value_bytes = options.max_value_bytes
        c_options.pin_primary = 1 if options.pin_primary else 0

    def close(self) -> None:
        if not self._handle:
            return
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_close(self._handle, ctypes.byref(error))
        self._handle = ctypes.c_void_p()
        self._native.check(code, error)

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def put_string(self, key: str, value: str) -> None:
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_put_string(
            self._handle, _encode(key), _encode(value), ctypes.byref(error)
        )
        self._native.check(code, error)

    def put_string_with_ttl(self, key: str, value: str, ttl_ms: int) -> None:
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_put_string_with_ttl(
            self._handle, _encode(key), _encode(value), ttl_ms, ctypes.byref(error)
        )
        self._native.check(code, error)

    def get_string(self, key: str) -> str:
        value = ctypes.c_void_p()
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_get_string(
            self._handle, _encode(key), ctypes.byref(value), ctypes.byref(error)
        )
        self._native.check(code, error)
        try:
            return ctypes.cast(value, ctypes.c_char_p).value.decode("utf-8", errors="replace")
        finally:
            self._native.lib.temporalstore_free_string(value)

    def delete_object(self, key: str) -> None:
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_delete_object(
            self._handle, _encode(key), ctypes.byref(error)
        )
        self._native.check(code, error)

    def expire(self, key: str, ttl_ms: int) -> None:
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_expire(
            self._handle, _encode(key), ttl_ms, ctypes.byref(error)
        )
        self._native.check(code, error)

    def ttl(self, key: str) -> int:
        value = ctypes.c_uint64()
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_ttl(
            self._handle, _encode(key), ctypes.byref(value), ctypes.byref(error)
        )
        self._native.check(code, error)
        return int(value.value)

    def hset(self, key: str, field: str, value: str) -> None:
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_hset(
            self._handle, _encode(key), _encode(field), _encode(value), ctypes.byref(error)
        )
        self._native.check(code, error)

    def batch_hset(self, entries: Iterable[dict]) -> None:
        for entry in entries:
            self.hset(str(entry["key"]), str(entry["field"]), str(entry.get("value", "")))

    def matrixark_batch_append_records(
        self,
        entries: Iterable[dict],
        *,
        count_key: Optional[str] = None,
        count_value: Optional[str] = None,
        append_options: Optional[dict] = None,
    ) -> None:
        values = list(entries)
        if not self._native.has_matrixark_batch_append_records:
            self.batch_hset(values)
            if count_key is not None and count_value is not None:
                self.put_string(count_key, count_value)
            return

        key_bytes = [_encode(str(entry["key"])) for entry in values]
        field_bytes = [_encode(str(entry["field"])) for entry in values]
        value_bytes = [_encode(str(entry.get("value", ""))) for entry in values]
        route_bytes = [
            _encode(json.dumps(entry.get("storage_route", {}), sort_keys=True, separators=(",", ":")))
            for entry in values
        ]
        array_type = _CHashEntry * len(values)
        c_entries = array_type(
            *[
                _CHashEntry(key_bytes[i], field_bytes[i], value_bytes[i], route_bytes[i])
                for i in range(len(values))
            ]
        )
        count_key_bytes = _encode(count_key) if count_key is not None else None
        count_value_bytes = _encode(count_value) if count_value is not None else None
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_matrixark_batch_append_records(
            self._handle,
            _optional_pointer(c_entries, _CHashEntry),
            len(values),
            count_key_bytes,
            count_value_bytes,
            ctypes.byref(error),
        )
        self._native.check(code, error)

    def matrixark_append_records(
        self,
        entries: Iterable[dict],
        *,
        count_key: Optional[str] = None,
        count_value: Optional[str] = None,
        append_options: Optional[dict] = None,
    ) -> None:
        self.matrixark_batch_append_records(
            entries,
            count_key=count_key,
            count_value=count_value,
            append_options=append_options,
        )

    def matrixark_retrieve_context_pack(self, request: dict | str) -> dict:
        if not self._native.has_matrixark_retrieve_context_pack:
            raise NotImplementedError("native matrixark_retrieve_context_pack is not available in this TemporalStore library")
        request_json = request if isinstance(request, str) else json.dumps(request, sort_keys=True, separators=(",", ":"))
        value = ctypes.c_void_p()
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_matrixark_retrieve_context_pack(
            self._handle,
            _encode(request_json),
            ctypes.byref(value),
            ctypes.byref(error),
        )
        self._native.check(code, error)
        try:
            payload = ctypes.cast(value, ctypes.c_char_p).value.decode("utf-8", errors="replace")
            decoded = json.loads(payload)
            if not isinstance(decoded, dict):
                raise RuntimeError("native matrixark_retrieve_context_pack returned non-object JSON")
            return decoded
        finally:
            self._native.lib.temporalstore_free_string(value)

    def hget(self, key: str, field: str) -> str:
        value = ctypes.c_void_p()
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_hget(
            self._handle, _encode(key), _encode(field), ctypes.byref(value), ctypes.byref(error)
        )
        self._native.check(code, error)
        try:
            return ctypes.cast(value, ctypes.c_char_p).value.decode("utf-8", errors="replace")
        finally:
            self._native.lib.temporalstore_free_string(value)

    def hdel(self, key: str, field: str) -> None:
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_hdel(
            self._handle, _encode(key), _encode(field), ctypes.byref(error)
        )
        self._native.check(code, error)

    def sadd(self, key: str, member: str) -> None:
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_sadd(
            self._handle, _encode(key), _encode(member), ctypes.byref(error)
        )
        self._native.check(code, error)

    def smembers(self, key: str) -> List[str]:
        out = _CStringArray()
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_smembers(
            self._handle, _encode(key), ctypes.byref(out), ctypes.byref(error)
        )
        self._native.check(code, error)
        try:
            return [
                out.values[i].decode("utf-8", errors="replace")
                for i in range(out.count)
            ]
        finally:
            self._native.lib.temporalstore_string_array_free(ctypes.byref(out))

    def add_feature_points(self, key: str, points: Iterable[FeaturePoint]) -> None:
        values = list(points)
        value_strings = [_encode(point.value) for point in values]
        array_type = _CFeaturePoint * len(values)
        c_points = array_type(
            *[_CFeaturePoint(point.timestamp, value_strings[i]) for i, point in enumerate(values)]
        )
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_add_feature_points(
            self._handle,
            _encode(key),
            _optional_pointer(c_points, _CFeaturePoint),
            len(values),
            ctypes.byref(error),
        )
        self._native.check(code, error)

    def query_feature_points(
        self,
        key: str,
        start_ts: int,
        end_ts: int,
        count: int,
        filters: Iterable[FeatureFilter] = (),
    ) -> List[FeaturePoint]:
        c_filters, filter_count, _field_strings = self._build_filters(filters)
        out = _CFeaturePointArray()
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_query_feature_points_with_filters(
            self._handle,
            _encode(key),
            start_ts,
            end_ts,
            count,
            _optional_pointer(c_filters, _CFeatureFilter),
            filter_count,
            ctypes.byref(out),
            ctypes.byref(error),
        )
        self._native.check(code, error)
        try:
            return [
                FeaturePoint(
                    out.points[i].timestamp,
                    out.points[i].value.decode("utf-8", errors="replace"),
                )
                for i in range(out.count)
            ]
        finally:
            self._native.lib.temporalstore_feature_point_array_free(ctypes.byref(out))

    def add_sequence_feature_rows(self, key: str, rows: Iterable[SequenceFeatureRow]) -> None:
        values = list(rows)
        array_type = _CSequenceFeatureRow * len(values)
        c_rows = array_type(
            *[
                _CSequenceFeatureRow(
                    row.timestamp, row.gid, row.action_type, row.duration, row.author_id
                )
                for row in values
            ]
        )
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_add_sequence_feature_rows(
            self._handle,
            _encode(key),
            _optional_pointer(c_rows, _CSequenceFeatureRow),
            len(values),
            ctypes.byref(error),
        )
        self._native.check(code, error)

    def query_sequence_feature_rows(
        self,
        key: str,
        start_ts: int,
        end_ts: int,
        count: int,
        filters: Iterable[FeatureFilter] = (),
    ) -> List[SequenceFeatureRow]:
        c_filters, filter_count, _field_strings = self._build_filters(filters)
        out = _CSequenceFeatureRowArray()
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_query_sequence_feature_rows(
            self._handle,
            _encode(key),
            start_ts,
            end_ts,
            count,
            _optional_pointer(c_filters, _CFeatureFilter),
            filter_count,
            ctypes.byref(out),
            ctypes.byref(error),
        )
        self._native.check(code, error)
        try:
            return [
                SequenceFeatureRow(
                    out.rows[i].timestamp,
                    out.rows[i].gid,
                    out.rows[i].action_type,
                    out.rows[i].duration,
                    out.rows[i].author_id,
                )
                for i in range(out.count)
            ]
        finally:
            self._native.lib.temporalstore_sequence_feature_row_array_free(ctypes.byref(out))

    def add_ips_instance(
        self,
        table: str,
        uid: int,
        timestamp_us: int,
        action_type: int,
        logical_table: int,
        features: Iterable[IpsFeatureStat],
    ) -> None:
        values = list(features)
        array_type = _CIpsFeatureStat * len(values)
        c_features = array_type(
            *[
                _CIpsFeatureStat(
                    item.id,
                    item.slot,
                    1 if item.has_slot else 0,
                    item.type,
                    item.v1,
                    item.v2,
                )
                for item in values
            ]
        )
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_add_ips_instance(
            self._handle,
            _encode(table),
            uid,
            timestamp_us,
            action_type,
            logical_table,
            _optional_pointer(c_features, _CIpsFeatureStat),
            len(values),
            ctypes.byref(error),
        )
        self._native.check(code, error)

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
        out = _CIpsFeatureArray()
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_query_ips_last_instances(
            self._handle,
            _encode(table),
            uid,
            action_type,
            logical_table,
            slot,
            top_k,
            last_instances,
            ctypes.byref(out),
            ctypes.byref(error),
        )
        self._native.check(code, error)
        try:
            return [
                IpsFeatureStat(
                    out.features[i].id,
                    out.features[i].slot,
                    out.features[i].has_slot != 0,
                    out.features[i].type,
                    out.features[i].v1,
                    out.features[i].v2,
                )
                for i in range(out.count)
            ]
        finally:
            self._native.lib.temporalstore_ips_feature_array_free(ctypes.byref(out))

    def risk_increment(
        self,
        key: str,
        amount: int = 1,
        ttl_seconds: int = 24 * 3600,
        precision: RiskPrecision = RiskPrecision.ONE_MINUTE,
        uuid: str = "",
        occur_time_seconds: int = 0,
    ) -> None:
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_risk_increment(
            self._handle,
            _encode(key),
            amount,
            ttl_seconds,
            int(precision),
            _encode(uuid),
            occur_time_seconds,
            ctypes.byref(error),
        )
        self._native.check(code, error)

    def risk_count(
        self,
        key: str,
        precision: RiskPrecision = RiskPrecision.ONE_MINUTE,
        window_start: int = -1,
        window_end: int = 0,
        window_unit: WindowUnit = WindowUnit.HOUR,
    ) -> int:
        value = ctypes.c_int64()
        error = ctypes.c_void_p()
        code = self._native.lib.temporalstore_risk_count(
            self._handle,
            _encode(key),
            int(precision),
            window_start,
            window_end,
            int(window_unit),
            ctypes.byref(value),
            ctypes.byref(error),
        )
        self._native.check(code, error)
        return int(value.value)

    def _build_filters(self, filters: Iterable[FeatureFilter]):
        filter_values = list(filters)
        field_strings = [_encode(item.field) for item in filter_values]
        filter_type = _CFeatureFilter * len(filter_values)
        c_filters = filter_type(
            *[
                _CFeatureFilter(field_strings[i], int(item.op), item.value)
                for i, item in enumerate(filter_values)
            ]
        )
        return c_filters, len(filter_values), field_strings
