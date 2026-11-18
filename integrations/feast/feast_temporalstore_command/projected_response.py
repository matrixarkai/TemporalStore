# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Build Feast response columns directly from a compact field projection."""

from typing import Any, Iterable, Sequence

from google.protobuf.timestamp_pb2 import Timestamp
from feast.protos.feast.serving.ServingService_pb2 import FieldStatus, GetOnlineFeaturesResponse
from feast.protos.feast.types.Value_pb2 import Value as ValueProto

from .client import decode_bytes


def populate_projected_response(
    rows: Sequence[Sequence[Any]], positions: Sequence[int], feature_names: Sequence[str],
    indexes: Iterable[Sequence[int]], response: Any, full_feature_names: bool,
    table: Any, output_len: int, include_feature_view_version_metadata: bool = False,
) -> None:
    projection = table.projection
    table_name = projection.name_to_use()
    clean_name = projection.name_alias or projection.name
    names = [f"{table_name}__{name}" if full_feature_names else name for name in feature_names]
    metadata = response.metadata
    versions = metadata.feature_view_metadata if include_feature_view_version_metadata else None
    add_version = versions is not None and all(item.name != clean_name for item in versions)
    version = (getattr(table, "current_version_number", 0) or 0) if add_version else 0

    prepared = []
    for row in rows:
        values = tuple(
            value if value is None or isinstance(value, bytes) else decode_bytes(value)
            for value in (row[position] if position < len(row) else None for position in positions)
        )
        timestamp = Timestamp()
        if values[-1]:
            stored = Timestamp()
            stored.ParseFromString(values[-1])
            # Keep the existing datetime validation and microsecond normalization.
            timestamp.FromDatetime(stored.ToDatetime())
        prepared.append((values, timestamp, bool(values[-1]) or any(values[:-1])))

    null_timestamp = Timestamp()
    timestamp_slots = [null_timestamp] * output_len
    value_slots = [None] * output_len
    present_rows = 0
    for row_index, destinations in enumerate(tuple(indexes)):
        values, timestamp, exists = prepared[row_index]
        for destination in destinations:
            timestamp_slots[destination] = timestamp
            if exists:
                value_slots[destination] = values
                present_rows += 1

    statuses = [FieldStatus.PRESENT if values is not None else FieldStatus.NOT_FOUND for values in value_slots]
    last_feature = {name: index for index, name in enumerate(feature_names)}
    null_values = [ValueProto()] * output_len
    missing_statuses = [FieldStatus.NOT_FOUND] * output_len
    has_values = any(values is not None for values in value_slots)
    start_vectors = len(response.results)
    start_names = len(metadata.feature_names.val)
    start_versions = len(versions) if versions is not None else 0
    had_metadata = response.HasField("metadata")
    try:
        # Amortize shared timestamp/status setup across wider projections.
        template = GetOnlineFeaturesResponse.FeatureVector(
            statuses=statuses, event_timestamps=timestamp_slots,
        ) if len(feature_names) >= 4 else None
        missing_template = None
        for feature_index, feature_name in enumerate(feature_names):
            vector = response.results.add()
            active = last_feature[feature_name] == feature_index
            if template is not None:
                if active:
                    vector.CopyFrom(template)
                else:
                    if missing_template is None:
                        missing_template = GetOnlineFeaturesResponse.FeatureVector(
                            statuses=missing_statuses, event_timestamps=timestamp_slots,
                        )
                    vector.CopyFrom(missing_template)
            if active and has_values:
                for values in value_slots:
                    value = vector.values.add()
                    if values is not None and values[feature_index]:
                        value.ParseFromString(values[feature_index])
            else:
                vector.values.extend(null_values)
            if template is None:
                vector.statuses.extend(statuses if active else missing_statuses)
                vector.event_timestamps.extend(timestamp_slots)
        metadata.feature_names.val.extend(names)
        if add_version:
            entry = versions.add()
            entry.name = clean_name
            entry.version = version
    except Exception:
        del response.results[start_vectors:]
        del metadata.feature_names.val[start_names:]
        if versions is not None:
            del versions[start_versions:]
        if not had_metadata:
            response.ClearField("metadata")
        raise

    try:
        from feast.metrics import track_feature_statuses

        present = present_rows * len(last_feature)
        track_feature_statuses(table.name, present, len(feature_names) * output_len - present)
    except Exception:
        pass
