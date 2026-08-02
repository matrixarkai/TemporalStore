#include <absl/strings/numbers.h>

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/cmd_manager.h"
#include "common/macros.h"
#include "common/time.h"
#include "extension/modules.pb.h"
#include "extension/temporal_aggregate/interface.pb.h"
#include "model/hash_model.h"
#include "partition/compute/execute_env.h"

#include <google/protobuf/repeated_field.h>

namespace bcache2 {
namespace temporal_aggregate {
namespace {

constexpr char kFieldSep = '\x1f';
constexpr int kBucketIdWidth = 20;

struct EncodedDimension {
    std::string name;
    std::string value;
};

Status ParseInt64(const std::string& text, int64_t* value) {
    char* end = nullptr;
    errno = 0;
    const int64_t parsed = std::strtoll(text.c_str(), &end, 10);
    if (errno == ERANGE || end == text.c_str() || *end != '\0') {
        return Status::InvalidArgument("stored aggregate value is not int64");
    }
    *value = parsed;
    return Status::OK();
}

std::string EncodePart(const std::string& input) {
    return std::to_string(input.size()) + ":" + input;
}

std::string EncodeBucketId(uint64_t bucket_id) {
    std::ostringstream oss;
    oss << std::setw(kBucketIdWidth) << std::setfill('0') << bucket_id;
    return oss.str();
}

std::string EncodeDimensionKey(const google::protobuf::RepeatedPtrField<Dimension>& dimensions) {
    std::vector<EncodedDimension> ordered;
    ordered.reserve(dimensions.size());
    for (const auto& dimension : dimensions) {
        ordered.push_back({dimension.name(), dimension.value()});
    }
    std::sort(ordered.begin(), ordered.end(), [](const EncodedDimension& left,
                                                 const EncodedDimension& right) {
        if (left.name != right.name) {
            return left.name < right.name;
        }
        return left.value < right.value;
    });

    std::string encoded;
    for (const auto& dimension : ordered) {
        if (!encoded.empty()) {
            encoded.push_back('&');
        }
        encoded.append(EncodePart(dimension.name));
        encoded.push_back('=');
        encoded.append(EncodePart(dimension.value));
    }
    return encoded;
}

std::string FieldPrefix(const std::string& metric,
                        const google::protobuf::RepeatedPtrField<Dimension>& dimensions,
                        uint64_t bucket_width_ms) {
    std::string prefix;
    prefix.append(EncodePart(metric));
    prefix.push_back(kFieldSep);
    prefix.append(std::to_string(bucket_width_ms));
    prefix.push_back(kFieldSep);
    prefix.append(EncodeDimensionKey(dimensions));
    prefix.push_back(kFieldSep);
    return prefix;
}

uint64_t BucketId(uint64_t timestamp_ms, uint64_t bucket_width_ms) {
    return timestamp_ms / bucket_width_ms;
}

Status ValidateKeyAndMetric(const std::string& key, const std::string& metric,
                            uint64_t bucket_width_ms) {
    if (key.empty()) {
        return Status::InvalidArgument("key is empty");
    }
    if (metric.empty()) {
        return Status::InvalidArgument("metric is empty");
    }
    if (bucket_width_ms == 0) {
        return Status::InvalidArgument("bucket_width_ms must be positive");
    }
    return Status::OK();
}

Status MergeValue(AggregateOp op, bool has_old_value, int64_t old_value, int64_t delta,
                  int64_t* merged) {
    switch (op) {
    case COUNT:
    case SUM:
        if ((delta > 0 && old_value > LLONG_MAX - delta) ||
            (delta < 0 && old_value < LLONG_MIN - delta)) {
            return Status::OutOfRange("aggregate value overflow");
        }
        *merged = old_value + delta;
        return Status::OK();
    case MIN:
        *merged = has_old_value ? std::min(old_value, delta) : delta;
        return Status::OK();
    case MAX:
        *merged = has_old_value ? std::max(old_value, delta) : delta;
        return Status::OK();
    default:
        return Status::InvalidArgument("unknown aggregate op");
    }
}

Status FoldQueryValue(AggregateOp op, int64_t bucket_value, bool* has_value, int64_t* value) {
    if (!*has_value) {
        *value = bucket_value;
        *has_value = true;
        return Status::OK();
    }
    return MergeValue(op, true, *value, bucket_value, value);
}

}  // namespace

Status Incr(ExecuteEnv* env, const IncrRequest& request, IncrResponse* response) {
    Status status = ValidateKeyAndMetric(request.key(), request.metric(), request.bucket_width_ms());
    if (!status.ok()) {
        return status;
    }

    ObjectHandle<model::HashModel> object;
    status = env->GetOrNewObject(request.key(), &object);
    if (!status.ok()) {
        return status;
    }
    if (request.ttl_ms() > 0) {
        object.SetTtl(GetCurrentTimeInMs() + request.ttl_ms());
    }

    const uint64_t bucket_id = BucketId(request.timestamp_ms(), request.bucket_width_ms());
    const std::string field =
        FieldPrefix(request.metric(), request.dimensions(), request.bucket_width_ms()) +
        EncodeBucketId(bucket_id);

    std::string old_text;
    int64_t old_value = 0;
    bool has_old_value = false;
    status = object->OrSet().Get(field, &old_text);
    if (status.ok()) {
        has_old_value = true;
        status = ParseInt64(old_text, &old_value);
        if (!status.ok()) {
            return status;
        }
    } else if (!status.IsNotFound()) {
        return status;
    }

    const int64_t delta = (request.op() == COUNT && request.value() == 0) ? 1 : request.value();
    int64_t merged = 0;
    status = MergeValue(request.op(), has_old_value, old_value, delta, &merged);
    if (!status.ok()) {
        return status;
    }

    status = object->OrSet().Set(nullptr, field, std::to_string(merged));
    if (!status.ok()) {
        return status;
    }
    response->set_bucket_value(merged);
    response->set_bucket_id(bucket_id);
    return Status::OK();
}
REGISTER_FUNCTION(TEMPORAL_AGGREGATE, INCR, Incr, Write);

Status Query(ExecuteEnv* env, const QueryRequest& request, QueryResponse* response) {
    Status status = ValidateKeyAndMetric(request.key(), request.metric(), request.bucket_width_ms());
    if (!status.ok()) {
        return status;
    }
    if (request.end_timestamp_ms() <= request.start_timestamp_ms()) {
        return Status::InvalidArgument("end_timestamp_ms must be greater than start_timestamp_ms");
    }

    ObjectHandle<model::HashModel> object;
    status = env->GetObject(request.key(), &object);
    if (status.IsNotFound()) {
        response->set_has_value(false);
        response->set_value(0);
        return Status::OK();
    }
    if (!status.ok()) {
        return status;
    }

    const uint64_t start_bucket = BucketId(request.start_timestamp_ms(), request.bucket_width_ms());
    const uint64_t end_bucket =
        BucketId(request.end_timestamp_ms() - 1, request.bucket_width_ms()) + 1;
    const std::string prefix =
        FieldPrefix(request.metric(), request.dimensions(), request.bucket_width_ms());
    const std::string start_field = prefix + EncodeBucketId(start_bucket);
    const std::string end_field = prefix + EncodeBucketId(end_bucket);

    bool has_value = false;
    int64_t value = 0;
    Status scan_status = Status::OK();
    object->OrSet().ScanRange(start_field, end_field, [&](const std::string& field,
                                                          const std::string& field_value) {
        int64_t bucket_value = 0;
        scan_status = ParseInt64(field_value, &bucket_value);
        if (!scan_status.ok()) {
            return false;
        }
        scan_status = FoldQueryValue(request.op(), bucket_value, &has_value, &value);
        if (!scan_status.ok()) {
            return false;
        }

        const std::string encoded_bucket = field.substr(prefix.size());
        uint64_t bucket_id = 0;
        if (!absl::SimpleAtoi(encoded_bucket, &bucket_id)) {
            scan_status = Status::InvalidArgument("stored bucket id is invalid");
            return false;
        }
        BucketValue* bucket = response->add_buckets();
        bucket->set_bucket_id(bucket_id);
        bucket->set_bucket_start_ms(bucket_id * request.bucket_width_ms());
        bucket->set_value(bucket_value);
        return true;
    });
    if (!scan_status.ok()) {
        return scan_status;
    }

    response->set_has_value(has_value);
    response->set_value(has_value ? value : 0);
    return Status::OK();
}
REGISTER_FUNCTION(TEMPORAL_AGGREGATE, QUERY, Query, Read);

}  // namespace temporal_aggregate
}  // namespace bcache2
