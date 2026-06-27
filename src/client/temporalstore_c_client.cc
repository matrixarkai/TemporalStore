#include "client/temporalstore_c_client.h"

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "client/temporalstore_client.h"

struct temporalstore_client {
    std::unique_ptr<bcache2::client::TemporalStoreClient> impl;
};

namespace {

char* CopyCString(const std::string& value) {
    char* out = static_cast<char*>(std::malloc(value.size() + 1));
    if (out == nullptr) {
        return nullptr;
    }
    std::memcpy(out, value.data(), value.size());
    out[value.size()] = '\0';
    return out;
}

int Finish(const bcache2::Status& status, char** error_message) {
    if (error_message != nullptr) {
        *error_message = nullptr;
    }
    if (status.ok()) {
        return 0;
    }
    if (error_message != nullptr) {
        *error_message = CopyCString(status.ToString());
    }
    return status.errorcode();
}

bcache2::Status NullError(const char* name) {
    return bcache2::Status::InvalidArgument(std::string(name) + " is null");
}

bcache2::client::LogLevel ToLogLevel(int level) {
    switch (level) {
    case 0:
        return bcache2::client::LogLevel::kAll;
    case 1:
        return bcache2::client::LogLevel::kDebug;
    case 2:
        return bcache2::client::LogLevel::kInfo;
    case 3:
        return bcache2::client::LogLevel::kWarning;
    case 4:
        return bcache2::client::LogLevel::kError;
    case 5:
        return bcache2::client::LogLevel::kFatal;
    default:
        return bcache2::client::LogLevel::kWarning;
    }
}

bcache2::client::RiskPrecision ToRiskPrecision(temporalstore_risk_precision_t precision) {
    switch (precision) {
    case TEMPORALSTORE_RISK_ONE_SECOND:
        return bcache2::client::RiskPrecision::kOneSecond;
    case TEMPORALSTORE_RISK_FIVE_SECONDS:
        return bcache2::client::RiskPrecision::kFiveSeconds;
    case TEMPORALSTORE_RISK_TEN_SECONDS:
        return bcache2::client::RiskPrecision::kTenSeconds;
    case TEMPORALSTORE_RISK_ONE_MINUTE:
        return bcache2::client::RiskPrecision::kOneMinute;
    case TEMPORALSTORE_RISK_FIVE_MINUTES:
        return bcache2::client::RiskPrecision::kFiveMinutes;
    case TEMPORALSTORE_RISK_TEN_MINUTES:
        return bcache2::client::RiskPrecision::kTenMinutes;
    case TEMPORALSTORE_RISK_ONE_HOUR:
        return bcache2::client::RiskPrecision::kOneHour;
    case TEMPORALSTORE_RISK_ONE_DAY:
        return bcache2::client::RiskPrecision::kOneDay;
    case TEMPORALSTORE_RISK_ONE_MONTH:
        return bcache2::client::RiskPrecision::kOneMonth;
    }
    return bcache2::client::RiskPrecision::kOneMinute;
}

bcache2::client::RiskWindowUnit ToWindowUnit(temporalstore_window_unit_t unit) {
    switch (unit) {
    case TEMPORALSTORE_WINDOW_SECOND:
        return bcache2::client::RiskWindowUnit::kSecond;
    case TEMPORALSTORE_WINDOW_MINUTE:
        return bcache2::client::RiskWindowUnit::kMinute;
    case TEMPORALSTORE_WINDOW_HOUR:
        return bcache2::client::RiskWindowUnit::kHour;
    case TEMPORALSTORE_WINDOW_DAY:
        return bcache2::client::RiskWindowUnit::kDay;
    }
    return bcache2::client::RiskWindowUnit::kHour;
}

bcache2::client::TemporalFeatureFilterOp ToFeatureFilterOp(
    temporalstore_feature_filter_op_t op) {
    switch (op) {
    case TEMPORALSTORE_FEATURE_FILTER_EQUAL:
        return bcache2::client::TemporalFeatureFilterOp::kEqual;
    case TEMPORALSTORE_FEATURE_FILTER_NOT_EQUAL:
        return bcache2::client::TemporalFeatureFilterOp::kNotEqual;
    case TEMPORALSTORE_FEATURE_FILTER_GREATER_THAN:
        return bcache2::client::TemporalFeatureFilterOp::kGreaterThan;
    case TEMPORALSTORE_FEATURE_FILTER_LESS_THAN:
        return bcache2::client::TemporalFeatureFilterOp::kLessThan;
    }
    return bcache2::client::TemporalFeatureFilterOp::kEqual;
}

bcache2::Status CheckClient(temporalstore_client_t* client) {
    if (client == nullptr || client->impl == nullptr) {
        return NullError("client");
    }
    return bcache2::Status::OK();
}

void ClearHashEntryArray(temporalstore_hash_entry_array_t* array) {
    if (array == nullptr) {
        return;
    }
    if (array->entries != nullptr) {
        for (size_t i = 0; i < array->count; ++i) {
            std::free(const_cast<char*>(array->entries[i].key));
            std::free(const_cast<char*>(array->entries[i].field));
            std::free(const_cast<char*>(array->entries[i].value));
            std::free(const_cast<char*>(array->entries[i].route_json));
        }
    }
    std::free(array->entries);
    array->entries = nullptr;
    array->count = 0;
}

void ClearStringArray(temporalstore_string_array_t* array) {
    if (array == nullptr) {
        return;
    }
    if (array->values != nullptr) {
        for (size_t i = 0; i < array->count; ++i) {
            std::free(array->values[i]);
        }
    }
    std::free(array->values);
    array->values = nullptr;
    array->count = 0;
}

bcache2::Status BuildFeatureQuery(uint64_t start_ts, uint64_t end_ts, uint64_t count,
                                  const temporalstore_feature_filter_t* filters,
                                  size_t filter_count,
                                  bcache2::client::TemporalFeatureQuery* query) {
    if (query == nullptr) {
        return NullError("query");
    }
    if (filters == nullptr && filter_count != 0) {
        return NullError("filters");
    }
    query->start_ts = start_ts;
    query->end_ts = end_ts;
    query->count = count;
    query->filters.clear();
    query->filters.reserve(filter_count);
    for (size_t i = 0; i < filter_count; ++i) {
        bcache2::client::TemporalFeatureFilter filter;
        filter.field = filters[i].field ? filters[i].field : "";
        filter.op = ToFeatureFilterOp(filters[i].op);
        filter.value = filters[i].value;
        query->filters.push_back(filter);
    }
    return bcache2::Status::OK();
}

}  // namespace

extern "C" {

void temporalstore_options_init(temporalstore_options_t* options) {
    if (options == nullptr) {
        return;
    }
    std::memset(options, 0, sizeof(*options));
    options->idc = "vdc1";
    options->host = "127.0.0.1";
    options->psm = "temporalstore.c.customer.client";
    options->log_dir = "./";
    options->log_level = 3;
    options->io_timeout_ms = 1000;
    options->connect_timeout_ms = 1000;
    options->request_timeout_ms = 5000;
    options->max_read_retries = 1;
    options->max_write_retries = 0;
    options->retry_backoff_ms = 2;
    options->max_feature_points_per_request = 1000;
    options->max_feature_query_count = 5000;
    options->max_key_bytes = 4096;
    options->max_value_bytes = 16ULL * 1024ULL * 1024ULL;
    options->pin_primary = 1;
}

void temporalstore_free_string(char* value) { std::free(value); }

void temporalstore_string_array_free(temporalstore_string_array_t* array) {
    ClearStringArray(array);
}

void temporalstore_hash_entry_array_free(temporalstore_hash_entry_array_t* array) {
    ClearHashEntryArray(array);
}

void temporalstore_feature_point_array_free(temporalstore_feature_point_array_t* array) {
    if (array == nullptr) {
        return;
    }
    if (array->points != nullptr) {
        for (size_t i = 0; i < array->count; ++i) {
            std::free(const_cast<char*>(array->points[i].value));
        }
    }
    std::free(array->points);
    array->points = nullptr;
    array->count = 0;
}

void temporalstore_sequence_feature_row_array_free(
    temporalstore_sequence_feature_row_array_t* array) {
    if (array == nullptr) {
        return;
    }
    std::free(array->rows);
    array->rows = nullptr;
    array->count = 0;
}

void temporalstore_ips_feature_array_free(temporalstore_ips_feature_array_t* array) {
    if (array == nullptr) {
        return;
    }
    std::free(array->features);
    array->features = nullptr;
    array->count = 0;
}

int temporalstore_connect(const temporalstore_options_t* options, temporalstore_client_t** client,
                          char** error_message) {
    if (options == nullptr) {
        return Finish(NullError("options"), error_message);
    }
    if (client == nullptr) {
        return Finish(NullError("client"), error_message);
    }
    *client = nullptr;

    bcache2::client::TemporalStoreClientOptions cpp_options;
    cpp_options.metaserver_addr = options->metaserver_addr ? options->metaserver_addr : "";
    cpp_options.metaserver_consul =
        options->metaserver_consul ? options->metaserver_consul : "";
    cpp_options.namespace_name = options->namespace_name ? options->namespace_name : "";
    cpp_options.table_name = options->table_name ? options->table_name : "";
    if (options->idc) {
        cpp_options.idc = options->idc;
    }
    if (options->host) {
        cpp_options.host = options->host;
    }
    if (options->psm) {
        cpp_options.psm = options->psm;
    }
    if (options->log_dir) {
        cpp_options.log_dir = options->log_dir;
    }
    cpp_options.log_level = ToLogLevel(options->log_level);
    cpp_options.io_timeout_ms = options->io_timeout_ms;
    cpp_options.connect_timeout_ms = options->connect_timeout_ms;
    cpp_options.request_timeout_ms = options->request_timeout_ms;
    cpp_options.max_read_retries = options->max_read_retries;
    cpp_options.max_write_retries = options->max_write_retries;
    cpp_options.retry_backoff_ms = options->retry_backoff_ms;
    cpp_options.max_feature_points_per_request = options->max_feature_points_per_request;
    cpp_options.max_feature_query_count = options->max_feature_query_count;
    cpp_options.max_key_bytes = options->max_key_bytes;
    cpp_options.max_value_bytes = options->max_value_bytes;
    cpp_options.pin_primary = options->pin_primary != 0;

    std::unique_ptr<bcache2::client::TemporalStoreClient> cpp_client;
    bcache2::Status status =
        bcache2::client::TemporalStoreClient::Connect(cpp_options, &cpp_client);
    if (!status.ok()) {
        return Finish(status, error_message);
    }
    temporalstore_client_t* out = new temporalstore_client;
    out->impl = std::move(cpp_client);
    *client = out;
    return Finish(bcache2::Status::OK(), error_message);
}

int temporalstore_close(temporalstore_client_t* client, char** error_message) {
    if (client == nullptr) {
        return Finish(bcache2::Status::OK(), error_message);
    }
    bcache2::Status status = bcache2::Status::OK();
    if (client->impl) {
        status = client->impl->Close();
    }
    delete client;
    return Finish(status, error_message);
}

int temporalstore_put_string(temporalstore_client_t* client, const char* key, const char* value,
                             char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->PutString(key ? key : "", value ? value : "");
    }
    return Finish(status, error_message);
}

int temporalstore_put_string_with_ttl(temporalstore_client_t* client, const char* key,
                                      const char* value, uint64_t ttl_ms,
                                      char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->PutStringWithTtl(key ? key : "", value ? value : "", ttl_ms);
    }
    return Finish(status, error_message);
}

int temporalstore_get_string(temporalstore_client_t* client, const char* key, char** value,
                             char** error_message) {
    if (value == nullptr) {
        return Finish(NullError("value"), error_message);
    }
    *value = nullptr;
    bcache2::Status status = CheckClient(client);
    std::string out;
    if (status.ok()) {
        status = client->impl->GetString(key ? key : "", &out);
    }
    if (status.ok()) {
        *value = CopyCString(out);
        if (*value == nullptr) {
            status = bcache2::Status::ResourceExhausted("failed to allocate output string");
        }
    }
    return Finish(status, error_message);
}

int temporalstore_delete_object(temporalstore_client_t* client, const char* key,
                                char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->DeleteObject(key ? key : "");
    }
    return Finish(status, error_message);
}

int temporalstore_expire(temporalstore_client_t* client, const char* key, uint64_t ttl_ms,
                         char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->Expire(key ? key : "", ttl_ms);
    }
    return Finish(status, error_message);
}

int temporalstore_ttl(temporalstore_client_t* client, const char* key, uint64_t* ttl_ms,
                      char** error_message) {
    if (ttl_ms == nullptr) {
        return Finish(NullError("ttl_ms"), error_message);
    }
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->Ttl(key ? key : "", ttl_ms);
    }
    return Finish(status, error_message);
}

int temporalstore_hset(temporalstore_client_t* client, const char* key, const char* field,
                       const char* value, char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->HSet(key ? key : "", field ? field : "", value ? value : "");
    }
    return Finish(status, error_message);
}

int temporalstore_hget(temporalstore_client_t* client, const char* key, const char* field,
                       char** value, char** error_message) {
    if (value == nullptr) {
        return Finish(NullError("value"), error_message);
    }
    *value = nullptr;
    bcache2::Status status = CheckClient(client);
    std::string out;
    if (status.ok()) {
        status = client->impl->HGet(key ? key : "", field ? field : "", &out);
    }
    if (status.ok()) {
        *value = CopyCString(out);
        if (*value == nullptr) {
            status = bcache2::Status::ResourceExhausted("failed to allocate output string");
        }
    }
    return Finish(status, error_message);
}

int temporalstore_hgetall(temporalstore_client_t* client, const char* key,
                          temporalstore_hash_entry_array_t* entries,
                          char** error_message) {
    if (entries == nullptr) {
        return Finish(NullError("entries"), error_message);
    }
    ClearHashEntryArray(entries);
    bcache2::Status status = CheckClient(client);
    std::vector<bcache2::client::HashEntry> values;
    if (status.ok()) {
        status = client->impl->HGetAll(key ? key : "", &values);
    }
    if (status.ok()) {
        entries->entries = static_cast<temporalstore_hash_entry_t*>(
            std::calloc(values.size(), sizeof(temporalstore_hash_entry_t)));
        if (entries->entries == nullptr && !values.empty()) {
            status = bcache2::Status::ResourceExhausted("failed to allocate hash entry array");
        } else {
            entries->count = values.size();
            for (size_t i = 0; i < values.size(); ++i) {
                entries->entries[i].key = CopyCString(values[i].key);
                entries->entries[i].field = CopyCString(values[i].field);
                entries->entries[i].value = CopyCString(values[i].value);
                entries->entries[i].route_json = CopyCString(values[i].route_json);
                if (entries->entries[i].key == nullptr || entries->entries[i].field == nullptr ||
                    entries->entries[i].value == nullptr || entries->entries[i].route_json == nullptr) {
                    status = bcache2::Status::ResourceExhausted("failed to allocate hash entry value");
                    ClearHashEntryArray(entries);
                    break;
                }
            }
        }
    }
    return Finish(status, error_message);
}

int temporalstore_hdel(temporalstore_client_t* client, const char* key, const char* field,
                       char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->HDel(key ? key : "", field ? field : "");
    }
    return Finish(status, error_message);
}

int temporalstore_hgetall(temporalstore_client_t* client, const char* key,
                          temporalstore_string_array_t* field_values,
                          char** error_message) {
    if (field_values == nullptr) {
        return Finish(NullError("field_values"), error_message);
    }
    ClearStringArray(field_values);
    bcache2::Status status = CheckClient(client);
    std::vector<std::pair<std::string, std::string>> values;
    if (status.ok()) {
        status = client->impl->HGetAll(key ? key : "", &values);
    }
    if (status.ok()) {
        const size_t output_count = values.size() * 2;
        field_values->values = static_cast<char**>(std::calloc(output_count, sizeof(char*)));
        if (field_values->values == nullptr && output_count != 0) {
            status = bcache2::Status::ResourceExhausted("failed to allocate string array");
        } else {
            field_values->count = output_count;
            for (size_t i = 0; i < values.size(); ++i) {
                field_values->values[i * 2] = CopyCString(values[i].first);
                field_values->values[i * 2 + 1] = CopyCString(values[i].second);
                if (field_values->values[i * 2] == nullptr ||
                    field_values->values[i * 2 + 1] == nullptr) {
                    status = bcache2::Status::ResourceExhausted("failed to allocate array value");
                    ClearStringArray(field_values);
                    break;
                }
            }
        }
    }
    return Finish(status, error_message);
}

int temporalstore_sadd(temporalstore_client_t* client, const char* key, const char* member,
                       char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->SAdd(key ? key : "", member ? member : "");
    }
    return Finish(status, error_message);
}

int temporalstore_matrixark_batch_append_records(temporalstore_client_t* client,
                                                 const temporalstore_hash_entry_t* entries,
                                                 size_t entry_count, const char* count_key,
                                                 const char* count_value,
                                                 char** error_message) {
    return temporalstore_matrixark_batch_append_records_v2(
        client, entries, entry_count, count_key, count_value, nullptr, error_message);
}

int temporalstore_matrixark_batch_append_records_v2(temporalstore_client_t* client,
                                                    const temporalstore_hash_entry_t* entries,
                                                    size_t entry_count, const char* count_key,
                                                    const char* count_value,
                                                    const char* append_options_json,
                                                    char** error_message) {
    if (entries == nullptr && entry_count != 0) {
        return Finish(NullError("entries"), error_message);
    }
    bcache2::Status status = CheckClient(client);
    std::vector<bcache2::client::MatrixArkHashRecord> records;
    if (status.ok()) {
        std::vector<bcache2::client::HashEntry> batch;
        batch.reserve(entry_count);
        for (size_t i = 0; i < entry_count; ++i) {
            const temporalstore_hash_entry_t& entry = entries[i];
            batch.push_back(bcache2::client::HashEntry{
                entry.key ? entry.key : "",
                entry.field ? entry.field : "",
                entry.value ? entry.value : "",
                entry.route_json ? entry.route_json : "",
            });
        }
        bcache2::client::MatrixArkBatchAppendOptions options;
        options.append_options_json = append_options_json ? append_options_json : "";
        status = client->impl->MatrixArkBatchAppendRecords(
            batch,
            (count_key != nullptr && count_key[0] != '\0') ? count_key : "",
            count_value != nullptr ? count_value : "",
            options);
    }
    return Finish(status, error_message);
}

int temporalstore_matrixark_retrieve_context_pack(temporalstore_client_t* client,
                                                  const char* request_json,
                                                  char** response_json,
                                                  char** error_message) {
    if (response_json == nullptr) {
        return Finish(NullError("response_json"), error_message);
    }
    *response_json = nullptr;
    bcache2::Status status = CheckClient(client);
    std::string out;
    if (status.ok()) {
        status = client->impl->MatrixArkRetrieveContextPack(
            request_json ? request_json : "", &out);
    }
    if (status.ok()) {
        *response_json = CopyCString(out);
        if (*response_json == nullptr) {
            status = bcache2::Status::ResourceExhausted("failed to allocate response_json");
        }
    }
    return Finish(status, error_message);
}

int temporalstore_smembers(temporalstore_client_t* client, const char* key,
                           temporalstore_string_array_t* members, char** error_message) {
    if (members == nullptr) {
        return Finish(NullError("members"), error_message);
    }
    ClearStringArray(members);
    bcache2::Status status = CheckClient(client);
    std::vector<std::string> values;
    if (status.ok()) {
        status = client->impl->SMembers(key ? key : "", &values);
    }
    if (status.ok()) {
        members->values = static_cast<char**>(std::calloc(values.size(), sizeof(char*)));
        if (members->values == nullptr && !values.empty()) {
            status = bcache2::Status::ResourceExhausted("failed to allocate string array");
        } else {
            members->count = values.size();
            for (size_t i = 0; i < values.size(); ++i) {
                members->values[i] = CopyCString(values[i]);
                if (members->values[i] == nullptr) {
                    status = bcache2::Status::ResourceExhausted("failed to allocate array value");
                    ClearStringArray(members);
                    break;
                }
            }
        }
    }
    return Finish(status, error_message);
}

int temporalstore_add_feature_points(temporalstore_client_t* client, const char* key,
                                     const temporalstore_feature_point_t* points, size_t count,
                                     char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok() && points == nullptr && count != 0) {
        status = NullError("points");
    }
    if (status.ok()) {
        std::vector<bcache2::client::TemporalFeaturePoint> cpp_points;
        cpp_points.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            cpp_points.push_back(
                bcache2::client::TemporalFeaturePoint{points[i].timestamp,
                                                       points[i].value ? points[i].value : ""});
        }
        status = client->impl->AddFeaturePoints(key ? key : "", cpp_points);
    }
    return Finish(status, error_message);
}

int temporalstore_query_feature_points(temporalstore_client_t* client, const char* key,
                                       uint64_t start_ts, uint64_t end_ts, uint64_t count,
                                       temporalstore_feature_point_array_t* points,
                                       char** error_message) {
    return temporalstore_query_feature_points_with_filters(client, key, start_ts, end_ts, count,
                                                           nullptr, 0, points, error_message);
}

int temporalstore_query_feature_points_with_filters(
    temporalstore_client_t* client, const char* key, uint64_t start_ts, uint64_t end_ts,
    uint64_t count, const temporalstore_feature_filter_t* filters, size_t filter_count,
    temporalstore_feature_point_array_t* points, char** error_message) {
    if (points == nullptr) {
        return Finish(NullError("points"), error_message);
    }
    temporalstore_feature_point_array_free(points);
    bcache2::Status status = CheckClient(client);
    std::vector<bcache2::client::TemporalFeaturePoint> cpp_points;
    if (status.ok()) {
        bcache2::client::TemporalFeatureQuery query;
        status = BuildFeatureQuery(start_ts, end_ts, count, filters, filter_count, &query);
        if (status.ok()) {
            status = client->impl->QueryFeaturePoints(key ? key : "", query, &cpp_points);
        }
    }
    if (status.ok()) {
        points->points = static_cast<temporalstore_feature_point_t*>(
            std::calloc(cpp_points.size(), sizeof(temporalstore_feature_point_t)));
        if (points->points == nullptr && !cpp_points.empty()) {
            status = bcache2::Status::ResourceExhausted("failed to allocate feature points");
        } else {
            points->count = cpp_points.size();
            for (size_t i = 0; i < cpp_points.size(); ++i) {
                points->points[i].timestamp = cpp_points[i].timestamp;
                points->points[i].value = CopyCString(cpp_points[i].value);
                if (points->points[i].value == nullptr) {
                    status = bcache2::Status::ResourceExhausted("failed to allocate point value");
                    temporalstore_feature_point_array_free(points);
                    break;
                }
            }
        }
    }
    return Finish(status, error_message);
}

int temporalstore_add_sequence_feature_rows(temporalstore_client_t* client, const char* key,
                                            const temporalstore_sequence_feature_row_t* rows,
                                            size_t count, char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok() && rows == nullptr && count != 0) {
        status = NullError("rows");
    }
    if (status.ok()) {
        std::vector<bcache2::client::SequenceFeatureRow> cpp_rows;
        cpp_rows.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            bcache2::client::SequenceFeatureRow row;
            row.timestamp = rows[i].timestamp;
            row.gid = rows[i].gid;
            row.action_type = rows[i].action_type;
            row.duration = rows[i].duration;
            row.author_id = rows[i].author_id;
            cpp_rows.push_back(row);
        }
        status = client->impl->AddSequenceFeatureRows(key ? key : "", cpp_rows);
    }
    return Finish(status, error_message);
}

int temporalstore_query_sequence_feature_rows(
    temporalstore_client_t* client, const char* key, uint64_t start_ts, uint64_t end_ts,
    uint64_t count, const temporalstore_feature_filter_t* filters, size_t filter_count,
    temporalstore_sequence_feature_row_array_t* rows, char** error_message) {
    if (rows == nullptr) {
        return Finish(NullError("rows"), error_message);
    }
    temporalstore_sequence_feature_row_array_free(rows);
    bcache2::Status status = CheckClient(client);
    std::vector<bcache2::client::SequenceFeatureRow> cpp_rows;
    if (status.ok()) {
        bcache2::client::TemporalFeatureQuery query;
        status = BuildFeatureQuery(start_ts, end_ts, count, filters, filter_count, &query);
        if (status.ok()) {
            status = client->impl->QuerySequenceFeatureRows(key ? key : "", query, &cpp_rows);
        }
    }
    if (status.ok()) {
        rows->rows = static_cast<temporalstore_sequence_feature_row_t*>(
            std::calloc(cpp_rows.size(), sizeof(temporalstore_sequence_feature_row_t)));
        if (rows->rows == nullptr && !cpp_rows.empty()) {
            status = bcache2::Status::ResourceExhausted("failed to allocate sequence rows");
        } else {
            rows->count = cpp_rows.size();
            for (size_t i = 0; i < cpp_rows.size(); ++i) {
                rows->rows[i].timestamp = cpp_rows[i].timestamp;
                rows->rows[i].gid = cpp_rows[i].gid;
                rows->rows[i].action_type = cpp_rows[i].action_type;
                rows->rows[i].duration = cpp_rows[i].duration;
                rows->rows[i].author_id = cpp_rows[i].author_id;
            }
        }
    }
    return Finish(status, error_message);
}

int temporalstore_add_ips_instance(temporalstore_client_t* client, const char* table, int64_t uid,
                                   int64_t timestamp_us, int32_t action_type,
                                   int32_t logical_table,
                                   const temporalstore_ips_feature_stat_t* features,
                                   size_t feature_count, char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok() && features == nullptr && feature_count != 0) {
        status = NullError("features");
    }
    if (status.ok()) {
        bcache2::client::IpsInstance instance;
        if (table != nullptr && table[0] != '\0') {
            instance.table = table;
        }
        instance.uid = uid;
        instance.timestamp_us = timestamp_us;
        instance.action_type = action_type;
        instance.logical_table = logical_table;
        instance.features.reserve(feature_count);
        for (size_t i = 0; i < feature_count; ++i) {
            bcache2::client::IpsFeatureStat feature;
            feature.id = features[i].id;
            feature.slot = features[i].slot;
            feature.has_slot = features[i].has_slot != 0;
            feature.type = features[i].type;
            feature.v1 = features[i].v1;
            feature.v2 = features[i].v2;
            instance.features.push_back(feature);
        }
        status = client->impl->AddIpsInstance(instance);
    }
    return Finish(status, error_message);
}

int temporalstore_query_ips_last_instances(temporalstore_client_t* client, const char* table,
                                           int64_t uid, int32_t action_type,
                                           int32_t logical_table, int32_t slot, int32_t top_k,
                                           int64_t last_instances,
                                           temporalstore_ips_feature_array_t* features,
                                           char** error_message) {
    if (features == nullptr) {
        return Finish(NullError("features"), error_message);
    }
    temporalstore_ips_feature_array_free(features);
    bcache2::Status status = CheckClient(client);
    std::vector<bcache2::client::IpsFeatureStat> cpp_features;
    if (status.ok()) {
        bcache2::client::IpsLastQuery query;
        if (table != nullptr && table[0] != '\0') {
            query.table = table;
        }
        query.uid = uid;
        query.action_type = action_type;
        query.logical_table = logical_table;
        query.slot = slot;
        query.top_k = top_k;
        query.last_instances = last_instances;
        status = client->impl->QueryIpsLastInstances(query, &cpp_features);
    }
    if (status.ok()) {
        features->features = static_cast<temporalstore_ips_feature_stat_t*>(
            std::calloc(cpp_features.size(), sizeof(temporalstore_ips_feature_stat_t)));
        if (features->features == nullptr && !cpp_features.empty()) {
            status = bcache2::Status::ResourceExhausted("failed to allocate IPS features");
        } else {
            features->count = cpp_features.size();
            for (size_t i = 0; i < cpp_features.size(); ++i) {
                features->features[i].id = cpp_features[i].id;
                features->features[i].slot = cpp_features[i].slot;
                features->features[i].has_slot = cpp_features[i].has_slot ? 1 : 0;
                features->features[i].type = cpp_features[i].type;
                features->features[i].v1 = cpp_features[i].v1;
                features->features[i].v2 = cpp_features[i].v2;
            }
        }
    }
    return Finish(status, error_message);
}

int temporalstore_risk_increment(temporalstore_client_t* client, const char* key, int64_t amount,
                                 uint64_t ttl_seconds,
                                 temporalstore_risk_precision_t precision, const char* uuid,
                                 uint64_t occur_time_seconds, char** error_message) {
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        status = client->impl->RiskIncrement(key ? key : "", amount, ttl_seconds,
                                             ToRiskPrecision(precision), uuid ? uuid : "",
                                             occur_time_seconds);
    }
    return Finish(status, error_message);
}

int temporalstore_risk_count(temporalstore_client_t* client, const char* key,
                             temporalstore_risk_precision_t precision, int64_t window_start,
                             int64_t window_end, temporalstore_window_unit_t window_unit,
                             int64_t* count, char** error_message) {
    if (count == nullptr) {
        return Finish(NullError("count"), error_message);
    }
    bcache2::Status status = CheckClient(client);
    if (status.ok()) {
        bcache2::client::RiskWindow window;
        window.start = window_start;
        window.end = window_end;
        window.unit = ToWindowUnit(window_unit);
        status = client->impl->RiskCount(key ? key : "", ToRiskPrecision(precision), window,
                                         count);
    }
    return Finish(status, error_message);
}

}  // extern "C"
