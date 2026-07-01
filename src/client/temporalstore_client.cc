#include "client/temporalstore_client.h"

#include <algorithm>
#include <chrono>
#include <ctime>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <utility>

#include "client/client_impl.h"
#include "common/cmd_manager.h"
#include "common/controller.h"
#include "common/sync_closure.h"
#include "extension/feature/interface.pb.h"
#include "extension/hash/interface.pb.h"
#include "extension/ips/interface.pb.h"
#include "extension/modules.pb.h"
#include "extension/risk/interface.pb.h"
#include "extension/set/interface.pb.h"

namespace bcache2 {
namespace client {
namespace {

Status ValidateNotEmpty(const std::string& value, const char* name) {
    if (value.empty()) {
        return Status::InvalidArgument(std::string(name) + " is empty");
    }
    return Status::OK();
}

Status ValidateSize(uint64_t size, uint64_t max_size, const char* name) {
    if (max_size > 0 && size > max_size) {
        return Status::InvalidArgument(std::string(name) + " is too large");
    }
    return Status::OK();
}

Status ValidateOutput(const void* output, const char* name) {
    if (output == nullptr) {
        return Status::InvalidArgument(std::string(name) + " is null");
    }
    return Status::OK();
}

bool IsRetryable(const Status& status) {
    return status.IsDeadlineExceeded() || status.IsUnavailable() || status.IsInternal() ||
           status.IsRetryLater() || status.IsPartitionLoading() || status.IsMetaChanged() ||
           status.IsTopomError();
}

const char* ToFilterOpString(TemporalFeatureFilterOp op) {
    switch (op) {
    case TemporalFeatureFilterOp::kEqual:
        return "=";
    case TemporalFeatureFilterOp::kNotEqual:
        return "!=";
    case TemporalFeatureFilterOp::kGreaterThan:
        return ">";
    case TemporalFeatureFilterOp::kLessThan:
        return "<";
    }
    return "=";
}

std::string BuildFilterExpression(const TemporalFeatureFilter& filter) {
    std::ostringstream os;
    os << filter.field << " " << ToFilterOpString(filter.op) << " " << filter.value;
    return os.str();
}

Status ValidateFeatureFilter(const TemporalFeatureFilter& filter) {
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(filter.field, "filter.field"));
    if (filter.field.find_first_of(" \t\r\n") != std::string::npos) {
        return Status::InvalidArgument("filter.field cannot contain whitespace");
    }
    return Status::OK();
}

Status ValidateFeatureQuery(const TemporalStoreClientOptions& options,
                            const TemporalFeatureQuery& query) {
    if (query.start_ts >= query.end_ts) {
        return Status::InvalidArgument("query.start_ts must be smaller than query.end_ts");
    }
    if (query.count == 0) {
        return Status::InvalidArgument("query.count must be positive");
    }
    RETURN_IF_STATUS_ERROR(
        ValidateSize(query.count, options.max_feature_query_count, "query.count"));
    for (const auto& filter : query.filters) {
        RETURN_IF_STATUS_ERROR(ValidateFeatureFilter(filter));
    }
    return Status::OK();
}

feature2::WritePolicy ToProto(TemporalFeatureWritePolicy policy) {
    switch (policy) {
    case TemporalFeatureWritePolicy::kUpsert:
        return feature2::UPSERT;
    case TemporalFeatureWritePolicy::kBlock:
        return feature2::BLOCK;
    case TemporalFeatureWritePolicy::kFirst:
        return feature2::FIRST;
    case TemporalFeatureWritePolicy::kUpdate:
        return feature2::UPDATE;
    }
    return feature2::UPSERT;
}

std::string SerializeSequenceFeatureRow(const SequenceFeatureRow& row) {
    feature2::FeaturePoint point;
    point.set_gid(row.gid);
    point.set_action_type(row.action_type);
    point.set_duration(row.duration);
    point.set_author_id(row.author_id);
    return point.SerializeAsString();
}

Status ParseSequenceFeatureRow(const TemporalFeaturePoint& raw, SequenceFeatureRow* row) {
    RETURN_IF_STATUS_ERROR(ValidateOutput(row, "row"));
    feature2::FeaturePoint point;
    if (!point.ParseFromString(raw.value)) {
        return Status::Internal("sequence feature row protobuf parse failed");
    }
    row->timestamp = raw.timestamp;
    row->gid = point.gid();
    row->action_type = point.action_type();
    row->duration = point.duration();
    row->author_id = point.author_id();
    return Status::OK();
}

risk::RiskPrecision ToProto(RiskPrecision precision) {
    switch (precision) {
    case RiskPrecision::kOneSecond:
        return risk::OneSecond;
    case RiskPrecision::kFiveSeconds:
        return risk::FiveSeconds;
    case RiskPrecision::kTenSeconds:
        return risk::TenSeconds;
    case RiskPrecision::kOneMinute:
        return risk::OneMinute;
    case RiskPrecision::kFiveMinutes:
        return risk::FiveMinutes;
    case RiskPrecision::kTenMinutes:
        return risk::TenMinutes;
    case RiskPrecision::kOneHour:
        return risk::OneHour;
    case RiskPrecision::kOneDay:
        return risk::OneDay;
    case RiskPrecision::kOneMonth:
        return risk::OneMonth;
    }
    return risk::OneMinute;
}

risk::WindowUnit ToProto(RiskWindowUnit unit) {
    switch (unit) {
    case RiskWindowUnit::kSecond:
        return risk::Second;
    case RiskWindowUnit::kMinute:
        return risk::Minute;
    case RiskWindowUnit::kHour:
        return risk::Hour;
    case RiskWindowUnit::kDay:
        return risk::Day;
    }
    return risk::Hour;
}

}  // namespace

struct TemporalStoreClient::Impl {
    TemporalStoreClientOptions options;
    std::unique_ptr<Client> client;
    std::unique_ptr<Table> table;
    TableCore* table_core = nullptr;
    bool closed = true;
    mutable std::shared_mutex mutex;

    template <typename Fn>
    Status WithRetry(bool write, Fn fn) {
        std::shared_lock<std::shared_mutex> lock(mutex);
        if (closed || table == nullptr || table_core == nullptr) {
            return Status::FailedPrecondition("client is closed");
        }
        const int retries = write ? options.max_write_retries : options.max_read_retries;
        Status last = Status::OK();
        for (int attempt = 0; attempt <= retries; ++attempt) {
            last = fn();
            if (last.ok() || !IsRetryable(last) || attempt == retries) {
                return last;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(
                std::max(0, options.retry_backoff_ms) * (attempt + 1)));
        }
        return last;
    }

    template <typename Request, typename Response>
    Status ExecuteRaw(uint16_t module_id, uint16_t function_id, const std::string& partition_key,
                      const Request& request, Response* response, bool write) {
        Status output_status = ValidateOutput(response, "response");
        if (!output_status.ok()) {
            return output_status;
        }
        return WithRetry(write, [&]() {
            TableCore::Request raw_request;
            TableCore::Response raw_response;
            Controller ctrl;
            CoSyncClosure sync;

            raw_request.cmd_id = MakeCmdId(module_id, function_id);
            raw_request.key = partition_key;
            raw_request.input.set_module_id(module_id);
            raw_request.input.set_function_id(function_id);

            std::string request_bytes;
            if (!request.SerializeToString(&request_bytes)) {
                return Status::Internal("request serialization failed");
            }
            raw_request.input.set_request_bytes(std::move(request_bytes));

            ctrl.set_timeout_ms(options.request_timeout_ms);
            table_core->Execute(&ctrl, &raw_request, &raw_response, &sync, nullptr,
                                RequestOptions());
            sync.Wait();
            if (!ctrl.status().ok()) {
                return ctrl.status();
            }
            if (!response->ParseFromString(raw_response.output->response_bytes())) {
                return Status::Internal("response parse failed");
            }
            return Status::OK();
        });
    }
};

TemporalStoreClient::TemporalStoreClient(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

TemporalStoreClient::~TemporalStoreClient() {
    if (impl_) {
        (void)Close();
    }
}

TemporalStoreClient::TemporalStoreClient(TemporalStoreClient&&) noexcept = default;

TemporalStoreClient& TemporalStoreClient::operator=(TemporalStoreClient&&) noexcept = default;

Status TemporalStoreClient::CheckInitialized() const {
    if (!impl_) {
        return Status::FailedPrecondition("client is moved-from or not initialized");
    }
    return Status::OK();
}

Status TemporalStoreClient::Connect(const TemporalStoreClientOptions& options,
                                    std::unique_ptr<TemporalStoreClient>* out) {
    RETURN_IF_STATUS_ERROR(ValidateOutput(out, "client"));
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(options.namespace_name, "namespace_name"));
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(options.table_name, "table_name"));
    if (options.metaserver_addr.empty() && options.metaserver_consul.empty()) {
        return Status::InvalidArgument("metaserver_addr or metaserver_consul is required");
    }
    if (options.io_timeout_ms <= 0 || options.connect_timeout_ms <= 0 ||
        options.request_timeout_ms == 0) {
        return Status::InvalidArgument("timeouts must be positive");
    }
    if (options.max_read_retries < 0 || options.max_write_retries < 0 ||
        options.retry_backoff_ms < 0 || options.max_feature_points_per_request < 0) {
        return Status::InvalidArgument("retry and batching options cannot be negative");
    }

    ClientOptions client_options;
    client_options.master_addr = options.metaserver_addr;
    client_options.master_consul = options.metaserver_consul;
    client_options.idc = options.idc;
    client_options.host = options.host;
    client_options.psm = options.psm;
    client_options.log_dir = options.log_dir;
    client_options.log_level = options.log_level;
    client_options.af = options.address_family;
    client_options.meta_sync_interval_ms = options.meta_sync_interval_ms;
    client_options.topo_error_retry_interval_ms = options.topo_error_retry_interval_ms;
    client_options.meta_fetch_timeout_ms = options.meta_fetch_timeout_ms;
    if (options.pin_primary) {
        client_options.partition_pick_opts.policy = PartitionPickOptions::Policy::kPrimary;
    }

    Client* raw_client = nullptr;
    RETURN_IF_STATUS_ERROR(Client::Create(client_options, &raw_client));
    std::unique_ptr<Client> client(raw_client);

    TableOptions table_options;
    table_options.io_timeout_ms = options.io_timeout_ms;
    table_options.connect_timeout_ms = options.connect_timeout_ms;
    table_options.continuous_failed_time_ms = options.continuous_failed_time_ms;

    Table* raw_table = nullptr;
    Status status =
        client->OpenTable(options.namespace_name, options.table_name, table_options, &raw_table);
    if (!status.ok()) {
        return status;
    }
    std::unique_ptr<Table> table(raw_table);
    TableCore* table_core = dynamic_cast<TableCore*>(raw_table);
    if (table_core == nullptr) {
        return Status::Internal("opened table is not TableCore");
    }

    std::unique_ptr<Impl> impl(new Impl);
    impl->options = options;
    impl->client = std::move(client);
    impl->table = std::move(table);
    impl->table_core = table_core;
    impl->closed = false;

    out->reset(new TemporalStoreClient(std::move(impl)));
    return Status::OK();
}

Status TemporalStoreClient::Close() {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    if (!impl_ || impl_->closed) {
        return Status::OK();
    }
    std::unique_lock<std::shared_mutex> lock(impl_->mutex);
    if (impl_->closed) {
        return Status::OK();
    }
    Status status = impl_->client->CloseTable(impl_->table.get());
    impl_->table.reset();
    impl_->table_core = nullptr;
    impl_->closed = true;
    return status;
}

Status TemporalStoreClient::PutString(const std::string& key, const std::string& value) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(key, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(key.size(), impl_->options.max_key_bytes, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(value.size(), impl_->options.max_value_bytes, "value"));
    return impl_->WithRetry(true, [&]() { return impl_->table->Set(key, value); });
}

Status TemporalStoreClient::PutStringWithTtl(const std::string& key, const std::string& value,
                                             uint64_t ttl_ms) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(key, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(key.size(), impl_->options.max_key_bytes, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(value.size(), impl_->options.max_value_bytes, "value"));
    return impl_->WithRetry(true, [&]() { return impl_->table->SetEx(key, value, ttl_ms); });
}

Status TemporalStoreClient::GetString(const std::string& key, std::string* value) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(key, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(key.size(), impl_->options.max_key_bytes, "key"));
    RETURN_IF_STATUS_ERROR(ValidateOutput(value, "value"));
    return impl_->WithRetry(false, [&]() { return impl_->table->Get(key, value); });
}

Status TemporalStoreClient::DeleteObject(const std::string& key) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(key, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(key.size(), impl_->options.max_key_bytes, "key"));
    return impl_->WithRetry(true, [&]() { return impl_->table->Del(key); });
}

Status TemporalStoreClient::Expire(const std::string& key, uint64_t ttl_ms) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(key, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(key.size(), impl_->options.max_key_bytes, "key"));
    return impl_->WithRetry(true, [&]() { return impl_->table->Expire(key, ttl_ms); });
}

Status TemporalStoreClient::Ttl(const std::string& key, uint64_t* ttl_ms) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(key, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(key.size(), impl_->options.max_key_bytes, "key"));
    RETURN_IF_STATUS_ERROR(ValidateOutput(ttl_ms, "ttl_ms"));
    return impl_->WithRetry(false, [&]() { return impl_->table->Ttl(key, ttl_ms); });
}

Status TemporalStoreClient::HSet(const std::string& key, const std::string& field,
                                 const std::string& value) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(key, "key"));
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(field, "field"));
    RETURN_IF_STATUS_ERROR(ValidateSize(key.size(), impl_->options.max_key_bytes, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(field.size(), impl_->options.max_key_bytes, "field"));
    RETURN_IF_STATUS_ERROR(ValidateSize(value.size(), impl_->options.max_value_bytes, "value"));
    ::bcache2::hash2::SetRequest request;
    request.set_key(key);
    request.set_field(field);
    request.set_value(value);
    ::bcache2::hash2::SetResponse response;
    return impl_->ExecuteRaw(Module::HASH, ::bcache2::hash2::SET, key, request, &response, true);
}

Status TemporalStoreClient::HGet(const std::string& key, const std::string& field,
                                 std::string* value) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(key, "key"));
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(field, "field"));
    RETURN_IF_STATUS_ERROR(ValidateSize(key.size(), impl_->options.max_key_bytes, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(field.size(), impl_->options.max_key_bytes, "field"));
    RETURN_IF_STATUS_ERROR(ValidateOutput(value, "value"));
    ::bcache2::hash2::GetRequest request;
    request.set_key(key);
    request.set_field(field);
    ::bcache2::hash2::GetResponse response;
    RETURN_IF_STATUS_ERROR(
        impl_->ExecuteRaw(Module::HASH, ::bcache2::hash2::GET, key, request, &response, false));
    if (!response.exist()) {
        return Status::NotFound("hash field not found");
    }
    *value = response.value();
    return Status::OK();
}

Status TemporalStoreClient::HDel(const std::string& key, const std::string& field) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(key, "key"));
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(field, "field"));
    RETURN_IF_STATUS_ERROR(ValidateSize(key.size(), impl_->options.max_key_bytes, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(field.size(), impl_->options.max_key_bytes, "field"));
    ::bcache2::hash2::DelRequest request;
    request.set_key(key);
    request.set_field(field);
    ::bcache2::hash2::DelResponse response;
    return impl_->ExecuteRaw(Module::HASH, ::bcache2::hash2::DEL, key, request, &response, true);
}

Status TemporalStoreClient::MatrixArkBatchAppendRecords(const std::vector<HashEntry>& entries,
                                                        const std::string& count_key,
                                                        const std::string& count_value) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    if (entries.empty() && count_key.empty()) {
        return Status::InvalidArgument("entries is empty");
    }
    for (const auto& entry : entries) {
        RETURN_IF_STATUS_ERROR(ValidateNotEmpty(entry.key, "entry.key"));
        RETURN_IF_STATUS_ERROR(ValidateNotEmpty(entry.field, "entry.field"));
        RETURN_IF_STATUS_ERROR(ValidateSize(entry.key.size(), impl_->options.max_key_bytes, "entry.key"));
        RETURN_IF_STATUS_ERROR(
            ValidateSize(entry.field.size(), impl_->options.max_key_bytes, "entry.field"));
        RETURN_IF_STATUS_ERROR(
            ValidateSize(entry.value.size(), impl_->options.max_value_bytes, "entry.value"));
        RETURN_IF_STATUS_ERROR(
            ValidateSize(entry.route_json.size(), impl_->options.max_value_bytes, "entry.route_json"));
    }
    if (!count_key.empty()) {
        RETURN_IF_STATUS_ERROR(ValidateSize(count_key.size(), impl_->options.max_key_bytes, "count_key"));
        RETURN_IF_STATUS_ERROR(
            ValidateSize(count_value.size(), impl_->options.max_value_bytes, "count_value"));
    }
    struct CoalescedEntry {
        HashEntry entry;
        size_t order = 0;
    };
    std::vector<CoalescedEntry> coalesced;
    coalesced.reserve(entries.size());
    std::unordered_map<std::string, size_t> index_by_hash_field;
    for (const auto& entry : entries) {
        const std::string identity = entry.key + "\n" + entry.field;
        auto iter = index_by_hash_field.find(identity);
        if (iter == index_by_hash_field.end()) {
            index_by_hash_field.emplace(identity, coalesced.size());
            coalesced.push_back(CoalescedEntry{entry, coalesced.size()});
        } else {
            coalesced[iter->second].entry = entry;
        }
    }
    std::stable_sort(coalesced.begin(), coalesced.end(), [](const CoalescedEntry& left, const CoalescedEntry& right) {
        if (left.entry.route_json == right.entry.route_json) {
            return left.order < right.order;
        }
        return left.entry.route_json < right.entry.route_json;
    });
    return impl_->WithRetry(true, [&]() {
        std::unique_ptr<Pipeline> pipeline;
        Pipeline* raw_pipeline = nullptr;
        RETURN_IF_STATUS_ERROR(impl_->table->OpenPipeline(&raw_pipeline));
        pipeline.reset(raw_pipeline);
        for (const auto& item : coalesced) {
            RETURN_IF_STATUS_ERROR(pipeline->HSet(item.entry.key, item.entry.field, item.entry.value));
        }
        if (!count_key.empty()) {
            RETURN_IF_STATUS_ERROR(pipeline->Set(count_key, count_value));
        }
        const std::vector<Status> statuses = pipeline->Sync();
        for (const auto& status : statuses) {
            RETURN_IF_STATUS_ERROR(status);
        }
        return Status::OK();
    });
}

Status TemporalStoreClient::SAdd(const std::string& key, const std::string& member) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(key, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(key.size(), impl_->options.max_key_bytes, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(member.size(), impl_->options.max_value_bytes, "member"));
    set::SAddRequest request;
    request.set_key(key);
    request.add_members(member);
    set::SAddResponse response;
    return impl_->ExecuteRaw(Module::SET, set::SADD, key, request, &response, true);
}

Status TemporalStoreClient::SMembers(const std::string& key, std::vector<std::string>* members) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(key, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(key.size(), impl_->options.max_key_bytes, "key"));
    RETURN_IF_STATUS_ERROR(ValidateOutput(members, "members"));
    set::SMembersRequest request;
    request.set_key(key);
    set::SMembersResponse response;
    RETURN_IF_STATUS_ERROR(
        impl_->ExecuteRaw(Module::SET, set::SMEMBERS, key, request, &response, false));
    members->assign(response.members().begin(), response.members().end());
    return Status::OK();
}

Status TemporalStoreClient::AddFeaturePoints(const std::string& key,
                                             const std::vector<TemporalFeaturePoint>& points,
                                             TemporalFeatureWritePolicy policy) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(key, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(key.size(), impl_->options.max_key_bytes, "key"));
    if (points.empty()) {
        return Status::InvalidArgument("points is empty");
    }
    for (const auto& point : points) {
        RETURN_IF_STATUS_ERROR(
            ValidateSize(point.value.size(), impl_->options.max_value_bytes, "point.value"));
    }

    const size_t max_points =
        impl_->options.max_feature_points_per_request <= 0
            ? points.size()
            : static_cast<size_t>(impl_->options.max_feature_points_per_request);
    for (size_t offset = 0; offset < points.size(); offset += max_points) {
        const size_t end = std::min(points.size(), offset + max_points);
        feature2::AddRequest request;
        request.set_key(key);
        request.set_format("protobuf");
        request.set_policy(ToProto(policy));
        request.mutable_point_list()->Reserve(static_cast<int>(end - offset));
        for (size_t i = offset; i < end; ++i) {
            auto* proto_point = request.add_point_list();
            proto_point->set_ts(points[i].timestamp);
            proto_point->set_value(points[i].value);
        }
        feature2::AddResponse response;
        RETURN_IF_STATUS_ERROR(
            impl_->ExecuteRaw(Module::FEATURE, feature2::ADD, key, request, &response, true));
    }
    return Status::OK();
}

Status TemporalStoreClient::QueryFeaturePoints(const std::string& key, uint64_t start_ts,
                                               uint64_t end_ts, uint64_t count,
                                               std::vector<TemporalFeaturePoint>* points) {
    TemporalFeatureQuery query;
    query.start_ts = start_ts;
    query.end_ts = end_ts;
    query.count = count;
    return QueryFeaturePoints(key, query, points);
}

Status TemporalStoreClient::QueryFeaturePoints(const std::string& key,
                                               const TemporalFeatureQuery& query,
                                               std::vector<TemporalFeaturePoint>* points) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(key, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(key.size(), impl_->options.max_key_bytes, "key"));
    RETURN_IF_STATUS_ERROR(ValidateOutput(points, "points"));
    RETURN_IF_STATUS_ERROR(ValidateFeatureQuery(impl_->options, query));
    feature2::QueryRequest request;
    request.set_key(key);
    request.set_start_ts(query.start_ts);
    request.set_end_ts(query.end_ts);
    request.set_count(query.count);
    request.set_format("protobuf");
    for (const auto& filter : query.filters) {
        request.add_filters(BuildFilterExpression(filter));
    }
    feature2::QueryResponse response;
    RETURN_IF_STATUS_ERROR(
        impl_->ExecuteRaw(Module::FEATURE, feature2::QUERY, key, request, &response, false));
    points->clear();
    points->reserve(response.point_list_size());
    for (const auto& point : response.point_list()) {
        points->push_back(TemporalFeaturePoint{point.ts(), point.value()});
    }
    return Status::OK();
}

Status TemporalStoreClient::AddSequenceFeatureRows(const std::string& key,
                                                   const std::vector<SequenceFeatureRow>& rows,
                                                   TemporalFeatureWritePolicy policy) {
    if (rows.empty()) {
        return Status::InvalidArgument("rows is empty");
    }
    std::vector<TemporalFeaturePoint> points;
    points.reserve(rows.size());
    for (const auto& row : rows) {
        points.push_back(TemporalFeaturePoint{row.timestamp, SerializeSequenceFeatureRow(row)});
    }
    return AddFeaturePoints(key, points, policy);
}

Status TemporalStoreClient::QuerySequenceFeatureRows(const std::string& key,
                                                     const TemporalFeatureQuery& query,
                                                     std::vector<SequenceFeatureRow>* rows) {
    RETURN_IF_STATUS_ERROR(ValidateOutput(rows, "rows"));
    std::vector<TemporalFeaturePoint> raw_points;
    RETURN_IF_STATUS_ERROR(QueryFeaturePoints(key, query, &raw_points));
    rows->clear();
    rows->reserve(raw_points.size());
    for (const auto& raw : raw_points) {
        SequenceFeatureRow row;
        RETURN_IF_STATUS_ERROR(ParseSequenceFeatureRow(raw, &row));
        rows->push_back(row);
    }
    return Status::OK();
}

Status TemporalStoreClient::AddIpsInstance(const IpsInstance& instance) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    if (instance.uid == 0) {
        return Status::InvalidArgument("instance.uid is required");
    }
    if (instance.timestamp_us == 0) {
        return Status::InvalidArgument("instance.timestamp_us is required");
    }
    if (instance.features.empty()) {
        return Status::InvalidArgument("instance.features is empty");
    }

    ips::AddRequest request;
    request.set_table(instance.table);
    request.set_enable_server_aggregator(instance.enable_server_aggregator);
    request.set_enable_idempotent(instance.enable_idempotent);
    auto* proto_instance = request.add_instance_list();
    proto_instance->set_uid(instance.uid);
    proto_instance->set_ts(instance.timestamp_us);
    proto_instance->set_action_type(instance.action_type);
    proto_instance->set_table(instance.logical_table);
    for (const auto& feature : instance.features) {
        auto* proto_feature = proto_instance->add_feature_stat32_list();
        proto_feature->set_id(feature.id);
        proto_feature->set_slot(feature.slot);
        proto_feature->set_has_slot(feature.has_slot);
        proto_feature->set_type(feature.type);
        proto_feature->mutable_int_pair()->set_v1(feature.v1);
        proto_feature->mutable_int_pair()->set_v2(feature.v2);
    }
    ips::AddResponse response;
    RETURN_IF_STATUS_ERROR(impl_->ExecuteRaw(Module::IPS, ips::ADD, std::to_string(instance.uid),
                                             request, &response, true));
    if (response.err_code() != ips::SUCCESS) {
        return Status::Internal("IPS add failed: " + response.error_desc());
    }
    return Status::OK();
}

Status TemporalStoreClient::QueryIpsLastInstances(const IpsLastQuery& query,
                                                  std::vector<IpsFeatureStat>* features) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    RETURN_IF_STATUS_ERROR(ValidateOutput(features, "features"));
    if (query.uid == 0) {
        return Status::InvalidArgument("query.uid is required");
    }

    ips::BatchQueryRequest request;
    auto* proto_query = request.add_reqs();
    proto_query->set_uid(query.uid);
    proto_query->set_decoupled(false);
    proto_query->set_table(query.table);
    proto_query->mutable_data_range()->set_type(ips::LAST_INSTANCES);
    proto_query->mutable_data_range()->set_range_val(query.last_instances);
    proto_query->mutable_filter()->set_table(query.logical_table);
    proto_query->mutable_filter()->set_action_type(query.action_type);
    proto_query->mutable_filter()->set_slot(query.slot);
    proto_query->mutable_filter()->set_top_k(query.top_k);
    proto_query->mutable_filter()->set_optor(ips::SORT_BY_TS);

    ips::BatchQueryResponse response;
    RETURN_IF_STATUS_ERROR(impl_->ExecuteRaw(Module::IPS, ips::BATCH_QUERY, std::to_string(query.uid),
                                             request, &response, false));
    if (response.err_code() != ips::SUCCESS) {
        return Status::NotFound("IPS query failed: " + response.error_desc());
    }
    if (response.rsps_size() < 1) {
        return Status::NotFound("IPS query returned no response");
    }
    features->clear();
    for (const auto& proto_feature : response.rsps(0).feature_stat32_list()) {
        IpsFeatureStat feature;
        feature.id = proto_feature.id();
        feature.slot = proto_feature.slot();
        feature.has_slot = proto_feature.has_slot();
        feature.type = proto_feature.type();
        feature.v1 = proto_feature.int_pair().v1();
        feature.v2 = proto_feature.int_pair().v2();
        features->push_back(feature);
    }
    return Status::OK();
}

Status TemporalStoreClient::RiskIncrement(const std::string& key, int64_t amount,
                                          uint64_t ttl_seconds, RiskPrecision precision,
                                          const std::string& uuid,
                                          uint64_t occur_time_seconds) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(key, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(key.size(), impl_->options.max_key_bytes, "key"));
    risk::HsetRequest request;
    request.set_key(key);
    request.set_value(std::to_string(amount));
    request.set_ttl(ttl_seconds);
    request.set_htype(risk::COUNT);
    request.set_precision(ToProto(precision));
    request.set_occur_time(
        occur_time_seconds == 0 ? static_cast<uint64_t>(std::time(nullptr)) : occur_time_seconds);
    if (!uuid.empty()) {
        request.set_uuid(uuid);
    }
    risk::HsetResponse response;
    RETURN_IF_STATUS_ERROR(impl_->ExecuteRaw(Module::RISK, risk::HSET, key, request, &response,
                                             true));
    if (response.err_code() != 0) {
        return Status::Internal("risk increment failed: " + response.err_msg());
    }
    return Status::OK();
}

Status TemporalStoreClient::RiskCount(const std::string& key, RiskPrecision precision,
                                      const RiskWindow& window, int64_t* count) {
    RETURN_IF_STATUS_ERROR(CheckInitialized());
    RETURN_IF_STATUS_ERROR(ValidateNotEmpty(key, "key"));
    RETURN_IF_STATUS_ERROR(ValidateSize(key.size(), impl_->options.max_key_bytes, "key"));
    RETURN_IF_STATUS_ERROR(ValidateOutput(count, "count"));
    risk::HqueryRequest request;
    request.set_key(key);
    request.set_precision(ToProto(precision));
    request.set_htype(risk::COUNT);
    auto* proto_window = request.add_windows();
    proto_window->set_start(window.start);
    proto_window->set_end(window.end);
    proto_window->set_unit(ToProto(window.unit));

    risk::HqueryResponse response;
    RETURN_IF_STATUS_ERROR(impl_->ExecuteRaw(Module::RISK, risk::HQUERY, key, request, &response,
                                             false));
    if (response.err_code() != 0) {
        return Status::Internal("risk count failed: " + response.err_msg());
    }
    if (response.result_list_size() < 1 || !response.result_list(0).has_result()) {
        return Status::NotFound("risk count has no result");
    }
    *count = response.result_list(0).result();
    return Status::OK();
}

}  // namespace client
}  // namespace bcache2
