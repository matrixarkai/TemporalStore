// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/partition.h"

#include <absl/strings/string_view.h>
#include <limits>
#include <memory>
#include <utility>

#include "butil/endpoint.h"
#include "common/coclosure.h"
#include "common/logging.h"
#include "common/scoped_invoker.h"
#include "common/time_tracer.h"
#include "partition/metrics.h"
#include "partition/remote_partition_stream.h"
#include "partition/storage/evicter.h"
#include "partition/storage/expirer.h"
#include "partition/storage/object_manager.h"
#include "partition/storage/page_compactor.h"
#include "partition/storage/page_gc.h"
#include "partition/storage/replicator.h"
#include "partition/storage/slot_context_manager.h"
#include "partition/storage/slot_store.h"
#include "partition/storage/storage_manager.h"
#include "protocol/config.pb.h"

namespace bcache2 {
namespace partition {

DEFINE_bool(secondary_pull_stream_from_primary, true,
            "When true, readonly secondary partitions pull index/oplog/page stream payloads "
            "from the primary through ServerService RPCs after GetInfo. When false, "
            "secondaries open the stream URI directly through Env, which preserves the old "
            "shared object-store/local-file recovery path.");

static void OnBrpcDone(CoSyncClosure* sync) { sync->Run(); }

Partition::Partition(const Options& options)
    : options_(options),
      blockcache_(options.blockcache),
      readonly_(options.readonly),
      membership_info_(std::move(options.membership)),
      metrics_manager_(
          new MetricsManager({{"partition_id", std::to_string(options.partition_id)},  //
                              {"table", options.table_name}},
                             "partition")),
      allocator_manager_(new AllocatorManager(metrics_manager_.get())),
      slot_context_manager_(
          new SlotContextManager(metrics_manager_.get(), allocator_manager_.get())),
      cmd_metrics_(new RequestMetrics(metrics_manager_.get(), "cmd", {})),
      index_(new Index(this, allocator_manager_.get(), slot_context_manager_.get(),
                       metrics_manager_.get())),
      op_logger_(
          new OpLogger(this, index_.get(), slot_context_manager_.get(), metrics_manager_.get())),
      page_store_(new PageStore(this, index_.get(), options_.env, metrics_manager_.get(),
                                options_.config.stream_config().store_rep_policy(),
                                options.partition_id, blockcache_)),
      slot_store_(new SlotStore(this, index_.get(), page_store_.get(), op_logger_.get(),
                                slot_context_manager_.get(), metrics_manager_.get())),
      object_manager_(new ObjectManager(this, index_.get(), page_store_.get(), op_logger_.get(),
                                        slot_store_.get(), slot_context_manager_.get(),
                                        allocator_manager_.get())),
      evicter_(new Evicter(this, object_manager_.get(), index_.get(), op_logger_.get(),
                           slot_store_.get(), allocator_manager_.get(), metrics_manager_.get())),
      page_gc_(new PageGc(this, options_.env, index_.get(), page_store_.get(), slot_store_.get(),
                          op_logger_.get(), metrics_manager_.get())),
      page_compactor_(
          new PageCompactor(this, index_.get(), slot_store_.get(), metrics_manager_.get())),
      expirer_(new Expirer(index_.get(), object_manager_.get(), op_logger_.get(),
                           metrics_manager_.get())),
      cmd_executor_(new CmdExecutor(this, object_manager_.get(),
                    op_logger_.get(), metrics_manager_.get())),
      storage_manager_(new StorageManager(
          this, cmd_executor_.get(), index_.get(), page_store_.get(), op_logger_.get(),
          slot_store_.get(), evicter_.get(), expirer_.get(), object_manager_.get(), page_gc_.get(),
          page_compactor_.get(), slot_context_manager_.get(), metrics_manager_.get(),
          allocator_manager_.get())),
      replicator_(new Replicator(this, index_.get(), op_logger_.get(), page_store_.get(),
                                 object_manager_.get(), metrics_manager_.get())),
      cmd_(new CmdExecutorManager(object_manager_.get(), metrics_manager_.get())) {
    slot_store_->SetObjectManager(object_manager_.get());
    load_success_ = metrics_manager_->Get<MetricsEnv::Counter>(kMetricsLoadPartitionSuccess, {});
    load_failed_ = metrics_manager_->Get<MetricsEnv::Counter>(kMetricsLoadPartitionFailed, {});
    load_latency_ = metrics_manager_->Get<MetricsEnv::Histogram>(kMetricsLoadPartitionLatency, {});
    unload_success_ =
        metrics_manager_->Get<MetricsEnv::Counter>(kMetricsUnLoadPartitionSuccess, {});
    unload_latency_ =
        metrics_manager_->Get<MetricsEnv::Histogram>(kMetricsUnLoadPartitionLatency, {});

    InitMembership();
}

Partition::~Partition() {}

Status Partition::Load() {
    LOG_CALL_INFO().put("PartitionId", options_.partition_id);
    BYTE_ASSERT(IsCoContext());

    if (stage_ != PartitionLoadStage::INIT) {
        LOG_WARNING("Partition not init state").put("PartitionId", options_.partition_id);
        return Status::FailedPrecondition("Partition not init state");
    }

    BYTE_DEFER({
        if (stage_ != PartitionLoadStage::LOADED) {
            stage_ = PartitionLoadStage::FAILED;
            load_failed_->get()->Increment();
            LOG_WARNING("Partition load failed").put("PartitionId", options_.partition_id);
        }
    });
    stage_ = PartitionLoadStage::LOADING;
    ScopedLatency latency(load_latency_->get());
    TimeTracer tracer;

    LOG_INFO("Partition start load")
        .put("PartitionId", options_.partition_id)
        .put("Uri", options_.uri)
        .put("LoadVersion", options_.load_version)
        .put("Config", options_.config.ShortDebugString());
    Status status = SetupCondition();
    if (!status.ok()) {
        LOG_ERROR("Setup condition failed")
            .put("PartitionId", options_.partition_id)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("SetupCondition");

    PartitionInfo remote_info;
    status = SetupRemoteInfo(&remote_info);
    if (!status.ok()) {
        LOG_ERROR("Setup remote info failed")
            .put("PartitionId", options_.partition_id)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("SetupRemoteInfo");

    status = SetupIndex(remote_info);
    if (!status.ok()) {
        LOG_ERROR("Setup index failed")
            .put("PartitionId", options_.partition_id)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("SetupIndex");

    status = SetupOplogger(remote_info);
    if (!status.ok()) {
        LOG_ERROR("Setup oplogger failed")
            .put("PartitionId", options_.partition_id)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("SetupOplogger");

    status = SetupPageStore(remote_info);
    if (!status.ok()) {
        LOG_ERROR("Setup page store failed")
            .put("PartitionId", options_.partition_id)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("SetupPageStore");

    status = SetupObjectManager();
    if (!status.ok()) {
        LOG_ERROR("Setup object manager failed")
            .put("PartitionId", options_.partition_id)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("SetupObjectManager");

    status = SetupStorageManager();
    if (!status.ok()) {
        LOG_ERROR("Setup storage manager failed")
            .put("PartitionId", options_.partition_id)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("SetupStorageManager");

    status = SetupReplicator();
    if (!status.ok()) {
        LOG_ERROR("Setup replicator failed")
            .put("PartitionId", options_.partition_id)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("SetupReplicator");

    stage_ = PartitionLoadStage::LOADED;
    load_success_->get()->Increment();

    const LimitConfig& limit_config = options_.config.limit_config();
    cmd_executor_->UpdateLimitConfig(limit_config);

    LOG_INFO("Partition Load Success")
        .put("PartitionId", options_.partition_id)
        .put("TimeTrace", tracer);

    status = cmd_executor_->LoadModuleCustomConfig(options_.config);
    BYTE_ASSERT(status.ok());
    return Status::OK();
}

Status Partition::Unload() {
    BYTE_ASSERT(IsCoContext());

    LOG_CALL_INFO().put("PartitionId", options_.partition_id);
    BYTE_ASSERT(IsCoContext());

    if (stage_ != PartitionLoadStage::LOADED && stage_ != PartitionLoadStage::FAILED) {
        return Status::FailedPrecondition("Invalid load stage " + PartitionLoadStage_Name(stage_));
    }

    ScopedLatency latency(unload_latency_->get());
    stage_ = PartitionLoadStage::UNLOADING;
    LOG_INFO("Partition start unload").put("PartitionId", options_.partition_id);

    while (inflight_io_count_ != 0) {
        LOG_INFO("Partition waiting for inflight io")
            .put("PartitionId", options_.partition_id)
            .put("InflightCount", inflight_io_count_);
        CoSleep(1 * 1000 * 1000);  // 1s
    }

    if (storage_manager_) {
        storage_manager_->Stop();
        LOG_INFO("Partition storage manager stopped").put("PartitionId", options_.partition_id);
    }

    if (replicator_) {
        replicator_->Stop();
        LOG_INFO("Partition replicator stopped").put("PartitionId", options_.partition_id);
    }

    if (op_logger_) {
        SYNC_CALL0(op_logger_->Close);
        LOG_INFO("Partition oplogger stopped").put("PartitionId", options_.partition_id);
    }

    if (page_store_) {
        page_store_->Close();
        LOG_INFO("Partition page store stopped").put("PartitionId", options_.partition_id);
    }

    if (index_) {
        SYNC_CALL0(index_->Close);
        LOG_INFO("Partition index stopped").put("PartitionId", options_.partition_id);
    }

    stage_ = PartitionLoadStage::UNLOADED;
    unload_success_->get()->Increment();
    LOG_INFO("Partition unloaded").put("PartitionId", options_.partition_id);
    return Status::OK();
}

Status Partition::SetupCondition() {
    std::string condition_uri = options_.uri + "-condition";
    Controller ctrl;
    stream::Env::ConditionData condition_data;
    options_.env->GetCondition(&ctrl, condition_uri, &condition_data);
    if (!ctrl.status().ok() && !ctrl.status().IsStoreNotFound()) {
        LOG_ERROR("Get condition data failed")
            .put("PartitionId", options_.partition_id)
            .put("Uri", options_.uri)
            .put("LockUri", condition_uri)  // yes, this is in essence just a lock
            .put("Error", ctrl.status().ToString());
        return ctrl.status();
    }

    stream::Env::Condition old_condition;
    if (ctrl.status().ok()) {
        old_condition.name = condition_uri;
        old_condition.data = condition_data;
    } else if (readonly_) {
        LOG_WARNING("Missing condition info").put("PartitionId", options_.partition_id);
        return Status::FailedPrecondition("Missing condition info");
    }

    ConditionInfoObserver condition_os(old_condition.data.data());
    if (!readonly_             // RW partition
        && ctrl.status().ok()  // found Condition
        && options_.load_version <= condition_os.LoadVersion()) {
        LOG_ERROR("Load version should be larger")
            .put("PartitionId", options_.partition_id)
            .put("Uri", options_.uri)
            .put("OldVersion", condition_os.LoadVersion())
            .put("NewVersion", options_.load_version);
        return Status::InvalidArgument("Load version should be larger");
    }

    if (readonly_) {
        // no need set condition for RO partition
        LOG_INFO("Load condition success")
            .put("PartitionId", options_.partition_id)
            .put("Uri", options_.uri)
            .put("Condition", ConditionInfoObserver(old_condition.data.data()));
        condition_ = std::move(old_condition);
        return Status::OK();
    }

    ConditionInfoV1::Structure condition_info;
    condition_info.version = ConditionVersion::kV1;
    condition_info.partition_id = options_.partition_id;
    condition_info.load_version = options_.load_version;
    if (!options_.host_v6.empty()) {
        condition_info.remote_family = AddressFamily::kIpv6;
        if (inet_pton(AF_INET6, options_.host_v6.c_str(), condition_info.remote_ip) != 1) {
            LOG_ERROR("Invalid IPv6 host")
                .put("PartitionId", options_.partition_id)
                .put("HostV6", options_.host_v6);
            return Status::InvalidArgument("Invalid IPv6 host");
        }
    } else {
        condition_info.remote_family = AddressFamily::kIpv4;
        if (inet_pton(AF_INET, options_.host.c_str(), condition_info.remote_ip) != 1) {
            LOG_ERROR("Invalid IPv4 host")
                .put("PartitionId", options_.partition_id)
                .put("Host", options_.host);
            return Status::InvalidArgument("Invalid IPv4 host");
        }
    }
    condition_info.remote_port = options_.port;

    condition_.name = condition_uri;
    memcpy(condition_.data.data(), &condition_info, condition_.data.size());

    ctrl.Reset();
    // update the condition with this partition's new condition, i.e., to acquire the lock
    // to avoid race conditions, old_condition is passed
    options_.env->SetCondition(&ctrl, old_condition, condition_uri, condition_.data);
    if (!ctrl.status().ok()) {
        LOG_ERROR("Set condition failed")
            .put("PartitionId", options_.partition_id)
            .put("Uri", options_.uri)
            .put("LockUri", condition_uri)
            .put("Error", ctrl.status().ToString());
        return ctrl.status();
    }

    LOG_INFO("Setup condition success")
        .put("PartitionId", options_.partition_id)
        .put("Uri", options_.uri)
        .put("OldCondition", ConditionInfoObserver(old_condition.data.data()))
        .put("NewCondition", ConditionInfoObserver(condition_.data.data()));
    return Status::OK();
}

// for read-only replicas to connect to the primary replica
Status Partition::SetupRemoteInfo(PartitionInfo* remote_info) {
    if (!readonly_) {
        // RW partition, no need setup remote info
        return Status::OK();
    }

    BYTE_ASSERT(IsCoContext());

    ConditionInfoObserver condition_info(condition_.data.data());
    brpc::Channel channel;
    if (channel.Init(condition_info.RemoteIpStr().c_str(), condition_info.RemotePort(), nullptr) !=
        0) {
        LOG_ERROR("Invalid remote addr")
            .put("PartitionId", options_.partition_id)
            .put("RemoteIpStr", condition_info.RemoteIpStr())
            .put("RemotePort", condition_info.RemotePort())
            .put("ConditionInfo", condition_info.ToString());
        return Status::InvalidArgument("Invalid remote addr");
    }

    // TODO(wangtai.10): custom timeout
    brpc::Controller ctrl;
    bcache2::ServerService_Stub stub(&channel);
    GetInfoRequest request;
    GetInfoResponse response;
    request.mutable_opt()->set_trace_id(butil::fast_rand());
    request.set_partition_id(GetPrimaryPartitionId());

    CoSyncClosure sync;
    stub.GetInfo(&ctrl, &request, &response, brpc::NewCallback(&OnBrpcDone, &sync));
    sync.Wait();

    if (ctrl.Failed()) {
        LOG_ERROR("Failed to send rpc call")
            .put("PartitionId", options_.partition_id)
            .put("ErrorText", ctrl.ErrorText());
        return Status::DeadlineExceeded(ctrl.ErrorText());
    }

    if (response.status().code() != kOK) {
        LOG_ERROR("Failed to get remote partition info")
            .put("PartitionId", options_.partition_id)
            .put("Status", response.status().message());
        return Status::FromRpcStatus(response.status());
    }

    *remote_info = std::move(*response.mutable_partition_info());
    LOG_INFO("Setup remote info success")
        .put("PartitionId", options_.partition_id)
        .put("RemoteInfo", remote_info->ShortDebugString());
    return Status::OK();
}

Status Partition::SetupIndex(const PartitionInfo& remote_info) {
    TimeTracer tracer;

    std::unique_ptr<stream::Stream> stream;
    std::string index_stream_uri = options_.uri + "-index/";
    Status status = LoadStream(index_stream_uri, PARTITION_STREAM_INDEX, 0, &stream);
    if (!status.ok()) {
        LOG_ERROR("LoadStream index stream fail")
            .put("PartitionId", options_.partition_id)
            .put("Uri", index_stream_uri)
            .put("Status", status);
        return status;
    }
    tracer.AddEvent("LoadStream");

    Index::Options opts;
    opts.stream = stream.release();
    index_->Init(std::move(opts));
    tracer.AddEvent("Init");

    if (readonly_) {
        status = index_->RestoreInfo(remote_info.index_info());
        if (!status.ok()) {
            LOG_ERROR("Failed restore index info")
                .put("PartitionId", options_.partition_id)
                .put("Status", status)
                .put("IndexInfo", remote_info.index_info().ShortDebugString());
            return status;
        }
    }
    status = index_->Load();
    if (!status.ok()) {
        LOG_ERROR("Failed load index")
            .put("PartitionId", options_.partition_id)
            .put("Status", status.ToString());
        return status;
    }
    tracer.AddEvent("Load");

    LOG_INFO("Setup index success")
        .put("PartitionId", options_.partition_id)
        .put("Uri", index_stream_uri)
        .put("Time", tracer);
    return Status::OK();
}

Status Partition::SetupOplogger(const PartitionInfo& remote_info) {
    std::unique_ptr<stream::Stream> stream;
    std::string oplog_stream_uri = options_.uri + "-oplog/";
    Status status = LoadStream(oplog_stream_uri, PARTITION_STREAM_OPLOG, 0, &stream);
    if (!status.ok()) {
        LOG_ERROR("LoadStream fail")
            .put("PartitionId", options_.partition_id)
            .put("Status", status.ToString());
        return status;
    }

    OpLogger::Options opts;
    opts.stream = stream.release();
    op_logger_->Init(std::move(opts));

    if (readonly_) {
        status = op_logger_->RestoreInfo(remote_info.op_logger_info());
        if (!status.ok()) {
            LOG_ERROR("Failed restore oplogger info")
                .put("PartitionId", options_.partition_id)
                .put("OploggerInfo", remote_info.op_logger_info().ShortDebugString())
                .put("Status", status);
            return status;
        }
    }

    return Status::OK();
}

Status Partition::SetupObjectManager() {
    Status status = object_manager_->Load();
    if (!status.ok()) {
        LOG_ERROR("Failed to load oplog")
            .put("PartitionId", options_.partition_id)
            .put("Status", status.ToString());
        return status;
    }
    LOG_INFO("Setup object manager success").put("PartitionId", options_.partition_id);
    return Status::OK();
}

Status Partition::SetupPageStore(const PartitionInfo& remote_info) {
    PageStore::Options opts;
    opts.condition = condition_;
    opts.stream_uri_pattern = options_.uri + "-page%d/";
    page_store_->Init(std::move(opts));

    Status status = page_store_->UpdateZones();
    if (!status.ok()) {
        LOG_ERROR("UpdateZones fail")
            .put("PartitionId", options_.partition_id)
            .put("Status", status.ToString());
        return status;
    }

    if (readonly_) {
        status = page_store_->RestoreInfo(remote_info.page_store_info());
        if (!status.ok()) {
            LOG_ERROR("Failed restore page store info")
                .put("PartitionId", options_.partition_id)
                .put("Status", status.ToString())
                .put("PageStoreInfo", remote_info.page_store_info().ShortDebugString());
            return status;
        }
    }

    LOG_INFO("Setup page store success").put("PartitionId", options_.partition_id);
    return Status::OK();
}

Status Partition::SetupStorageManager() {
    if (readonly_) {
        options_.config.mutable_evicter_config()->mutable_operation_type()->set_value(
            OperationType::DUMP);
    }
    evicter_->Init(options_.config.evicter_config());

    StorageManager::Options opts;
    if (readonly_) {
        opts.enable_prepare = false;
        opts.enable_oplog_rolling = false;
        opts.enable_evict = true;
        opts.enable_expire = false;
        opts.enable_page_gc = false;
        opts.enable_index_gc = false;
        opts.enable_page_compaction = false;
    }
    storage_manager_->Init(std::move(opts));

    if (FLAGS_start_storage_manager_when_loading) {
        storage_manager_->Start();
    }

    LOG_INFO("Setup storage manager success").put("PartitionId", options_.partition_id);
    return Status::OK();
}

Status Partition::SetupReplicator() {
    if (readonly_) {  // read-only followers
        LOG_INFO("Start replicator").put("PartitionId", options_.partition_id);
        replicator_->Start();
    }
    return Status::OK();
}

Status Partition::OpenPartitionStream(const std::string& uri, PartitionStreamKind stream_kind,
                                      uint32_t zone_id, bool created, stream::Stream** stream) {
    LOG_INFO("LoadStream").put("PartitionId", options_.partition_id).put("Uri", uri);

    if (readonly_ && FLAGS_secondary_pull_stream_from_primary) {
        RemotePartitionStream::Options remote_options;
        remote_options.partition = this;
        remote_options.stream_kind = stream_kind;
        remote_options.zone_id = zone_id;
        std::unique_ptr<stream::Stream> remote_stream(new RemotePartitionStream(remote_options));
        Status status = remote_stream->Load();
        if (!status.ok()) {
            return status;
        }
        *stream = remote_stream.release();
        LOG_INFO("Open remote primary-backed stream success")
            .put("PartitionId", options_.partition_id)
            .put("PrimaryPartitionId", GetPrimaryPartitionId())
            .put("StreamKind", stream_kind)
            .put("ZoneId", zone_id)
            .put("Uri", uri);
        return Status::OK();
    }

    if (readonly_) {
        LOG_INFO("Open readonly stream from Env")
            .put("PartitionId", options_.partition_id)
            .put("PrimaryPartitionId", GetPrimaryPartitionId())
            .put("StreamKind", stream_kind)
            .put("ZoneId", zone_id)
            .put("Uri", uri);
    }

    Controller ctrl;
    stream::Env::OpenOptions options;
    options.created = created;
    options.metrics_manager = metrics_manager_.get();
    options.readonly = readonly_;
    options.rep_policy = options_.config.stream_config().store_rep_policy();
    options_.env->OpenStream(&ctrl, condition_, uri, options, stream);
    if (!ctrl.status().ok()) {
        return ctrl.status();
    }
    return Status::OK();
}

Status Partition::LoadStream(const std::string& uri, PartitionStreamKind stream_kind,
                             uint32_t zone_id,
                             std::unique_ptr<stream::Stream>* stream_ptr) {
    stream::Stream* stream = nullptr;
    Status status = OpenPartitionStream(uri, stream_kind, zone_id, true, &stream);
    if (!status.ok()) {
        return status;
    }
    stream_ptr->reset(stream);
    return Status::OK();
}

void Partition::ReadPartitionStream(Controller* ctrl, const ReadPartitionStreamRequest* request,
                                    ReadPartitionStreamResponse* response,
                                    Closure<void>* callback) {
    if (request->size() > std::numeric_limits<size_t>::max()) {
        ctrl->set_status(Status::InvalidArgument("Partition stream read size too large"));
        callback->Run();
        return;
    }
    const size_t read_size = static_cast<size_t>(request->size());
    std::string* data = response->mutable_data();
    data->resize(read_size);
    void* buffer = data->empty() ? nullptr : &(*data)[0];

    switch (request->stream_kind()) {
    case PARTITION_STREAM_INDEX:
        index_->ReadRawStream(ctrl, request->offset(), buffer, read_size, callback);
        return;
    case PARTITION_STREAM_OPLOG:
        op_logger_->ReadRawStream(ctrl, request->offset(), buffer, read_size, callback);
        return;
    case PARTITION_STREAM_PAGE:
        page_store_->ReadRawStream(request->zone_id(), ctrl, request->offset(), buffer,
                                   read_size, callback);
        return;
    default:
        ctrl->set_status(Status::InvalidArgument("Unknown partition stream kind"));
        callback->Run();
        return;
    }
}

void Partition::ScanPartitionStream(Controller* ctrl, const ScanPartitionStreamRequest* request,
                                    ScanPartitionStreamResponse* response,
                                    Closure<void>* callback) {
    ScopedInvoker done(callback);

    stream::ScopedIterator iter;
    switch (request->stream_kind()) {
    case PARTITION_STREAM_INDEX:
        iter = index_->NewRawStreamIterator(request->start_offset(), request->end_offset());
        break;
    case PARTITION_STREAM_OPLOG:
        iter = op_logger_->NewRawStreamIterator(request->start_offset(), request->end_offset());
        break;
    case PARTITION_STREAM_PAGE:
        iter = page_store_->NewRawStreamIterator(request->zone_id(), request->start_offset(),
                                                 request->end_offset());
        break;
    default:
        ctrl->set_status(Status::InvalidArgument("Unknown partition stream kind"));
        return;
    }

    if (iter == nullptr) {
        ctrl->set_status(Status::NotFound("Partition stream not found"));
        return;
    }

    const uint32_t max_records = request->max_records() > 0 ? request->max_records() : 1;
    const uint64_t max_bytes =
        request->max_bytes() > 0 ? request->max_bytes() : std::numeric_limits<uint64_t>::max();
    uint64_t returned_bytes = 0;

    for (uint32_t i = 0; i < max_records; ++i) {
        Status status = iter->Next();
        if (status.IsOutOfRange()) {
            response->set_end_of_stream(true);
            ctrl->set_status(Status::OK());
            return;
        }
        if (!status.ok()) {
            ctrl->set_status(status);
            return;
        }

        const absl::string_view record = iter->Data();
        if (response->records_size() > 0 && returned_bytes + record.size() > max_bytes) {
            ctrl->set_status(Status::OK());
            return;
        }

        PartitionStreamRecord* out = response->add_records();
        out->set_offset(iter->Id());
        out->set_data(record.data(), record.size());
        returned_bytes += record.size();
    }

    ctrl->set_status(Status::OK());
}

Status Partition::SetConfig(const Config& config) {
    if (stage_ != PartitionLoadStage::LOADED) {
        return Status::FailedPrecondition("partition not loaded");
    }

    const Config& original = GetConfig();
    if (config.version() < original.version()) {
        return Status::FailedPrecondition("legacy config version");
    }
    if (config.version() == original.version()) {
        return Status::OK();
    }

    options_.config.MergeFrom(config);
    if (readonly_) {
        options_.config.mutable_evicter_config()->mutable_operation_type()->set_value(
            OperationType::DUMP);
    }

    op_logger_->UpdateConfig(config);
    index_->UpdateConfig(config);
    page_store_->UpdateConfig(config);
    evicter_->UpdateConfig(config.evicter_config());
    cmd_executor_->UpdateLimitConfig(config.limit_config());
    cmd_executor_->UpdateModuleCustomConfig(config);
    return Status::OK();
}

const Config& Partition::GetConfig() const { return options_.config; }

PartitionInfo Partition::GetInfo() const {
    PartitionInfo info;
    info.set_stage(stage_);
    info.set_readonly(readonly_);
    if (LIKELY(stage_ == PartitionLoadStage::LOADED)) {
        *info.mutable_index_info() = std::move(index_->GetInfo());
        *info.mutable_index_info()->mutable_model_alloc_stats() =
            std::move(allocator_manager_->GetAllocator(AllocatorType::kModel)->GetStats());
        *info.mutable_index_info()->mutable_slot_context_alloc_stats() =
            std::move(allocator_manager_->GetAllocator(AllocatorType::kSlotContext)->GetStats());
        *info.mutable_op_logger_info() = std::move(op_logger_->GetInfo());
        *info.mutable_page_store_info() = std::move(page_store_->GetInfo());
        *info.mutable_condition_info() =
            std::move(ConditionInfoObserver(condition_.data.data()).TransformProtocol());
        *info.mutable_replicator_info() = std::move(replicator_->GetInfo());
    }
    return info;
}

void Partition::InitMembership() {
    if (membership_info_.units_size() == 0) {
        LOG_INFO("no membership info, i am controlled by alchemy cfgsrv")
            .put("PartitionId", options_.partition_id);
        primary_partition_id_ = options_.partition_id;
        return;
    }
    for (const auto& unit : membership_info_.units()) {
        for (uint64_t partition_id : unit.active_id_list()) {
            if (partition_id == options_.partition_id) {
                primary_partition_id_ = unit.primary_id();
                partition_unit_id_ = unit.partition_unit_id();
                break;
            }
        }
    }  // for membership
}

Status Partition::UpdateMembership(const MembershipInfo& info) {
    if (info.partition_set_version() < membership_info_.partition_set_version()) {
        return Status::FailedPrecondition("legacy membership info");
    }
    bool global_update = false;
    if (info.partition_set_version() > membership_info_.partition_set_version()) {
        global_update = true;
    }

    bool im_valid = false;
    for (const auto& unit : info.units()) {
        if (unit.partition_unit_id() != partition_unit_id_) {
            continue;
        }

        if (!global_update && unit.version() == partition_unit_version_) {
            // no change
            return Status::OK();
        } else if (unit.version() < partition_unit_version_) {
            return Status::FailedPrecondition("legacy membership unit info");
        }

        partition_unit_version_ = unit.version();
        if (primary_partition_id_ != unit.primary_id()) {
            LOG_WARNING("primary partition changed")
                .put("PartitionId", options_.partition_id)
                .put("from", primary_partition_id_)
                .put("to", unit.primary_id());
            primary_partition_id_ = unit.primary_id();
        }
        for (uint64_t partition_id : unit.active_id_list()) {
            if (partition_id == options_.partition_id) {
                im_valid = true;
            }
        }
        break;
    }
    membership_info_ = info;
    if (!im_valid) {
        LOG_WARNING("im not in valid membership now")
            .put("partition_id", options_.partition_id)
            .put("membership_version", membership_info_.partition_set_version());
        // Note: Unload behavior is performed in meta_tinker
    }

    return Status::OK();
}

Status Partition::GetStats(PartitionStats* stats) {
    stats->set_id(options_.partition_id);
    stats->set_stage(stage_);
    if (stage_ != PartitionLoadStage::LOADED) {
        return Status::OK();
    }
    if (readonly_) {
        *stats->mutable_replicator_status() = replicator_->GetStatus().ToRpcStatus();
        stats->set_role(PartitionRole::PARTITION_ROLE_SECONDARY);
    } else {
        stats->set_role(PartitionRole::PARTITION_ROLE_PRIMARY);
    }

    // ...
    return Status::OK();
}

Status Partition::UpdateCondition() {
    std::string condition_uri = options_.uri + "-condition";
    Controller ctrl;
    stream::Env::ConditionData condition_data;
    options_.env->GetCondition(&ctrl, condition_uri, &condition_data);
    if (!ctrl.status().ok()) {
        LOG_WARNING("Get condition data failed")
            .put("PartitionId", options_.partition_id)
            .put("Uri", options_.uri)
            .put("LockUri", condition_uri)
            .put("Status", ctrl.status());
        return ctrl.status();
    }

    stream::Env::Condition new_condition;
    new_condition.name = condition_uri;
    new_condition.data = condition_data;

    ConditionInfoObserver new_condition_info(new_condition.data.data());
    ConditionInfoObserver curr_condition_info(condition_.data.data());
    if (UNLIKELY(new_condition_info.LoadVersion() < curr_condition_info.LoadVersion())) {
        // maybe bug
        LOG_ERROR("Load version should be larger")
            .put("PartitionId", options_.partition_id)
            .put("Uri", options_.uri)
            .put("CurrConditionInfo", curr_condition_info.ToString())
            .put("NewConditionInfo", new_condition_info.ToString());
        return Status::FailedPrecondition("Invalid condition info");
    }

    condition_.name = condition_uri;
    condition_.data = condition_data;
    return Status::OK();
}

}  // namespace partition
}  // namespace bcache2
