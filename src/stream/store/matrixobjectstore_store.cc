// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "stream/store/matrixobjectstore_store.h"

#include <absl/strings/str_format.h>
#include <byte/string/format.h>

#include <algorithm>

#include "common/string_utils.h"
#include "stream/store/matrixobjectstore_wrapper.h"

namespace bcache2 {
namespace stream {

namespace {

inline std::string MatrixObjectStoreStatusString(matrixobjectstore_status status) {
    switch (status) {
    case STATUS_OK:
        return "MATRIXOBJECTSTORE_OK";
    case STATUS_NOT_FOUND:
        return "MATRIXOBJECTSTORE_NOT_FOUND";
    case STATUS_ALREADY_EXIST:
        return "MATRIXOBJECTSTORE_ALREADY_EXIST";
    case STATUS_CORRUPTION:
        return "MATRIXOBJECTSTORE_CORRUPTION";
    case STATUS_NOT_SUPPORTED:
        return "MATRIXOBJECTSTORE_NOT_SUPPORTED";
    case STATUS_INVALID_ARGUMENT:
        return "MATRIXOBJECTSTORE_INVALID_ARGUMENT";
    case STATUS_IO_ERROR:
        return "MATRIXOBJECTSTORE_IO_ERROR";
    case STATUS_TIMEOUT:
        return "MATRIXOBJECTSTORE_TIMEOUT";
    case STATUS_PERMISSION_DENIED:
        return "MATRIXOBJECTSTORE_PERMISSION_DENIED";
    case STATUS_IO_BUSY:
        return "MATRIXOBJECTSTORE_IO_BUSY";
    case STATUS_END_FILE:
        return "MATRIXOBJECTSTORE_END_FILE";
    case STATUS_INTERNAL_ERR:
        return "MATRIXOBJECTSTORE_INTERNAL_ERR";
    case STATUS_HANDLE_CLOSED:
        return "MATRIXOBJECTSTORE_HANDLE_CLOSED";
    case STATUS_NEED_REOPEN:
        return "MATRIXOBJECTSTORE_NEED_REOPEN";
    case STATUS_NOT_READY:
        return "MATRIXOBJECTSTORE_NOT_READY";
    case STATUS_NO_SPACE:
        return "MATRIXOBJECTSTORE_NO_SPACE";
    case STATUS_POWER_OFF:
        return "MATRIXOBJECTSTORE_POWER_OFF";
    case STATUS_OVERFLOW:
        return "MATRIXOBJECTSTORE_OVERFLOW";
    case STATUS_CONDITION_VERIFY_FAILED:
        return "MATRIXOBJECTSTORE_CONDITION_VERIFY_FAILED";
    case STATUS_NEED_REWRITE:
        return "MATRIXOBJECTSTORE_NEED_REWRITE";
    case STATUS_DATA_LOST:
        return "MATRIXOBJECTSTORE_DATA_LOST";
    case STATUS_CHECKSUM_FAIL:
        return "MATRIXOBJECTSTORE_CHECKSUM_FAIL";
    case STATUS_HARDLINK_OUT_LIMIT:
        return "MATRIXOBJECTSTORE_HARDLINK_OUT_LIMIT";
    case STATUS_UNKNOWN_ERR:
        return "MATRIXOBJECTSTORE_UNKNOWN_ERR";
    default:
        return "MATRIXOBJECTSTORE_" + std::to_string(static_cast<int>(status));
    }
}

inline Status MatrixObjectStoreMessageToStatus(const matrixobjectstore_message& message) {
    switch (message.status_) {
    case STATUS_CONDITION_VERIFY_FAILED:
        return Status::StoreConditionFailed("Condition changed");
    case STATUS_NOT_FOUND:
        return Status::StoreNotFound("Not found");
    default:
        return Status::StoreInternal(MatrixObjectStoreStatusString(message.status_));
    }
}

}  // namespace

BlobImpl::BlobImpl(const std::string& uri, MetricsManager* metrics_manager, matrixobjectstore_blob* blob)
    : uri_(uri), blob_(blob) {
    metrics_.Init(metrics_manager, uri);
}

BlobImpl::~BlobImpl() {}

void BlobImpl::Close() {
    LOG_CALL_DEBUG().put("Uri", uri_);
    matrixobjectstore_message message;
    MATRIXOBJECTSTORE_CLOSE(blob_, &message);
    if (message.status_ != STATUS_OK) {
        LOG_WARNING("Close blob failed")
            .put("Uri", uri_)
            .put("Status", MatrixObjectStoreMessageToStatus(message));
    }
    LOG_INFO("Close blob").put("Uri", uri_);
}

void BlobImpl::Append(Controller* ctrl, const void* data, size_t size, Closure<void>* callback) {
    LOG_CALL_DEBUG().put("Uri", uri_).put("Size", size);
    matrixobjectstore_io_options options = matrixobjectstore_default_write_options();
    options.timeout_in_ms_ = ctrl->timeout_ms();
    Task* task = new Task;
    task->ctrl = ctrl;
    task->callback = callback;
    task->thread = byte::GetCurrentThread();
    task->read = false;
    task->metrics = &metrics_;
    matrixobjectstore_message message;
    metrics_.append_qps->get()->Increment();
    metrics_.append_throughput->get()->Add(size);
    MATRIXOBJECTSTORE_ASYNC_WRITE(blob_, data, size, &options, &message, &BlobImpl::IoCallback, task);
    if (message.status_ != STATUS_OK) {
        std::unique_ptr<Task> scoped_ptr(task);
        LOG_ERROR("Append to matrixobjectstore: async write blob failed")
            .put("Uri", uri_)
            .put("Size", size)
            .put("Status", MatrixObjectStoreMessageToStatus(message));
        task->ctrl->set_status(MatrixObjectStoreMessageToStatus(message));
        task->thread->Invoke(callback);
        return;
    }
}

void BlobImpl::Read(Controller* ctrl, size_t offset, void* data, size_t size,
                    Closure<void>* callback) {
    LOG_CALL_DEBUG().put("Uri", uri_).put("Size", size).put("Offset", offset);
    matrixobjectstore_io_options read_options = matrixobjectstore_default_read_options();
    read_options.timeout_in_ms_ = ctrl->timeout_ms();
    Task* task = new Task;
    task->ctrl = ctrl;
    task->callback = callback;
    task->thread = byte::GetCurrentThread();
    task->read = true;
    task->metrics = &metrics_;
    metrics_.read_qps->get()->Increment();
    metrics_.read_throughput->get()->Add(size);
    matrixobjectstore_message message;
    MATRIXOBJECTSTORE_ASYNC_PREAD(blob_, data, size, offset, &read_options, &message, &BlobImpl::IoCallback,
                          task);
    if (message.status_ != STATUS_OK) {
        std::unique_ptr<Task> scoped_ptr(task);
        LOG_ERROR("Async pread blob failed")
            .put("Uri", uri_)
            .put("Offset", offset)
            .put("Size", size)
            .put("Status", MatrixObjectStoreMessageToStatus(message));
        task->ctrl->set_status(MatrixObjectStoreMessageToStatus(message));
        task->thread->Invoke(callback);
        return;
    }
}

void BlobImpl::IoCallback(ssize_t size, struct matrixobjectstore_message* message, void* args) {
    Task* task = static_cast<Task*>(args);
    // BYTE_ASSERT(!task->inplace);
    std::unique_ptr<Task> scoped_ptr(task);
    LOG_DEBUG("Callback").put("Size", size).put("Code", message->status_);
    if (task->read) {
        task->metrics->read_latency->get()->Set(task->cost.GetElapsedInUs());
    } else {
        task->metrics->append_latency->get()->Set(task->cost.GetElapsedInUs());
    }
    task->ctrl->set_status(message->status_ == STATUS_OK ? Status::OK()
                                                         : MatrixObjectStoreMessageToStatus(*message));
    task->thread->Invoke(task->callback);
}

MatrixObjectStoreImpl::MatrixObjectStoreImpl() {}

MatrixObjectStoreImpl::~MatrixObjectStoreImpl() {}

void MatrixObjectStoreImpl::SetCondition(Controller* ctrl, const std::string& uri,
                                 const ConditionData& data, const SetConditionOptions& options) {
    matrixobjectstore_update_inline_blob_options update_inline_blob_options =
        matrixobjectstore_default_update_inline_blob_options();
    update_inline_blob_options.timeout_in_ms_ = ctrl->timeout_ms();
    matrixobjectstore_inline_blob_stat stat;
    BYTE_ASSERT(data.size() == k_inline_blob_content_size);
    memcpy(stat.content_, data.data(), data.size());
    matrixobjectstore_message message;
    bool res =
        MATRIXOBJECTSTORE_UPDATE_INLINE_BLOB(uri.c_str(), &stat, &update_inline_blob_options, &message);
    // TODO(guogaofeng): matrixobjectstore_update_inline_blob should return false when status is not_found
    if (message.status_ == STATUS_NOT_FOUND) {
        matrixobjectstore_create_inline_blob_options create_options =
            matrixobjectstore_default_create_inline_blob_options();
        create_options.timeout_in_ms_ = ctrl->timeout_ms();
        res = MATRIXOBJECTSTORE_CREATE_INLINE_BLOB(uri.c_str(), &create_options, &message);
        if (!res) {
            LOG_ERROR("Create inline blob failed")
                .put("Uri", uri)
                .put("Status", MatrixObjectStoreMessageToStatus(message));
            ctrl->set_status(MatrixObjectStoreMessageToStatus(message));
            return;
        }
        res =
            MATRIXOBJECTSTORE_UPDATE_INLINE_BLOB(uri.c_str(), &stat, &update_inline_blob_options, &message);
    }

    // TODO(guogaofeng): matrixobjectstore_update_inline_blob should return false when status is not_found
    if (!res || message.status_ == STATUS_NOT_FOUND) {
        LOG_ERROR("Update inline blob failed")
            .put("Uri", uri)
            .put("Status", MatrixObjectStoreMessageToStatus(message));
        ctrl->set_status(MatrixObjectStoreMessageToStatus(message));
        return;
    }
    LOG_INFO("Update inline blob success")
        .put("Uri", uri)
        .put("Data", DebugRawString(data.data(), data.size()));
    ctrl->set_status(Status::OK());
}

void MatrixObjectStoreImpl::StatCondition(Controller* ctrl, const std::string& uri, ConditionData* data) {
    matrixobjectstore_stat_inline_blob_options options = matrixobjectstore_default_stat_inline_blob_options();
    options.timeout_in_ms_ = ctrl->timeout_ms();
    matrixobjectstore_inline_blob_stat stat;
    matrixobjectstore_message message;
    bool res = MATRIXOBJECTSTORE_STAT_INLINE_BLOB(uri.c_str(), &stat, &options, &message);
    if (!res) {
        LOG_ERROR("Stat inline blob failed")
            .put("Uri", uri)
            .put("Status", MatrixObjectStoreMessageToStatus(message));
        if (message.status_ == STATUS_NOT_FOUND) {
            ctrl->set_status(MatrixObjectStoreMessageToStatus(message));
        } else {
            ctrl->set_status(MatrixObjectStoreMessageToStatus(message));
        }
        return;
    }
    BYTE_ASSERT(data->size() == k_inline_blob_content_size);
    std::copy(std::begin(stat.content_), std::end(stat.content_), data->begin());
    ctrl->set_status(Status::OK());
    LOG_INFO("Stat inline blob success")
        .put("Uri", uri)
        .put("Data", DebugRawString(data->data(), data->size()));
}

void MatrixObjectStoreImpl::List(Controller* ctrl, const std::string& path, std::vector<BlobInfo>* files) {
    if (path.size() <= 2 || path.back() != '/') {
        LOG_ERROR("Invalid blob path").put("Uri", path);
        ctrl->set_status(Status::InvalidArgument("Path must end with '/'"));
        return;
    }

    std::string::size_type pos = path.find_last_of('/', path.size() - 2);
    if (pos == std::string::npos) {
        LOG_ERROR("Invalid blob path").put("Uri", path);
        ctrl->set_status(Status::InvalidArgument("Dir not found"));
        return;
    }
    std::string pool_name = path.substr(0, pos + 1);
    std::string dir_name = path.substr(pos + 1);

    matrixobjectstore_open_pool_options options = matrixobjectstore_default_open_pool_options();
    // Truncate data that exceeds prefix length
    snprintf(options.prefix_blob_name_,
             sizeof(options.prefix_blob_name_) / sizeof(options.prefix_blob_name_[0]), "%s",
             dir_name.c_str());
    snprintf(options.start_blob_name_,
             sizeof(options.start_blob_name_) / sizeof(options.start_blob_name_[0]), "%s",
             dir_name.c_str());
    options.timeout_in_ms_ = ctrl->timeout_ms();

    matrixobjectstore_message message;
    matrixobjectstore_pool* pool = MATRIXOBJECTSTORE_OPEN_POOL(pool_name.c_str(), &options, &message);
    if (pool == nullptr) {
        LOG_ERROR("Open matrixobjectstore pool failed")
            .put("PoolName", pool_name)
            .put("PrefixBlobName", dir_name)
            .put("Status", MatrixObjectStoreMessageToStatus(message));
        ctrl->set_status(MatrixObjectStoreMessageToStatus(message));
        return;
    }

    files->clear();
    matrixobjectstore_traverse_options traverse_options = matrixobjectstore_default_traverse_options();
    traverse_options.timeout_in_ms_ = 5000L;
    traverse_options.return_blob_id = true;
    matrixobjectstore_entry entry;
    while (MATRIXOBJECTSTORE_TRAVERSE_POOL(pool, &entry, &traverse_options, &message)) {
        if (message.status_ != STATUS_OK) {
            LOG_ERROR("Traverse matrixobjectstore pool failed")
                .put("PoolName", pool_name)
                .put("PrefixBlobName", pool_name)
                .put("Status", MatrixObjectStoreMessageToStatus(message));
            ctrl->set_status(MatrixObjectStoreMessageToStatus(message));
            return;
        }
        std::string origin_name = entry.name_;
        pos = origin_name.find('/');
        if (pos == std::string::npos) {
            LOG_ERROR("Blob name invalid").put("Uri", path).put("BlobName", origin_name);
            ctrl->set_status(Status::Internal("Blob name invalid"));
            return;
        }
        BlobInfo info;
        info.name = origin_name.substr(pos + 1);
        files->push_back(info);
    }
    if (message.status_ != STATUS_OK) {
        LOG_ERROR("Traverse matrixobjectstore pool failed")
            .put("PoolName", pool_name)
            .put("PrefixBlobName", pool_name)
            .put("Status", MatrixObjectStoreMessageToStatus(message));
        ctrl->set_status(MatrixObjectStoreMessageToStatus(message));
        return;
    }

    MATRIXOBJECTSTORE_CLOSE_POOL(pool, &message);
    if (message.status_ != STATUS_OK) {
        LOG_ERROR("Close matrixobjectstore pool failed")
            .put("PoolName", pool_name)
            .put("PrefixBlobName", pool_name)
            .put("Status", MatrixObjectStoreMessageToStatus(message));
    }

    ctrl->set_status(Status::OK());
    return;
}

void MatrixObjectStoreImpl::Open(Controller* ctrl, const std::string& uri, const OpenOptions& options,
                         Blob** blob) {
    LOG_CALL_DEBUG().put("Uri", uri);
    TimeCost time_cost;
    int open_mode = 0;
    if (options.mode == OpenMode::kRead) {
        open_mode = BS_O_RDONLY;
    } else if (options.mode == OpenMode::kWrite) {
        open_mode = BS_O_RDWR | BS_O_CREATE | BS_O_ASYNC;
    } else {
        ctrl->set_status(Status::InvalidArgument("Invalid open mode"));
        return;
    }

    matrixobjectstore_open_options open_options = matrixobjectstore_default_open_options();
    SetCondition(options.condition, &open_options.condition_);
    open_options.timeout_in_ms_ = ctrl->timeout_ms();
    switch (options.rep_policy.value()) {
    case StoreRepPolicy::REP_FLAT:
        open_options.rep_policy_ = ReplicatedPolicy::TYPE_REP_FLAT;
        break;
    case StoreRepPolicy::REP_G2:
        open_options.rep_policy_ = ReplicatedPolicy::TYPE_REP_G2;
        break;
    case StoreRepPolicy::REP_G3:
        open_options.rep_policy_ = ReplicatedPolicy::TYPE_REP_G3;
        break;
    case StoreRepPolicy::REP_G1:
        open_options.rep_policy_ = ReplicatedPolicy::TYPE_REP_G1;
        break;
    default:
        LOG_ERROR("Open blob failed, invalid rep policy")
            .put("Uri", uri)
            .put("RepPolicy", options.rep_policy.value());
        ctrl->set_status(Status::InvalidArgument("invalid rep policy"));
        return;
    }

    matrixobjectstore_message message;
    matrixobjectstore_blob* bs_blob = MATRIXOBJECTSTORE_OPEN(uri.c_str(), open_mode, &open_options, &message);
    if (bs_blob == nullptr) {
        LOG_ERROR("Open blob failed")
            .put("Uri", uri)
            .put("Status", MatrixObjectStoreMessageToStatus(message));
        ctrl->set_status(MatrixObjectStoreMessageToStatus(message));
        return;
    }

    LOG_INFO("Open blob success").put("Uri", uri);
    *blob = new BlobImpl(uri, options.metrics_manager, bs_blob);
    ctrl->set_status(Status::OK());
}

void MatrixObjectStoreImpl::Delete(Controller* ctrl, const std::string& uri, const DeleteOptions& options) {
    LOG_CALL_DEBUG().put("Uri", uri);
    TimeCost time_cost;
    matrixobjectstore_delete_options delete_options = matrixobjectstore_default_delete_options();
    SetCondition(options.condition, &delete_options.condition_);
    delete_options.permanent_delete_ = true;
    delete_options.timeout_in_ms_ = ctrl->timeout_ms();
    matrixobjectstore_message message;
    bool success = MATRIXOBJECTSTORE_DELETE(uri.c_str(), &delete_options, &message);
    if (!success) {
        LOG_ERROR("Delete blob failed")
            .put("Uri", uri)
            .put("Status", MatrixObjectStoreMessageToStatus(message));
        ctrl->set_status(MatrixObjectStoreMessageToStatus(message));
        return;
    }
    LOG_INFO("Delete blob success").put("Uri", uri);
    ctrl->set_status(Status::OK());
}

void MatrixObjectStoreImpl::Freeze(Controller* ctrl, const std::string& uri, const FreezeOptions& options) {
    LOG_CALL_DEBUG().put("Uri", uri);
    TimeCost time_cost;
    matrixobjectstore_open_options open_options = matrixobjectstore_default_open_options();
    SetCondition(options.condition, &open_options.condition_);
    open_options.timeout_in_ms_ = ctrl->timeout_ms();
    matrixobjectstore_message message;
    int open_mode = BS_O_RDWR | BS_O_AEXCL;
    matrixobjectstore_blob* bs_blob = MATRIXOBJECTSTORE_OPEN(uri.c_str(), open_mode, &open_options, &message);
    if (bs_blob == nullptr) {
        LOG_ERROR("Open blob failed")
            .put("Uri", uri)
            .put("Status", MatrixObjectStoreMessageToStatus(message));
        ctrl->set_status(MatrixObjectStoreMessageToStatus(message));
        return;
    }

    MATRIXOBJECTSTORE_CLOSE(bs_blob, &message);
    if (message.status_ != STATUS_OK) {
        LOG_WARNING("Close blob failed")
            .put("Uri", uri)
            .put("Status", MatrixObjectStoreMessageToStatus(message));
        // The failure of close does not affect freeze
    }

    LOG_INFO("Freeze blob success").put("Uri", uri);
    ctrl->set_status(Status::OK());
}

void MatrixObjectStoreImpl::Stat(Controller* ctrl, const std::string& uri, const StatOptions& options,
                         BlobStat* stat) {
    TimeCost time_cost;
    matrixobjectstore_stat_options stat_options = matrixobjectstore_default_stat_options();
    stat_options.timeout_in_ms_ = ctrl->timeout_ms();
    matrixobjectstore_stat_t bs_stat;
    matrixobjectstore_message message;
    bool success = MATRIXOBJECTSTORE_STAT(uri.c_str(), &bs_stat, &stat_options, &message);
    if (!success) {
        LOG_ERROR("Stat blob failed")
            .put("Uri", uri)
            .put("Status", MatrixObjectStoreMessageToStatus(message));
        ctrl->set_status(MatrixObjectStoreMessageToStatus(message));
        return;
    }
    stat->size = bs_stat.size_;
    ctrl->set_status(Status::OK());
}

void MatrixObjectStoreImpl::Rename(Controller* ctrl, const std::string& src_uri, const std::string& dst_uri,
                           const RenameOptions& options) {
    LOG_CALL_DEBUG().put("SrcUri", src_uri).put("DstUri", dst_uri);
    matrixobjectstore_rename_options rename_options = matrixobjectstore_default_rename_options();
    SetCondition(options.condition, &rename_options.condition_);
    rename_options.timeout_in_ms_ = ctrl->timeout_ms();
    matrixobjectstore_message message;
    if (!MATRIXOBJECTSTORE_RENAME(src_uri.c_str(), dst_uri.c_str(), &rename_options, &message)) {
        LOG_ERROR("Rename blob failed")
            .put("SrcUri", src_uri)
            .put("DstUri", dst_uri)
            .put("Status", MatrixObjectStoreMessageToStatus(message));
        ctrl->set_status(MatrixObjectStoreMessageToStatus(message));
        return;
    }
    LOG_INFO("Rename blob success").put("SrcUri", src_uri).put("DstUri", dst_uri);
    ctrl->set_status(Status::OK());
}

void MatrixObjectStoreImpl::SetCondition(const Condition& condition,
                                 matrixobjectstore_blob_condition* blob_condition) {
    blob_condition->lock_name_ = condition.name.c_str();
    memcpy(blob_condition->content_, condition.data.data(), Env::kInlineBlobSize);
}

}  // namespace stream
}  // namespace bcache2
