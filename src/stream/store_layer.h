// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/macros.h>
#include <byte/thread/async_thread.h>

#include <memory>
#include <string>
#include <vector>

#include "common/coclosure.h"
#include "stream/store/matrixobjectstore_store.h"
#include "stream/store/local_file_store.h"
#include "stream/store/object_store_backend.h"
#include "stream/store/shared_file_store.h"
#ifdef BCACHE2_ENABLE_S3_STORE
#include "stream/store/s3_store.h"
#endif
#include "stream/store/store.h"
#include "stream/store/unsupported_object_store.h"

// If not in a coroutine, then just invoke the method
// If so, submit this task to the background thread pool, yield control to let other coroutines
// run, and wait synchronously
#define COROUTINE_CALL(method, ...)                        \
    if (!IsCoContext()) {                                  \
        method(__VA_ARGS__);                               \
    } else {                                               \
        CoSyncClosure sync;                                \
        auto func = [&] {                                  \
            method(__VA_ARGS__);                           \
            sync.Run();                                    \
        };                                                 \
        bg_thread_pool_->PushTask(NewCoFuncClosure(func)); \
        sync.Wait();                                       \
    }

#define DISPATCH(method, ...)                                              \
    Store* store_env = GetStoreEnv(uri);                                   \
    if (store_env == nullptr) {                                            \
        LOG_ERROR("Schema not supported").put("Uri", uri);                 \
        ctrl->set_status(Status::InvalidArgument("Schema not supported")); \
        return;                                                            \
    }                                                                      \
    COROUTINE_CALL(store_env->method, __VA_ARGS__);

namespace bcache2 {
namespace stream {

// The highest level of abstraction for storage
// It dispatches IO requests to local files or the object-store compatibility layer.
class StoreLayer {
 public:
    explicit StoreLayer(byte::AsyncThreadPool* bg_thread_pool) : bg_thread_pool_(bg_thread_pool) {
        matrixobjectstore_store_.reset(new MatrixObjectStoreImpl());
#ifdef BCACHE2_ENABLE_S3_STORE
        s3_store_.reset(new S3Store("S3"));
        ceph_s3_store_.reset(new S3Store("CephS3"));
#else
        s3_store_.reset(new UnsupportedObjectStore(
            "S3", "build an S3 Store adapter and route s3:// URIs to it"));
        ceph_s3_store_.reset(new UnsupportedObjectStore(
            "CephS3", "use the S3 adapter against Ceph RGW for ceph:// or ceph+s3:// URIs"));
#endif
        ceph_rados_store_.reset(new UnsupportedObjectStore(
            "CephRados", "build a native librados adapter for rados:// URIs"));
        shared_file_store_.reset(new SharedFileStore());
        local_file_store_.reset(new LocalFileStore());
    }

    // lock-like
    void SetCondition(Controller* ctrl, const std::string& uri, const Env::ConditionData& data,
                      const Store::SetConditionOptions& options) {
        DISPATCH(SetCondition, ctrl, uri, data, options);
    }

    void StatCondition(Controller* ctrl, const std::string& uri, Env::ConditionData* data) {
        DISPATCH(StatCondition, ctrl, uri, data);
    }

    void List(Controller* ctrl, const std::string& uri, std::vector<Store::BlobInfo>* blobs) {
        DISPATCH(List, ctrl, uri, blobs);
    }

    void Open(Controller* ctrl, const std::string& uri, const Store::OpenOptions& options,
              Blob** blob) {
        DISPATCH(Open, ctrl, uri, options, blob);
    }

    void Delete(Controller* ctrl, const std::string& uri, const Store::DeleteOptions& options) {
        DISPATCH(Delete, ctrl, uri, options);
    }

    void Freeze(Controller* ctrl, const std::string& uri, const Store::FreezeOptions& options) {
        DISPATCH(Freeze, ctrl, uri, options);
    }

    void Rename(Controller* ctrl, const std::string& src_uri, const std::string& dst_uri,
                const Store::RenameOptions& options) {
        std::string uri = src_uri;
        DISPATCH(Rename, ctrl, src_uri, dst_uri, options);
    }

    void Stat(Controller* ctrl, const std::string& uri, const Store::StatOptions& options,
              Store::BlobStat* stat) {
        DISPATCH(Stat, ctrl, uri, options, stat);
    }

 private:
    Store* GetStoreEnv(const std::string& uri) {
        switch (DetectObjectStoreBackend(uri)) {
        case ObjectStoreBackend::kMatrixObjectStore:
            return matrixobjectstore_store_.get();
        case ObjectStoreBackend::kS3:
            return s3_store_.get();
        case ObjectStoreBackend::kCephS3:
            return ceph_s3_store_.get();
        case ObjectStoreBackend::kCephRados:
            return ceph_rados_store_.get();
        case ObjectStoreBackend::kSharedFile:
            return shared_file_store_.get();
        case ObjectStoreBackend::kLocalFile:
            return local_file_store_.get();
        case ObjectStoreBackend::kUnknown:
            return nullptr;
        }
        return nullptr;
    }

    byte::AsyncThreadPool* bg_thread_pool_ = nullptr;
    std::unique_ptr<MatrixObjectStoreImpl> matrixobjectstore_store_;
    std::unique_ptr<Store> s3_store_;
    std::unique_ptr<Store> ceph_s3_store_;
    std::unique_ptr<UnsupportedObjectStore> ceph_rados_store_;
    std::unique_ptr<SharedFileStore> shared_file_store_;
    std::unique_ptr<LocalFileStore> local_file_store_;

    DISALLOW_COPY_AND_ASSIGN(StoreLayer);
};

}  // namespace stream
}  // namespace bcache2
