#pragma once

#include <absl/strings/match.h>

#include <string>

namespace bcache2 {
namespace stream {

enum class ObjectStoreBackend {
    kMatrixObjectStore,
    kS3,
    kCephS3,
    kCephRados,
    kSharedFile,
    kLocalFile,
    kUnknown,
};

struct ObjectStoreBackendCapabilities {
    const char* backend = "unknown";
    const char* uri_scheme = "unknown";
    bool runtime_linked = false;
    bool operations_fail_closed = true;
    bool condition_metadata = false;
    bool prefix_list = false;
    bool metadata_stat = false;
    bool append_write = false;
    bool byte_range_read = false;
    bool delete_object = false;
    bool copy_or_rename = false;
    bool split_services = false;
    bool s3_compatible = false;
    bool local_file_compatible = false;
};

inline ObjectStoreBackend DetectObjectStoreBackend(const std::string& uri) {
    if (absl::StartsWith(uri, "matrixobject://") ||
        absl::StartsWith(uri, "matrixobjectstore://") ||
        absl::StartsWith(uri, "blob://") || absl::StartsWith(uri, "local://")) {
        return ObjectStoreBackend::kMatrixObjectStore;
    }
    if (absl::StartsWith(uri, "s3://")) {
        return ObjectStoreBackend::kS3;
    }
    // Ceph RGW is S3-compatible. Keep ceph:// as a shorter alias and ceph+s3:// as explicit.
    if (absl::StartsWith(uri, "ceph://") || absl::StartsWith(uri, "ceph+s3://")) {
        return ObjectStoreBackend::kCephS3;
    }
    // Reserved for a future native librados adapter.
    if (absl::StartsWith(uri, "rados://")) {
        return ObjectStoreBackend::kCephRados;
    }
    if (absl::StartsWith(uri, "shared-file://") || absl::StartsWith(uri, "shared://") ||
        absl::StartsWith(uri, "efs://") || absl::StartsWith(uri, "nfs://")) {
        return ObjectStoreBackend::kSharedFile;
    }
    if (absl::StartsWith(uri, "file://")) {
        return ObjectStoreBackend::kLocalFile;
    }
    return ObjectStoreBackend::kUnknown;
}

inline const char* ObjectStoreBackendName(ObjectStoreBackend backend) {
    switch (backend) {
    case ObjectStoreBackend::kMatrixObjectStore:
        return "matrixobject";
    case ObjectStoreBackend::kS3:
        return "s3";
    case ObjectStoreBackend::kCephS3:
        return "ceph_s3";
    case ObjectStoreBackend::kCephRados:
        return "ceph_rados";
    case ObjectStoreBackend::kSharedFile:
        return "shared_file";
    case ObjectStoreBackend::kLocalFile:
        return "local_file";
    case ObjectStoreBackend::kUnknown:
        return "unknown";
    }
    return "unknown";
}

inline bool ObjectStoreBackendRuntimeLinked(ObjectStoreBackend backend) {
    switch (backend) {
    case ObjectStoreBackend::kMatrixObjectStore:
    case ObjectStoreBackend::kSharedFile:
    case ObjectStoreBackend::kLocalFile:
        return true;
    case ObjectStoreBackend::kS3:
    case ObjectStoreBackend::kCephS3:
#ifdef BCACHE2_ENABLE_S3_STORE
        return true;
#else
        return false;
#endif
    case ObjectStoreBackend::kCephRados:
    case ObjectStoreBackend::kUnknown:
        return false;
    }
    return false;
}

inline const char* ObjectStoreBackendUriScheme(ObjectStoreBackend backend) {
    switch (backend) {
    case ObjectStoreBackend::kMatrixObjectStore:
        return "matrixobject";
    case ObjectStoreBackend::kS3:
        return "s3";
    case ObjectStoreBackend::kCephS3:
        return "ceph+s3";
    case ObjectStoreBackend::kCephRados:
        return "rados";
    case ObjectStoreBackend::kSharedFile:
        return "shared-file";
    case ObjectStoreBackend::kLocalFile:
        return "file";
    case ObjectStoreBackend::kUnknown:
        return "unknown";
    }
    return "unknown";
}

inline ObjectStoreBackendCapabilities ObjectStoreBackendCapabilityReport(ObjectStoreBackend backend) {
    const bool runtime_linked = ObjectStoreBackendRuntimeLinked(backend);
    const bool file_like =
        backend == ObjectStoreBackend::kLocalFile || backend == ObjectStoreBackend::kSharedFile;
    const bool matrixobject = backend == ObjectStoreBackend::kMatrixObjectStore;
    const bool s3_compatible =
        backend == ObjectStoreBackend::kS3 || backend == ObjectStoreBackend::kCephS3;
    const bool native_rados = backend == ObjectStoreBackend::kCephRados;
    const bool object_api = matrixobject || file_like || s3_compatible || native_rados;
    return ObjectStoreBackendCapabilities{
        ObjectStoreBackendName(backend),
        ObjectStoreBackendUriScheme(backend),
        runtime_linked,
        object_api && !runtime_linked,
        object_api,
        object_api,
        object_api,
        object_api,
        object_api,
        object_api,
        object_api,
        matrixobject,
        s3_compatible,
        file_like,
    };
}

}  // namespace stream
}  // namespace bcache2
