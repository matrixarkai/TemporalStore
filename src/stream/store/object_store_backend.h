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

    // Canonical generic object-store capability names shared with Rust and S3-style adapters.
    bool atomic_publish = false;
    bool unique_put = false;
    bool conditional_create = false;
    bool direct_upload_from_path = false;
    bool direct_download_to_path = false;
    bool metadata_head = false;
    bool prefix_list = false;
    bool paginated_list = false;
    bool delete_capability = false;
    bool bulk_delete = false;
    bool object_copy = false;
    bool prefix_delete = false;
    bool byte_range_read = false;
    bool checksum_sha256 = false;
    bool opaque_object_validators = false;
    bool object_version_ids = false;
    bool split_services = false;
    bool s3_compatible = false;
    bool local_file_compatible = false;

    // Compatibility aliases for older C++ call sites. New public reports should prefer
    // the canonical fields above.
    bool condition_metadata = false;
    bool metadata_stat = false;
    bool append_write = false;
    bool delete_object = false;
    bool copy_or_rename = false;
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
    ObjectStoreBackendCapabilities report;
    report.backend = ObjectStoreBackendName(backend);
    report.uri_scheme = ObjectStoreBackendUriScheme(backend);
    report.runtime_linked = runtime_linked;
    report.operations_fail_closed = object_api && !runtime_linked;
    report.atomic_publish = object_api;
    report.unique_put = object_api;
    report.conditional_create = object_api;
    report.direct_upload_from_path = object_api;
    report.direct_download_to_path = object_api;
    report.metadata_head = object_api;
    report.prefix_list = object_api;
    report.paginated_list = object_api;
    report.delete_capability = object_api;
    report.bulk_delete = object_api;
    report.object_copy = object_api;
    report.prefix_delete = object_api;
    report.byte_range_read = object_api;
    report.checksum_sha256 = object_api;
    report.opaque_object_validators = object_api;
    report.object_version_ids = matrixobject || s3_compatible;
    report.split_services = matrixobject;
    report.s3_compatible = s3_compatible;
    report.local_file_compatible = file_like;

    report.condition_metadata = report.conditional_create;
    report.metadata_stat = report.metadata_head;
    report.append_write = report.unique_put;
    report.delete_object = report.delete_capability;
    report.copy_or_rename = report.object_copy;
    return report;
}

}  // namespace stream
}  // namespace bcache2
