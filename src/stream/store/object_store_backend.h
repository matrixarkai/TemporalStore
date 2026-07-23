#pragma once

#include <absl/strings/match.h>

#include <string>

namespace bcache2 {
namespace stream {

enum class ObjectStoreBackend {
    kByteStore,
    kS3,
    kCephS3,
    kCephRados,
    kSharedFile,
    kLocalFile,
    kUnknown,
};

inline ObjectStoreBackend DetectObjectStoreBackend(const std::string& uri) {
    if (absl::StartsWith(uri, "blob://") || absl::StartsWith(uri, "local://")) {
        return ObjectStoreBackend::kByteStore;
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
    case ObjectStoreBackend::kByteStore:
        return "ByteStore";
    case ObjectStoreBackend::kS3:
        return "S3";
    case ObjectStoreBackend::kCephS3:
        return "CephS3";
    case ObjectStoreBackend::kCephRados:
        return "CephRados";
    case ObjectStoreBackend::kSharedFile:
        return "SharedFile";
    case ObjectStoreBackend::kLocalFile:
        return "LocalFile";
    case ObjectStoreBackend::kUnknown:
        return "Unknown";
    }
    return "Unknown";
}

}  // namespace stream
}  // namespace bcache2
