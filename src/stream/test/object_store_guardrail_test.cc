#include <cstdlib>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "common/controller.h"
#include "stream/store/object_store_backend.h"
#include "stream/store_layer.h"

namespace bcache2 {
namespace stream {
namespace {

Store::ConditionData BuildConditionData() {
    Store::ConditionData data;
    data.fill('\0');
    data[0] = 'x';
    return data;
}

void ExpectUnimplemented(const std::string& uri, const Status& status) {
    EXPECT_TRUE(status.IsUnimplemented()) << uri << ": " << status.ToString();
    EXPECT_NE(status.ToString().find("object-store backend is not linked"), std::string::npos)
        << status.ToString();
}

void ExpectUnsupportedBackendFailsClosed(const std::string& uri) {
    StoreLayer store_layer(nullptr);

    {
        Controller ctrl;
        Store::BlobStat stat;
        store_layer.Stat(&ctrl, uri, Store::StatOptions(), &stat);
        ExpectUnimplemented(uri, ctrl.status());
    }
    {
        Controller ctrl;
        std::vector<Store::BlobInfo> blobs;
        store_layer.List(&ctrl, uri, &blobs);
        ExpectUnimplemented(uri, ctrl.status());
    }
    {
        Controller ctrl;
        Blob* blob = reinterpret_cast<Blob*>(0x1);
        store_layer.Open(&ctrl, uri, Store::OpenOptions(), &blob);
        ExpectUnimplemented(uri, ctrl.status());
        EXPECT_EQ(nullptr, blob);
    }
    {
        Controller ctrl;
        store_layer.Delete(&ctrl, uri, Store::DeleteOptions());
        ExpectUnimplemented(uri, ctrl.status());
    }
    {
        Controller ctrl;
        store_layer.Freeze(&ctrl, uri, Store::FreezeOptions());
        ExpectUnimplemented(uri, ctrl.status());
    }
    {
        Controller ctrl;
        store_layer.Rename(&ctrl, uri, uri + ".renamed", Store::RenameOptions());
        ExpectUnimplemented(uri, ctrl.status());
    }
    {
        Controller ctrl;
        store_layer.SetCondition(&ctrl, uri, BuildConditionData(), Store::SetConditionOptions());
        ExpectUnimplemented(uri, ctrl.status());
    }
    {
        Controller ctrl;
        Store::ConditionData data;
        store_layer.StatCondition(&ctrl, uri, &data);
        ExpectUnimplemented(uri, ctrl.status());
    }
}

#ifdef BCACHE2_ENABLE_S3_STORE
class ScopedUnsetEnv {
 public:
    explicit ScopedUnsetEnv(std::vector<std::string> names) : names_(std::move(names)) {
        for (const auto& name : names_) {
            const char* value = std::getenv(name.c_str());
            values_.push_back(value == nullptr ? "" : value);
            existed_.push_back(value != nullptr);
            unsetenv(name.c_str());
        }
    }

    ~ScopedUnsetEnv() {
        for (size_t i = 0; i < names_.size(); ++i) {
            if (existed_[i]) {
                setenv(names_[i].c_str(), values_[i].c_str(), 1);
            } else {
                unsetenv(names_[i].c_str());
            }
        }
    }

 private:
    std::vector<std::string> names_;
    std::vector<std::string> values_;
    std::vector<bool> existed_;
};
#endif

}  // namespace

TEST(ObjectStoreBackendGuardrailTest, FutureBackendsFailClosedAcrossApis) {
    for (const auto& uri : {
#ifndef BCACHE2_ENABLE_S3_STORE
             "s3://bucket/prefix/blob1",
             "ceph://bucket/prefix/blob1",
             "ceph+s3://bucket/prefix/blob1",
#endif
             "rados://pool/prefix/blob1",
         }) {
        SCOPED_TRACE(uri);
        ExpectUnsupportedBackendFailsClosed(uri);
    }
}

#ifdef BCACHE2_ENABLE_S3_STORE
TEST(ObjectStoreBackendGuardrailTest, S3BackendsRequireEndpointConfiguration) {
    ScopedUnsetEnv unset_s3_env({
        "TEMPORALSTORE_S3_ENDPOINT",
        "AWS_ENDPOINT_URL_S3",
        "S3_ENDPOINT_URL",
        "AWS_ENDPOINT_URL",
        "TEMPORALSTORE_S3_UNSIGNED",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "MINIO_ROOT_USER",
        "MINIO_ROOT_PASSWORD",
    });
    for (const auto& uri : {
             "s3://bucket/prefix/blob1",
             "ceph://bucket/prefix/blob1",
             "ceph+s3://bucket/prefix/blob1",
         }) {
        SCOPED_TRACE(uri);
        StoreLayer store_layer(nullptr);
        Controller ctrl;
        Store::BlobStat stat;
        store_layer.Stat(&ctrl, uri, Store::StatOptions(), &stat);
        EXPECT_TRUE(ctrl.status().IsInvalidArgument() || ctrl.status().IsPermissionDenied())
            << ctrl.status().ToString();
    }
}
#endif

TEST(ObjectStoreBackendGuardrailTest, UnknownBackendsRemainInvalidArguments) {
    StoreLayer store_layer(nullptr);
    Controller ctrl;
    Store::BlobStat stat;
    store_layer.Stat(&ctrl, "http://bucket/prefix/blob1", Store::StatOptions(), &stat);
    EXPECT_TRUE(ctrl.status().IsInvalidArgument()) << ctrl.status().ToString();
}

TEST(ObjectStoreBackendGuardrailTest, PublicBackendNamesAreCanonical) {
    EXPECT_EQ("matrixobject",
              std::string(ObjectStoreBackendName(
                  DetectObjectStoreBackend("matrixobject://bucket/prefix"))));
    EXPECT_EQ("matrixobject",
              std::string(ObjectStoreBackendName(
                  DetectObjectStoreBackend("matrixobjectstore://bucket/prefix"))));
    EXPECT_EQ("s3", std::string(ObjectStoreBackendName(DetectObjectStoreBackend("s3://b/k"))));
    EXPECT_EQ("ceph_s3",
              std::string(ObjectStoreBackendName(DetectObjectStoreBackend("ceph+s3://b/k"))));
    EXPECT_EQ("shared_file",
              std::string(ObjectStoreBackendName(DetectObjectStoreBackend("shared-file://b/k"))));
    EXPECT_EQ("local_file",
              std::string(ObjectStoreBackendName(DetectObjectStoreBackend("file:///tmp/k"))));
    EXPECT_EQ("matrixobject",
              std::string(ObjectStoreBackendUriScheme(ObjectStoreBackend::kMatrixObjectStore)));
    EXPECT_EQ("file", std::string(ObjectStoreBackendUriScheme(ObjectStoreBackend::kLocalFile)));
}

TEST(ObjectStoreBackendGuardrailTest, PublicCapabilityReportsAreProviderNeutral) {
    const auto matrixobject =
        ObjectStoreBackendCapabilityReport(ObjectStoreBackend::kMatrixObjectStore);
    EXPECT_EQ("matrixobject", std::string(matrixobject.backend));
    EXPECT_EQ("matrixobject", std::string(matrixobject.uri_scheme));
    EXPECT_TRUE(matrixobject.runtime_linked);
    EXPECT_FALSE(matrixobject.operations_fail_closed);
    EXPECT_TRUE(matrixobject.condition_metadata);
    EXPECT_TRUE(matrixobject.prefix_list);
    EXPECT_TRUE(matrixobject.metadata_stat);
    EXPECT_TRUE(matrixobject.append_write);
    EXPECT_TRUE(matrixobject.byte_range_read);
    EXPECT_TRUE(matrixobject.delete_object);
    EXPECT_TRUE(matrixobject.copy_or_rename);
    EXPECT_TRUE(matrixobject.split_services);
    EXPECT_FALSE(matrixobject.s3_compatible);

    const auto s3 = ObjectStoreBackendCapabilityReport(ObjectStoreBackend::kS3);
    EXPECT_EQ("s3", std::string(s3.backend));
    EXPECT_EQ("s3", std::string(s3.uri_scheme));
    EXPECT_TRUE(s3.s3_compatible);
#ifdef BCACHE2_ENABLE_S3_STORE
    EXPECT_TRUE(s3.runtime_linked);
    EXPECT_FALSE(s3.operations_fail_closed);
#else
    EXPECT_FALSE(s3.runtime_linked);
    EXPECT_TRUE(s3.operations_fail_closed);
#endif

    const auto ceph = ObjectStoreBackendCapabilityReport(ObjectStoreBackend::kCephS3);
    EXPECT_EQ("ceph_s3", std::string(ceph.backend));
    EXPECT_EQ("ceph+s3", std::string(ceph.uri_scheme));
    EXPECT_TRUE(ceph.s3_compatible);

    const auto rados = ObjectStoreBackendCapabilityReport(ObjectStoreBackend::kCephRados);
    EXPECT_EQ("ceph_rados", std::string(rados.backend));
    EXPECT_EQ("rados", std::string(rados.uri_scheme));
    EXPECT_FALSE(rados.runtime_linked);
    EXPECT_TRUE(rados.operations_fail_closed);

    const auto shared_file = ObjectStoreBackendCapabilityReport(ObjectStoreBackend::kSharedFile);
    EXPECT_TRUE(shared_file.runtime_linked);
    EXPECT_TRUE(shared_file.local_file_compatible);
    EXPECT_FALSE(shared_file.operations_fail_closed);
}

}  // namespace stream
}  // namespace bcache2
