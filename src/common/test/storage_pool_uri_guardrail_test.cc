#include <string>

#include "gtest/gtest.h"

#include "common/proto_enhance.h"

namespace bcache2::common::test {
namespace {

TableInfo BuildTableInfoWithStorageUri(const std::string& storage_pool_uri) {
    TableInfo info;
    info.set_name("table1");
    info.set_namespace_name("ns1");
    info.set_partition_set_num(1);
    auto* unit = info.add_partition_units();
    unit->set_partition_num(1);
    auto* loc = unit->add_placement_set();
    loc->set_vregion("vregion");
    loc->set_vdc("vdc1");
    loc->set_vau("vau1");
    unit->set_storage_pool_uri(storage_pool_uri);
    info.mutable_quota()->set_ops_read(1000);
    return info;
}

}  // namespace

TEST(StoragePoolUriGuardrailTest, AllowsOnlyImplementedBackends) {
    for (const auto& uri : {
             "file:///tmp/temporalstore/pool/",
             "shared-file:///mnt/temporalstore/pool/",
             "shared:///mnt/temporalstore/pool/",
             "efs:///mnt/temporalstore/pool/",
             "nfs:///mnt/temporalstore/pool/",
             "local://temporalstore/pool/",
             "blob://temporalstore/pool/",
             "s3://temporalstore-test/pool/",
             "ceph://temporalstore-test/pool/",
             "ceph+s3://temporalstore-test/pool/",
         }) {
        Status status = Validate(BuildTableInfoWithStorageUri(uri));
        ASSERT_TRUE(status.ok()) << uri << ": " << status.ToString();
    }

    for (const auto& uri : {
             "",
             "rados://temporalstore-test/pool/",
             "http://temporalstore-test/pool/",
         }) {
        Status status = Validate(BuildTableInfoWithStorageUri(uri));
        ASSERT_TRUE(status.IsFailedPrecondition()) << uri << ": " << status.ToString();
        ASSERT_NE(status.ToString().find("invalid storage pool uri"), std::string::npos)
            << status.ToString();
    }
}

}  // namespace bcache2::common::test
