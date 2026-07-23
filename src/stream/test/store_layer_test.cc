// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "stream/store_layer.h"

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "test/common/temp_dir.h"

DEFINE_string(schema, "file://", "blob schema");

namespace bcache2 {
namespace stream {

class StoreLayerTest : public testing::Test {
 public:
    void SetUp() override {
        bytestore_set_flag("bytestore_client_log_level", "1");
        bytestore_init();
        base_uri_ = FLAGS_schema + temp_dir_.GetDir() + "/cluster/public";

        metrics_manager_.reset(new MetricsManager({}, "partition"));

        byte::AsyncThreadPoolOptions tp_options;
        tp_options.name_ = "test";
        background_pool_.reset(new byte::AsyncThreadPool());
        ASSERT_TRUE(background_pool_->Init(tp_options));
        ASSERT_TRUE(background_pool_->Start());

        store_layer_.reset(new StoreLayer(background_pool_.get()));

        Controller ctrl;
        store_layer_->SetCondition(&ctrl, base_uri_ + "/cond1", BuildValue("value1"),
                                   Store::SetConditionOptions());
        ASSERT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();

        valid_condition_.name = base_uri_ + "/cond1";
        valid_condition_.data = BuildValue("value1");

        invalid_condition_.name = base_uri_ + "/cond1";
        invalid_condition_.data = BuildValue("value2");

        ctrl.Reset();
        Store::OpenOptions open_options;
        open_options.mode = Store::OpenMode::kWrite;
        open_options.condition = valid_condition_;
        open_options.metrics_manager = metrics_manager_.get();
        Blob* blob = nullptr;
        store_layer_->Open(&ctrl, base_uri_ + "/blob1", open_options, &blob);
        EXPECT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();
        blob_.reset(blob);

        ctrl.Reset();
        blob = nullptr;
        store_layer_->Open(&ctrl, base_uri_ + "/blob2", open_options, &blob);
        EXPECT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();
        delete blob;

        ctrl.Reset();
        std::string data = "Data1";
        SYNC_CALL(blob_->Append, &ctrl, data.data(), data.size());
        ASSERT_TRUE(ctrl.status().ok());
    }
    void TearDown() override { bytestore_shutdown(); }

 protected:
    Env::ConditionData BuildValue(const std::string& data) {
        BYTE_ASSERT(data.size() <= Env::kInlineBlobSize);
        Env::ConditionData value;
        value.fill('\0');
        memcpy(value.data(), data.data(), data.size());
        return value;
    }

    TempDir temp_dir_;
    std::string base_uri_;
    std::unique_ptr<byte::AsyncThreadPool> background_pool_;
    std::unique_ptr<StoreLayer> store_layer_;
    Store::Condition valid_condition_;
    Store::Condition invalid_condition_;
    std::unique_ptr<Blob> blob_;
    std::unique_ptr<MetricsManager> metrics_manager_;
};

TEST_F(StoreLayerTest, InvalidSchema) {
    Controller ctrl;
    store_layer_->Delete(&ctrl, "xxx://xxxx/xx/xx", Store::DeleteOptions());
    EXPECT_TRUE(ctrl.status().IsInvalidArgument());
}

TEST_F(StoreLayerTest, StatCondition) {
    Controller ctrl;
    Env::ConditionData data;
    store_layer_->StatCondition(&ctrl, base_uri_ + "/cond1", &data);
    EXPECT_EQ(BuildValue("value1"), data);
}

TEST_F(StoreLayerTest, ValidCondition) {
    Controller ctrl;
    Store::SetConditionOptions options;
    options.condition.name = base_uri_ + "/cond1";
    options.condition.data = BuildValue("value1");
    store_layer_->SetCondition(&ctrl, base_uri_ + "/cond1", BuildValue("value1"), options);
    ASSERT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();
}

TEST_F(StoreLayerTest, InvalidCondition) {
    {
        Controller ctrl;
        Store::SetConditionOptions options;
        options.condition = invalid_condition_;
        store_layer_->SetCondition(&ctrl, base_uri_ + "/cond1", BuildValue("value1"), options);
        ASSERT_TRUE(ctrl.status().IsStoreConditionFailed()) << ctrl.status().ToString();
    }

    {
        Controller ctrl;
        Store::OpenOptions options;
        options.mode = Store::OpenMode::kWrite;
        options.condition = invalid_condition_;
        options.metrics_manager = metrics_manager_.get();
        Blob* blob = nullptr;
        store_layer_->Open(&ctrl, base_uri_ + "/blob1", options, &blob);
        EXPECT_TRUE(ctrl.status().IsStoreConditionFailed()) << ctrl.status().ToString();
    }

    {
        Controller ctrl;
        Store::DeleteOptions options;
        options.condition = invalid_condition_;
        store_layer_->Delete(&ctrl, base_uri_ + "/blob1", options);
        EXPECT_TRUE(ctrl.status().IsStoreConditionFailed()) << ctrl.status().ToString();
    }

    {
        Controller ctrl;
        Store::FreezeOptions options;
        options.condition = invalid_condition_;
        store_layer_->Freeze(&ctrl, base_uri_ + "/blob1", options);
        EXPECT_TRUE(ctrl.status().IsStoreConditionFailed()) << ctrl.status().ToString();
    }

    {
        Controller ctrl;
        Store::RenameOptions options;
        options.condition = invalid_condition_;
        store_layer_->Rename(&ctrl, base_uri_ + "/blob1", base_uri_ + "/blob2", options);
        EXPECT_TRUE(ctrl.status().IsStoreConditionFailed()) << ctrl.status().ToString();
    }
}

TEST_F(StoreLayerTest, Create) {
    Controller ctrl;
    Store::OpenOptions options;
    options.mode = Store::OpenMode::kWrite;
    options.condition = valid_condition_;
    options.metrics_manager = metrics_manager_.get();
    Blob* blob = nullptr;
    store_layer_->Open(&ctrl, base_uri_ + "/blob11", options, &blob);
    EXPECT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();

    ctrl.Reset();
    std::vector<Store::BlobInfo> blobs;
    store_layer_->List(&ctrl, base_uri_ + "/", &blobs);
    ASSERT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();
    auto it = std::find_if(blobs.begin(), blobs.end(),
                           [](const Store::BlobInfo& blob) { return blob.name == "blob11"; });
    ASSERT_TRUE(it != blobs.end());
}

TEST_F(StoreLayerTest, Delete) {
    Controller ctrl;
    Store::DeleteOptions options;
    options.condition = valid_condition_;
    store_layer_->Delete(&ctrl, base_uri_ + "/blob1", options);
    EXPECT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();

    ctrl.Reset();
    std::vector<Store::BlobInfo> blobs;
    store_layer_->List(&ctrl, base_uri_ + "/", &blobs);
    ASSERT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();
    auto it = std::find_if(blobs.begin(), blobs.end(),
                           [](const Store::BlobInfo& blob) { return blob.name == "blob1"; });
    ASSERT_TRUE(it == blobs.end());
}

TEST_F(StoreLayerTest, Freeze) {
    Controller ctrl;
    Store::FreezeOptions options;
    options.condition = valid_condition_;
    store_layer_->Freeze(&ctrl, base_uri_ + "/blob1", options);
    ASSERT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();

    ctrl.Reset();
    std::string data = "Data2";
    SYNC_CALL(blob_->Append, &ctrl, data.data(), data.size());
    ASSERT_FALSE(ctrl.status().ok());
}

TEST_F(StoreLayerTest, Rename) {
    Controller ctrl;
    Store::RenameOptions options;
    options.condition = valid_condition_;
    store_layer_->Rename(&ctrl, base_uri_ + "/blob1", base_uri_ + "/blob11", options);
    EXPECT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();

    ctrl.Reset();
    Store::StatOptions stat_options;
    Store::BlobStat stat;
    store_layer_->Stat(&ctrl, base_uri_ + "/blob11", stat_options, &stat);
    EXPECT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();
    EXPECT_EQ(5, stat.size);
}

TEST_F(StoreLayerTest, Stat) {
    Controller ctrl;
    Store::StatOptions options;
    Store::BlobStat stat;
    store_layer_->Stat(&ctrl, base_uri_ + "/blob1", options, &stat);
    EXPECT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();
    EXPECT_EQ(5, stat.size);
}

TEST_F(StoreLayerTest, Append) {
    std::string data = "Data2";
    Controller ctrl;
    SYNC_CALL(blob_->Append, &ctrl, data.data(), data.size());
    ASSERT_TRUE(ctrl.status().ok());

    char buf[10];
    ctrl.Reset();
    SYNC_CALL(blob_->Read, &ctrl, 5, buf, 5);
    ASSERT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();
    buf[5] = '\0';

    EXPECT_EQ(buf, data);
}

TEST_F(StoreLayerTest, Read) {
    Controller ctrl;
    char buf[10];
    SYNC_CALL(blob_->Read, &ctrl, 0, buf, 5);
    ASSERT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();
    buf[5] = '\0';

    EXPECT_STREQ("Data1", buf);
}

TEST_F(StoreLayerTest, List) {
    Controller ctrl;
    std::vector<Store::BlobInfo> blobs;
    store_layer_->List(&ctrl, base_uri_ + "/", &blobs);
    ASSERT_TRUE(ctrl.status().ok()) << ctrl.status().ToString();
    sort(blobs.begin(), blobs.end(), [](const Store::BlobInfo& left, const Store::BlobInfo& right) {
        return left.name < right.name;
    });
    ASSERT_EQ(3, blobs.size());
    EXPECT_EQ("blob1", blobs[0].name);
    EXPECT_EQ("blob2", blobs[1].name);
    EXPECT_EQ("cond1", blobs[2].name);
}

}  // namespace stream
}  // namespace bcache2
