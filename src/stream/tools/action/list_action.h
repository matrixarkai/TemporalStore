// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <string>
#include <vector>

#include "stream/log_based_util.h"
#include "stream/store_layer.h"
#include "stream/tools/action/action.h"

namespace bcache2 {
namespace stream {
namespace tool {

class ListAction : public Action {
 public:
    ListAction(StoreLayer* store_layear, const std::string& uri)
        : store_(store_layear), uri_(uri) {}
    ~ListAction() {}

    Status Run() override {
        Controller ctrl;
        std::vector<Store::BlobInfo> blobs;
        store_->List(&ctrl, uri_, &blobs);
        if (!ctrl.status().ok()) {
            return Status::Internal("List failed: " + ctrl.status().ToString());
        }

        // data blob first
        for (auto& blob : blobs) {
            BlobInfo blob_info;
            if (!BlobNameToInfo(blob.name, &blob_info)) {
                return Status::Internal("Invalid blob name " + blob.name);
            }
            if (blob_info.type == BlobType::kDataBlob) {
                printf("%s%s\n", uri_.c_str(), blob.name.c_str());
            }
        }
        for (auto& blob : blobs) {
            BlobInfo blob_info;
            if (!BlobNameToInfo(blob.name, &blob_info)) {
                return Status::Internal("Invalid blob name " + blob.name);
            }
            if (blob_info.type == BlobType::kTmpBlob) {
                printf("%s%s\n", uri_.c_str(), blob.name.c_str());
            }
        }

        return Status::OK();
    }

 private:
    StoreLayer* store_ = nullptr;
    std::string uri_;
};

}  // namespace tool
}  // namespace stream
}  // namespace bcache2
