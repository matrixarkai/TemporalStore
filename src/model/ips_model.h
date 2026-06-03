// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <string>
#include <utility>
#include <vector>

#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "extension/ips/interface.pb.h"
#include "model/orset_model.h"

namespace bcache2 {
namespace model {

// NOTE: Keep semantics same with BCache1 OrderedTree before steady stage.
class IpsOrSet {
 public:
    explicit IpsOrSet(PersistentMap<std::string, std::string>* data);

    Status OnLoaded() { return Status::OK(); }

    Status OnChange(std::string key, std::string) { return Status::OK(); }

    Status Set(partition::CmdContext* ctx, std::string key, std::string value) {
        data_->Put(ctx, key, value);
        return Status::OK();
    }

    Status Get(std::string key, std::string* value) {
        auto iter = data_->Find(key);
        if (iter == data_->End()) {
            return Status::NotFound("key not exist");
        }
        *value = iter.Second();
        return Status::OK();
    }

    Status Query(uint64_t start, uint64_t end,
                 std::vector<std::pair<std::string, std::string>>* result) {
        return Status::OK();
    }

    Status GetMaxItem(std::string* key, std::string* value) {
        if (data_->Size() == 0) {
            return Status::NotFound("empty persistent map");
        }

        PersistentMap<std::string, std::string>::IterateFunc iter_wrapper =
            [&key, &value](const std::string& key_str, const std::string& value_str) -> bool {
            BYTE_ASSERT(!value_str.empty());
            *key = key_str;
            *value = value_str;
            return false;
        };
        Status ret = ScanBackward("", iter_wrapper);
        if (!ret.IsOK()) {
            return ret;
        }
        return Status::OK();
    }

    Status Del(partition::CmdContext* ctx, const std::string& key) {
        data_->Del(ctx, key);
        return Status::OK();
    }

    Status UpdateUserMeta(partition::CmdContext* ctx, const std::string& value) {
        // data_->Put(ctx, meta_key, value);
        user_meta_str = value;
        return Status::OK();
    }

    Status GetUserMeta(std::string* value) {
        if (user_meta_str.empty()) return Status::NotFound("no user data");
        *value = user_meta_str;
        return Status::OK();
    }

    Status DeleteTree() { return Status::OK(); }

    Status Size(uint64_t* size) {
        *size = data_->Size();
        return Status::OK();
    }

    void DelAll(partition::CmdContext* ctx) {
        while (data_->Size() > 0) {
            data_->DelBegin(ctx);
        }
    }

    // [start, +inf)
    // start = "" => -inf
    Status Scan(const std::string& start_key,
                PersistentMap<std::string, std::string>::IterateFunc iter_func) {
        if (data_->Size() == 0) {
            return Status::NotFound("empty persistent map");
        }

        bool found_valid = false;
        PersistentMap<std::string, std::string>::IterateFunc iter_wrapper =
            [&iter_func, &found_valid](const std::string& key_str,
                                       const std::string& value_str) -> bool {
            if (value_str.empty()) {
                // TODO(wy): Need to dig the reason, keep the same with OrderedTree first
                return true;
            }
            found_valid = true;
            return iter_func(key_str, value_str);
        };

        Status ret = Status::OK();
        if (!start_key.empty()) {
            ret = data_->Scan(start_key, iter_wrapper);
        } else {
            ret = data_->Scan(data_->Begin().First(), iter_wrapper);
        }
        if (!ret.IsOK()) {
            return ret;
        }
        if (!found_valid) {
            return Status::NotFound("");
        }
        return Status::OK();
    }

    // (-inf, start]
    // start = "" => +inf
    Status ScanBackward(const std::string& start_key,
                        PersistentMap<std::string, std::string>::IterateFunc iter_func) {
        if (data_->Size() == 0) {
            return Status::NotFound("empty persistent map");
        }

        bool found_valid = false;
        PersistentMap<std::string, std::string>::IterateFunc iter_wrapper =
            [&iter_func, &found_valid](const std::string& key_str,
                                       const std::string& value_str) -> bool {
            if (value_str.empty()) {
                return true;
            }
            found_valid = true;
            return iter_func(key_str, value_str);
        };

        Status ret = Status::OK();
        if (!start_key.empty()) {
            ret = data_->ScanBackward(start_key, iter_wrapper);
        } else {
            ret = data_->ScanBackward((--data_->End()).First(), iter_wrapper);
        }
        if (!ret.IsOK()) {
            return ret;
        }
        if (!found_valid) {
            return Status::NotFound("empty persistent map");
        }
        return Status::OK();
    }

    PersistentMap<std::string, std::string>* data_;

 private:
    // constexpr static const char* meta_key = "meta";
    std::string user_meta_str = "";
};

using IpsModel = OrSetModel<std::string, std::string, IpsOrSet>;

}  // namespace model
}  // namespace bcache2
