// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <string>

#include "google/protobuf/io/zero_copy_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/message.h"
#include "google/protobuf/util/message_differencer.h"

#include "common/proto_enhance.h"
#include "common/status.h"

namespace bcache2 {
namespace metaserver {

class DeepCopy {
 public:
    virtual ~DeepCopy() {}
    virtual bool Equal(DeepCopy* rhs) = 0;
    virtual void DeepCopyTo(DeepCopy* rhs) = 0;
};

class Serializable {
 public:
    virtual ~Serializable() {}

    virtual std::string GetSerializeTypeName() = 0;
    virtual Status SerializeToString(std::string* ouput) = 0;
    virtual Status ParseFromString(const std::string& data) = 0;

    Status PackToStream(google::protobuf::io::ZeroCopyOutputStream* stream);
    Status UnPackFromStream(google::protobuf::io::ZeroCopyInputStream* stream);
};

#define DEFINE_COMMON_SERIALIZABLE(name)                         \
    std::string GetSerializeTypeName() override { return name; } \
    Status SerializeToString(std::string* output) override {     \
        std::lock_guard<bthread::Mutex> _(mu_);                  \
        if (!info_.SerializeToString(output)) {                  \
            return Status::Internal("Serialize Error");          \
        }                                                        \
        return Status::OK();                                     \
    }                                                            \
    Status ParseFromString(const std::string& input) override {  \
        decltype(info_) info;                                    \
        if (!info.ParseFromString(input)) {                      \
            return Status::Internal("Parse Error");              \
        }                                                        \
        std::lock_guard<bthread::Mutex> _(mu_);                  \
        info_ = info;                                            \
        return Status::OK();                                     \
    }

template <typename T>
bool MapEqual(T lhs, T rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (auto& pair : lhs) {
        if (!pair.second->Equal(rhs[pair.first].get())) {
            return false;
        }
    }
    return true;
}

template <typename T>
bool MapEqual2(T lhs, T rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (auto& pair : lhs) {
        if (pair.second != rhs[pair.first]) {
            return false;
        }
    }
    return true;
}

template <typename T>
bool SetEqual(T lhs, T rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (auto& m : lhs) {
        auto iter = rhs.find(m);
        if (iter == rhs.end()) {
            return false;
        }
        using Type = std::decay_t<decltype(m)>;
        if constexpr (std::is_same_v<Type, std::string>) {
            if (m != *iter) {
                return false;
            }
        } else {
            if (!m->Equal(iter->get())) {
                return false;
            }
        }
    }
    return true;
}

}  // namespace metaserver
}  // namespace bcache2

