// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <absl/memory/memory.h>
#include <byte/include/assert.h>
#include <byte/include/byte_log.h>
#include <google/protobuf/compiler/importer.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/util/json_util.h>

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/module_config.h"
#include "common/status.h"

namespace bcache2 {

class MemorySourceTree : public google::protobuf::compiler::SourceTree {
 public:
    void RegisterProto(const std::string& filename, const std::string& content) {
        protos_[filename] = content;
    }

    google::protobuf::io::ZeroCopyInputStream* Open(const std::string& filename) override {
        if (protos_.find(filename) == protos_.end()) {
            last_error_ = "proto not found";
            return nullptr;
        }
        std::string& proto = protos_[filename];
        auto is = new google::protobuf::io::ArrayInputStream(proto.c_str(), proto.size());
        last_error_ = "";
        return is;
    }

    std::string GetLastErrorMessage() override { return last_error_; }

 private:
    std::map<std::string, std::string> protos_;
    std::string last_error_;
};

class MemoryMultiFileErrorCollector : public google::protobuf::compiler::MultiFileErrorCollector {
 public:
    void AddError(const std::string& filename, int line, int column,
                  const std::string& message) override {
        LOG(ERROR) << "filename: " << filename << " line: " << line << " column: " << column
                   << " msg: " << message << std::endl;
    }

    void AddWarning(const std::string& filename, int line, int column,
                    const std::string& message) override {
        LOG(WARNING) << "filename: " << filename << " line: " << line << " column: " << column
                     << " msg: " << message << std::endl;
    }
};

static const char builtin_proto[] = R"(
syntax = "proto3";

package builtin;

message Pair {
    int64 v1 = 1;
    int64 v2 = 2;
}

message List {
    repeated int64 v = 1;
}

)";

static const char user_proto_head[] = R"(
syntax = "proto3";

package user;

)";

static const char kDefaultMessageName[] = "LongSeq";

class SchemaContainer {
 public:
    // load user-defined Proto message types
    bool Init(const std::string& user_proto, const std::vector<std::string>& message_names) {
        auto user_proto_full = user_proto_head + user_proto;
        source_tree_.RegisterProto("builtin", builtin_proto);
        source_tree_.RegisterProto("user", user_proto_full);
        importer_.reset(new google::protobuf::compiler::Importer(&source_tree_, &err_collector_));
        auto builtin_fd = importer_->Import("builtin");  // if syntax error, will return nullptr
        BYTE_ASSERT(builtin_fd != nullptr);
        auto user_fd = importer_->Import("user");
        if (user_fd == nullptr) {
            LOG(ERROR) << "proto syntax illegal";
            return false;
        }

        auto pair_pd = builtin_fd->pool()->FindMessageTypeByName("builtin.Pair");
        BYTE_ASSERT(pair_pd != nullptr);
        auto list_pd = builtin_fd->pool()->FindMessageTypeByName("builtin.List");
        BYTE_ASSERT(list_pd != nullptr);
        prototypes_builtin_.emplace(std::string("Pair"), message_factory_.GetPrototype(pair_pd));
        prototypes_builtin_.emplace(std::string("List"), message_factory_.GetPrototype(list_pd));

        for (auto& name : message_names) {
            auto user_pd = user_fd->pool()->FindMessageTypeByName("user." + name);
            if (user_pd == nullptr) {
                LOG(ERROR) << "message name " << name << " not found in proto";
                return false;
            }
            prototypes_user_.emplace(name, message_factory_.GetPrototype(user_pd));
        }
        return true;
    }

    const google::protobuf::Message* FindUserPrototype(const std::string& message_name) {
        if (prototypes_user_.find(message_name) == prototypes_user_.end()) {
            return nullptr;
        }
        return prototypes_user_[message_name];
    }

    const google::protobuf::Message* FindBuiltinPrototype(const std::string& message_name) {
        if (prototypes_builtin_.find(message_name) == prototypes_builtin_.end()) {
            return nullptr;
        }
        return prototypes_builtin_[message_name];
    }

 private:
    MemorySourceTree source_tree_;
    MemoryMultiFileErrorCollector err_collector_;
    std::unique_ptr<google::protobuf::compiler::Importer> importer_;
    google::protobuf::DynamicMessageFactory message_factory_;
    std::map<std::string, const google::protobuf::Message*> prototypes_builtin_;
    std::map<std::string, const google::protobuf::Message*> prototypes_user_;
};

class Schema : public ModuleCustomConfig {
 public:
    Schema() { BYTE_ASSERT(ReloadProto("message dummy{}", {})); }
    ~Schema() {}
    Status LoadCustomConfig(const CustomConfig& config) {
        InitDefault();
        return Status::OK();
    }

    Status UpdateCustomConfig(const CustomConfig& config) {
        InitDefault();
        return Status::OK();
    }

    void InitDefault() {
        const std::string user_proto = R"(
message LongSeq {
    uint64 gid = 1;
    uint32 action_type = 2;
    uint32 duration = 3;
    uint64 author_id = 4;
}
        )";
        BYTE_ASSERT(ReloadProto(user_proto, {kDefaultMessageName}));
    }

    bool ReloadProto(const std::string& user_proto, const std::vector<std::string>& message_names) {
        SchemaContainer* new_container = new SchemaContainer;
        if (!new_container->Init(user_proto, message_names)) {
            LOG(ERROR) << "reload proto failed";
            delete new_container;
            return false;
        }
        container_.reset(new_container);
        return true;
    }
    // its usage is like schema.Of("XXXType");
    const google::protobuf::Message* Of(const std::string& message_name) {
        return container_->FindUserPrototype(message_name);
    }

    const google::protobuf::Message* Of() { return Of(kDefaultMessageName); }

    const google::protobuf::Message* List() { return container_->FindBuiltinPrototype("List"); }

    const google::protobuf::Message* Pair() { return container_->FindBuiltinPrototype("Pair"); }

 private:
    std::unique_ptr<SchemaContainer> container_;
};

class InstanceWrapper {
 public:
    explicit InstanceWrapper(google::protobuf::Message* instance, bool owner = false)
        : instance_(instance), owner_(owner) {}

    ~InstanceWrapper() {
        if (owner_ && instance_) {
            delete instance_;
        }
    }

    google::protobuf::Message* Instance() { return instance_; }
    // Protobuf reflection
    void SetUInt32(const std::string& key, const uint32_t value) {
        auto ref = instance_->GetReflection();
        auto pd = instance_->GetDescriptor();
        ref->SetUInt32(instance_, pd->FindFieldByName(key), value);
    }

    uint32_t GetUInt32(const std::string& key) {
        auto ref = instance_->GetReflection();
        auto pd = instance_->GetDescriptor();
        return ref->GetUInt32(*instance_, pd->FindFieldByName(key));
    }

    void SetUInt64(const std::string& key, const uint64_t value) {
        auto ref = instance_->GetReflection();
        auto pd = instance_->GetDescriptor();
        ref->SetUInt64(instance_, pd->FindFieldByName(key), value);
    }

    uint64_t GetUInt64(const std::string& key) {
        auto ref = instance_->GetReflection();
        auto pd = instance_->GetDescriptor();
        return ref->GetUInt64(*instance_, pd->FindFieldByName(key));
    }

    void SetString(const std::string& key, const std::string& value) {
        auto ref = instance_->GetReflection();
        auto pd = instance_->GetDescriptor();
        ref->SetString(instance_, pd->FindFieldByName(key), value);
    }

    std::string GetString(const std::string& key) {
        auto ref = instance_->GetReflection();
        auto pd = instance_->GetDescriptor();
        return ref->GetString(*instance_, pd->FindFieldByName(key));
    }

    size_t Size() { return instance_->ByteSize(); }

    bool FromJson(const std::string& json) {
        google::protobuf::util::JsonParseOptions options;
        options.ignore_unknown_fields = true;
        auto s = google::protobuf::util::JsonStringToMessage(json, instance_, options);
        return s.ok();
    }

    std::string AsJson() {
        google::protobuf::util::JsonPrintOptions options;
        options.add_whitespace = true;
        options.always_print_primitive_fields = true;
#if GOOGLE_PROTOBUF_VERSION >= 3003000
        options.preserve_proto_field_names = true;
#endif
        std::string output;
        google::protobuf::util::MessageToJsonString(*instance_, &output, options);
        return output;
    }

    std::string Serialize() { return instance_->SerializeAsString(); }

 private:
    google::protobuf::Message* instance_;
    bool owner_;
};

class InstanceWriter {
 public:
    explicit InstanceWriter(const google::protobuf::Message* prototype) {
        instance_.reset(prototype->New());
    }

    InstanceWrapper Instance() { return InstanceWrapper(instance_.get()); }

 private:
    std::unique_ptr<google::protobuf::Message> instance_;
};

class InstanceReader {
 public:
    explicit InstanceReader(const google::protobuf::Message* prototype) : prototype_(prototype) {}

    std::unique_ptr<InstanceWrapper> Decode(const std::string& content) {
        auto instance = prototype_->New();
        auto ok = instance->ParseFromString(content);
        if (!ok) {
            delete instance;
            return nullptr;
        }
        return absl::make_unique<InstanceWrapper>(instance, true);
    }

 private:
    const google::protobuf::Message* prototype_;
};

}  // namespace bcache2
