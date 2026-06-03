// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/strings/string_view.h>

#include <string>

#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "protocol/feature_module.pb.h"

namespace bcache2 {
namespace model {

template <typename T>
inline size_t DataSize(const T& data);

template <>
inline size_t DataSize<uint32_t>(const uint32_t& data) {
    return 0;
}

template <>
inline size_t DataSize<uint64_t>(const uint64_t& data) {
    return 0;
}

template <>
inline size_t DataSize<std::string>(const std::string& data) {
    return data.length();
}

template <typename T>
inline bool ReadData(google::protobuf::io::CodedInputStream* stream, T* data);

template <>
inline bool ReadData<uint32_t>(google::protobuf::io::CodedInputStream* stream, uint32_t* data) {
    return stream->ReadVarint32(data);
}

template <>
inline bool ReadData<uint64_t>(google::protobuf::io::CodedInputStream* stream, uint64_t* data) {
    return stream->ReadVarint64(data);
}

template <>
inline bool ReadData<std::string>(google::protobuf::io::CodedInputStream* stream,
                                  std::string* data) {
    uint32_t size = 0;
    if (!stream->ReadVarint32(&size)) {
        return false;
    }
    return stream->ReadString(data, size);
}

template <>
inline bool ReadData<feature::Point>(google::protobuf::io::CodedInputStream* stream,
                                     feature::Point* data) {
    uint64_t ts = 0;
    uint32_t slen = 0;
    std::string value;
    if (!stream->ReadVarint64(&ts)) return false;
    if (!stream->ReadVarint32(&slen)) return false;
    value.resize(slen);
    if (!stream->ReadString(&value, slen)) return false;
    data->set_ts(ts);
    data->set_value(value);
    return true;
}

template <typename T>
inline void WriteData(google::protobuf::io::CodedOutputStream* output, const T& data);

template <>
inline void WriteData<uint32_t>(google::protobuf::io::CodedOutputStream* output,
                                const uint32_t& data) {
    output->WriteVarint32(data);
}

template <>
inline void WriteData<uint64_t>(google::protobuf::io::CodedOutputStream* output,
                                const uint64_t& data) {
    output->WriteVarint64(data);
}

template <>
inline void WriteData<int>(google::protobuf::io::CodedOutputStream* output, const int& data) {
    output->WriteVarint32SignExtended(data);
}

template <>
inline void WriteData<std::string>(google::protobuf::io::CodedOutputStream* output,
                                   const std::string& data) {
    output->WriteVarint32(data.size());
    output->WriteString(data);
}

template <>
inline void WriteData<feature::Point>(google::protobuf::io::CodedOutputStream* output,
                                      const feature::Point& data) {
    output->WriteVarint64(data.ts());
    output->WriteVarint32(data.value().size());
    output->WriteString(data.value());
}

template <typename T>
inline std::string SerializeToString(const T& value) {
    std::string buffer;
    google::protobuf::io::StringOutputStream output(&buffer);
    google::protobuf::io::CodedOutputStream stream(&output);
    WriteData<T>(&stream, value);
    stream.Trim();
    return buffer;
}

template <typename T>
inline bool ParseFromString(const std::string& buffer, T* value) {
    google::protobuf::io::ArrayInputStream input(buffer.data(), buffer.size());
    google::protobuf::io::CodedInputStream stream(&input);
    return ReadData<T>(&stream, value);
}

inline bool ReadKvItemFromStream(google::protobuf::io::CodedInputStream* input, std::string* key,
                                 std::string* value, uint8_t* cluster_id, bool* deleted,
                                 uint64_t* timestamp) {
    uint32_t size = 0;
    if (!input->ReadVarint32(&size)) {
        return false;
    }
    key->resize(size);
    if (!input->ReadRaw(&((*key)[0]), size)) {
        return false;
    }
    if (!input->ReadVarint32(&size)) {
        return false;
    }
    value->resize(size);
    if (!input->ReadRaw(&(*value)[0], size)) {
        return false;
    }
    uint32_t number = 0;
    if (!input->ReadVarint32(&number)) {
        return false;
    }
    *cluster_id = number;
    if (!input->ReadVarint32(&number)) {
        return false;
    }
    *deleted = number;
    uint64_t number64 = 0;
    if (!input->ReadVarint64(&number64)) {
        return false;
    }
    *timestamp = number64;
    return true;
}

inline void WriteKvItemToStream(google::protobuf::io::CodedOutputStream* output,
                                absl::string_view key, absl::string_view value, uint8_t cluster_id,
                                bool deleted, uint64_t timestamp) {
    output->WriteVarint32(key.size());
    output->WriteRaw(key.data(), key.size());
    output->WriteVarint32(value.size());
    output->WriteRaw(value.data(), value.size());
    output->WriteVarint32(cluster_id);
    output->WriteVarint32(deleted);
    output->WriteVarint64(timestamp);
}

}  // namespace model
}  // namespace bcache2
