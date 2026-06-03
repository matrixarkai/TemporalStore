// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/assert.h>
#include <byte/include/macros.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <stdint.h>

#include <string>

#include "protocol/storage.pb.h"

namespace bcache2 {
namespace stream {

const uint64_t kBlockSize = 1UL << 17;       // 128KB
const uint64_t kBlockFooterSize = 1UL << 7;  // 128B
const uint64_t kMagic = 0xBCBCBCBC66666666UL;
const uint32_t kVersion = 1UL;

enum class BlobType {
    kTmpBlob,
    kDataBlob,
};

struct BlobInfo {
    BlobType type = BlobType::kTmpBlob;
    uint64_t blob_id = 0;
    std::string name;

    BlobInfo() {}
    BlobInfo(BlobType type, uint64_t blob_id) : type(type), blob_id(blob_id) {}

    bool operator<(const BlobInfo& other) const { return blob_id < other.blob_id; }
};

// DON'T change the structure
struct ProtoHeader {
    uint32_t proto_size = 0;
    uint32_t proto_crc = 0;
};

bool BlobNameToInfo(const std::string& name, BlobInfo* info);

std::string BlobInfoToName(const BlobInfo& blob);

inline size_t RecordHeaderLength(uint32_t length) {
    return google::protobuf::io::CodedOutputStream::VarintSize32(length) + sizeof(uint32_t);
}

inline bool WriteRecordHeader(uint32_t length, uint32_t crc32c, char* buf, size_t size,
                              uint32_t* consumed_size) {
    *consumed_size = RecordHeaderLength(length);
    if (size < *consumed_size) {
        return false;
    }
    uint8_t* proto_buf = reinterpret_cast<uint8_t*>(buf);
    proto_buf = google::protobuf::io::CodedOutputStream::WriteVarint32ToArray(length, proto_buf);
    proto_buf =
        google::protobuf::io::CodedOutputStream::WriteLittleEndian32ToArray(crc32c, proto_buf);
    BYTE_ASSERT(proto_buf - reinterpret_cast<uint8_t*>(buf) == *consumed_size)
        << length << " " << crc32c << " " << size << " "
        << (proto_buf - reinterpret_cast<uint8_t*>(buf)) << " " << *consumed_size;
    return true;
}

inline bool ReadRecordHeader(const char* buf, size_t size, uint32_t* length, uint32_t* crc32c,
                             uint32_t* consumed_size) {
    google::protobuf::io::ArrayInputStream input(buf, size);
    google::protobuf::io::CodedInputStream stream(&input);
    if (!stream.ReadVarint32(length) || !stream.ReadLittleEndian32(crc32c)) {
        return false;
    }
    *consumed_size = stream.CurrentPosition();
    BYTE_ASSERT(*consumed_size == RecordHeaderLength(*length))
        << size << " " << *length << " " << *crc32c << " " << *consumed_size << " "
        << RecordHeaderLength(*length);
    return true;
}

inline size_t UpperAlign(size_t size, size_t unit) { return (size + unit - 1) / unit * unit; }

inline size_t LowerAlign(size_t size, size_t unit) { return size / unit * unit; }

bool GetBlockFooter(const char* buf, storage::BlockFooter* footer);

bool CheckBlobHeader(const storage::BlobHeader& before_header,
                     const storage::BlobHeader& after_header);

bool CheckBlobInfo(const storage::BlobInfo& before_info, const storage::BlobInfo& after_info);

inline size_t CalcBlobHeaderSize(size_t proto_header_size, size_t start_offset) {
    size_t left_size = (kBlockSize - start_offset % kBlockSize) % kBlockSize;
    return UpperAlign(proto_header_size + sizeof(ProtoHeader) + left_size, kBlockSize) - left_size;
}

storage::BlobInfo FillBlobInfo(const storage::BlobHeader& blob_header, bool frozen);

bool SerializeBlockFooter(const storage::BlockFooter& footer, char* buf, size_t size);

}  // namespace stream
}  // namespace bcache2
