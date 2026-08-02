// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "stream/log_based_util.h"

#include <byte/algorithm/crc32.h>
#include <byte/include/assert.h>
#include <byte/string/format.h>
#include <google/protobuf/util/message_differencer.h>

#include "common/logging.h"
#include "common/time.h"

namespace bcache2 {
namespace stream {

bool BlobNameToInfo(const std::string& name, BlobInfo* info) {
    char type[1024];
    char tail = 0;
    int ret = sscanf(name.c_str(), "%3s-%lu%c", type, &info->blob_id, &tail);
    if (ret != 2) {
        return false;
    }
    if (strncmp(type, "TMP", 3) == 0) {
        info->type = BlobType::kTmpBlob;
    } else if (strncmp(type, "DAT", 3) == 0) {
        info->type = BlobType::kDataBlob;
    } else {
        return false;
    }
    info->name = name;
    return true;
}

std::string BlobInfoToName(const BlobInfo& blob) {
    std::string name;
    if (blob.type == BlobType::kTmpBlob) {
        name += "TMP-";
    } else if (blob.type == BlobType::kDataBlob) {
        name += "DAT-";
    } else {
        BYTE_ASSERT(false);
    }

    name += byte::StringPrint("%010lu", blob.blob_id);
    return name;
}

bool GetBlockFooter(const char* buf, storage::BlockFooter* footer) {
    const ProtoHeader* proto_header =
        reinterpret_cast<const ProtoHeader*>(&buf[kBlockFooterSize - sizeof(ProtoHeader)]);
    const char* start = buf + kBlockFooterSize - sizeof(ProtoHeader) - proto_header->proto_size;
    if (!footer->ParseFromArray(start, proto_header->proto_size)) {
        LOG_ERROR("Parse block footer failed").put("Size", proto_header->proto_size);
        return false;
    }
    uint32_t crc32c = byte::CRCUtil::ComputeCRC32(0, start, proto_header->proto_size);
    if (crc32c != proto_header->proto_crc) {
        LOG_ERROR("Blobk header crc mismatch")
            .put("Expected", proto_header->proto_crc)
            .put("Real", crc32c);
        return false;
    }
    return true;
}

// Only if
// Before: x x x x 1 2 3 4 5
// After:          1 2 3 4 5 x x x x
bool CheckBlobHeader(const storage::BlobHeader& before_header,
                     const storage::BlobHeader& after_header) {
    if (after_header.blob_id() <= before_header.blob_id()) {
        return false;
    }
    if (after_header.start_record_sequence() < before_header.start_record_sequence()) {
        return false;
    }
    int i1 = 0;
    while (i1 < before_header.data_blobs_size() && 0 < after_header.data_blobs_size() &&
           before_header.data_blobs(i1).blob_id() < after_header.data_blobs(0).blob_id()) {
        ++i1;
    }
    if (before_header.data_blobs_size() - i1 > after_header.data_blobs_size()) {
        return false;
    }
    int i2 = 0;
    for (; i1 < before_header.data_blobs_size(); ++i1, ++i2) {
        const storage::BlobInfo& blob1 = before_header.data_blobs(i1);
        const storage::BlobInfo& blob2 = after_header.data_blobs(i2);
        if (!google::protobuf::util::MessageDifferencer::Equals(blob1, blob2)) {
            return false;
        }
    }
    return true;
}

storage::BlobInfo FillBlobInfo(const storage::BlobHeader& blob_header, bool frozen) {
    storage::BlobInfo blob_info;
    blob_info.set_blob_id(blob_header.blob_id());
    blob_info.set_end_record_sequence(blob_header.start_record_sequence());
    blob_info.set_start_offset(blob_header.start_offset());
    blob_info.set_blob_start_offset(
        CalcBlobHeaderSize(blob_header.header_size(), blob_header.start_offset()));
    blob_info.set_blob_end_offset(blob_info.blob_start_offset());
    blob_info.set_freeze_ms(frozen ? GetCurrentTimeInMs() : 0);
    blob_info.set_end_offset(blob_header.start_offset());
    blob_info.set_truncated_offset(blob_header.truncated_offset());
    return blob_info;
}

bool SerializeBlockFooter(const storage::BlockFooter& footer, char* buf, size_t size) {
    if (sizeof(ProtoHeader) + footer.ByteSize() > size) {
        LOG_ERROR("Serialize block footer failed: buffer not enought")
            .put("Footer", footer.ShortDebugString())
            .put("FooterSize", footer.ByteSize())
            .put("BufferSize", size);
        return false;
    }
    size_t pos = size - sizeof(ProtoHeader) - footer.ByteSize();
    memset(buf, 0, pos);
    if (!footer.SerializeToArray(buf + pos, footer.ByteSize())) {
        LOG_ERROR("Serialize block footer to array failed")
            .put("Footer", footer.ShortDebugString());
        return false;
    }
    uint32_t crc32c = byte::CRCUtil::ComputeCRC32(0, buf + pos, footer.ByteSize());
    pos += footer.ByteSize();
    ProtoHeader* header = reinterpret_cast<ProtoHeader*>(buf + pos);
    BYTE_ASSERT(pos + sizeof(ProtoHeader) == size);
    header->proto_size = footer.ByteSize();
    header->proto_crc = crc32c;
    return true;
}

bool CheckBlobInfo(const storage::BlobInfo& before_info, const storage::BlobInfo& after_info) {
    if (before_info.blob_id() != after_info.blob_id()) {
        return false;
    }
    if (before_info.end_record_sequence() > after_info.end_record_sequence()) {
        return false;
    }
    if (before_info.end_offset() > after_info.end_offset()) {
        return false;
    }
    if (before_info.truncated_offset() > after_info.truncated_offset()) {
        return false;
    }
    if (before_info.blob_end_offset() > after_info.blob_end_offset()) {
        return false;
    }
    return true;
}

}  // namespace stream
}  // namespace bcache2
