// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/meta/serializable.h"

#include <string>

#include "butil/crc32c.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"

#include "protocol/metaserver.pb.h"

namespace bcache2 {
namespace metaserver {

Status Serializable::PackToStream(google::protobuf::io::ZeroCopyOutputStream* stream) {
    std::string serialized_data;
    Status status = SerializeToString(&serialized_data);
    RETURN_IF_STATUS_ERROR(status);

    SnapshotRecordInfo info;
    info.set_type_name(GetSerializeTypeName());
    info.set_size(serialized_data.size());
    std::string serialized_info;
    info.SerializeToString(&serialized_info);

    uint32_t crc32c = butil::crc32c::Extend(                                   //
        butil::crc32c::Value(serialized_info.data(), serialized_info.size()),  //
        serialized_data.data(), serialized_data.size());

    google::protobuf::io::CodedOutputStream out(stream);
    out.WriteLittleEndian32(static_cast<uint32_t>(serialized_info.size()));
    out.WriteLittleEndian32(crc32c);
    out.WriteString(serialized_info);
    out.WriteString(serialized_data);
    return Status::OK();
}

Status Serializable::UnPackFromStream(google::protobuf::io::ZeroCopyInputStream* stream) {
    google::protobuf::io::CodedInputStream in(stream);
    uint32_t info_size = 0;
    uint32_t crc32c = 0;
    uint32_t crc32c_computed = 0;
    if (!in.ReadLittleEndian32(&info_size)) {
        return Status::Internal("I/O Error");
    }
    if (!in.ReadLittleEndian32(&crc32c)) {
        return Status::Internal("I/O Error");
    }
    std::string buf;
    if (!in.ReadString(&buf, info_size)) {
        return Status::Internal("I/O Error");
    }
    crc32c_computed = butil::crc32c::Value(buf.data(), buf.size());
    SnapshotRecordInfo info;
    if (!info.ParseFromString(buf)) {
        return Status::Internal("Parse Error, RecordInfo parse failed");
    }
    buf.clear();
    if (!in.ReadString(&buf, info.size())) {
        return Status::Internal("I/O Error");
    }
    crc32c_computed = butil::crc32c::Extend(crc32c_computed, buf.data(), buf.size());
    if (crc32c != crc32c_computed) {
        return Status::Internal("Data Corrupted");
    }
    if (info.type_name() != GetSerializeTypeName()) {
        return Status::Internal("TypeName not match");
    }

    return ParseFromString(buf);
}

}  // namespace metaserver
}  // namespace bcache2

