// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/storage/page_store.h"

#include <byte/include/assert.h>
#include <byte/include/macros.h>
#include <gflags/gflags.h>
#include <lz4.h>

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "blockcache/blockcache.h"
#include "common/coclosure.h"
#include "common/function_closure.h"
#include "common/logging.h"
#include "common/scoped_invoker.h"
#include "common/sync_closure.h"
#include "partition/index/index.h"
#include "partition/partition.h"
#include "partition/partition_fiu.h"
#include "partition/storage/zone.h"
#include "protocol/config.pb.h"

DECLARE_int32(storage_zone_size);
DECLARE_bool(page_store_enable_compress);
DECLARE_uint64(page_store_compress_trigger_threshold);

namespace bcache2 {
namespace partition {

PageStore::PageStore(Partition* partition, Index* index, stream::Env* env,
                     MetricsManager* metrics_manager, StoreRepPolicy rep_policy,
                     uint64_t partition_id, blockcache::BlockCache* blockcache)
    : partition_(partition),
      index_(index),
      env_(env),
      metrics_manager_(metrics_manager),
      rep_policy_(rep_policy),
      partition_id_(partition_id),
      blockcache_(blockcache) {
    metrics_.Init(metrics_manager);
}

PageStore::~PageStore() {}

void PageStore::Init(Options opts) {
    condition_ = opts.condition;
    stream_uri_pattern_ = opts.stream_uri_pattern;
}

// load zone info from the index and create streams for new zones
Status PageStore::UpdateZones() {
    if (version_ != UINT64_MAX && version_ == index_->GetZoneVersion()) {
        // no need to update zones
        return Status::OK();
    }

    storage::IndexLog::MetaItem meta_info =
        index_->MetaInfo();  // index has the canonical meta information
    for (uint32_t zone_id = 1; zone_id < zones_.size(); ++zone_id) {
        if (zones_[zone_id] == nullptr) {
            continue;
        }

        ZoneInfo zone_info;
        bool found = index_->GetZoneInfo(zone_id, &zone_info);
        if (!found) {  // delete zones that should not exist
            auto stream = zones_[zone_id]->stream.release();
            zones_[zone_id].reset();
            SYNC_CALL0(stream->Close);
            continue;
        }
    }

    for (const auto& item : meta_info.zones()) {
        const storage::IndexLog::ZoneInfo& zone_info = item.second;
        zones_.resize(std::max(zone_info.zone_id() + 1UL, zones_.size()));
        switch (zone_info.state()) {
        case storage::IndexLog::INIT:
            break;
        case storage::IndexLog::CREATED:
            writing_zone_id_ = zone_info.zone_id();  // only one CREATED zone is expected
            FALLTHROUGH_INTENDED;
        case storage::IndexLog::RECYCLED:
        case storage::IndexLog::FROZEN:
            if (zones_[zone_info.zone_id()] == nullptr) {
                stream::Stream* stream = nullptr;
                Status status = partition_->OpenPartitionStream(
                    GetZoneUri(zone_info.zone_id()), PARTITION_STREAM_PAGE, zone_info.zone_id(),
                    false, &stream);
                if (!status.ok()) {
                    LOG_ERROR("Open stream failed")
                        .put("PartitionId", partition_->GetPartitionID())
                        .put("Error", status)
                        .put("ZoneId", zone_info.ShortDebugString());
                    return status;
                }
                zones_[zone_info.zone_id()].reset(
                    new ZoneOpenInfo(zone_info.zone_id(), stream, metrics_manager_));
                break;
            }
        default:
            break;
        }
    }

    LOG_INFO("Update zone info success")
        .put("PartitionId", partition_->GetPartitionID())
        .put("NewVersion", meta_info.zone_version());
    version_ = meta_info.zone_version();
    return Status::OK();
}

void PageStore::Close() {
    for (auto& zone : zones_) {
        if (!zone) {
            continue;
        }
        if (zone->stream) {
            SYNC_CALL0(zone->stream->Close);
        }
    }
}

Status PageStore::PrepareNewZone(bool force_new_zone) {
    // as long as the writing zone is not full
    if (!force_new_zone && writing_zone_id_ != 0 &&
        zones_[writing_zone_id_]->stream->Stat().length <
            static_cast<uint64_t>(FLAGS_storage_zone_size)) {
        return Status::OK();
    }

    if (writing_zone_id_ != 0) {  // freeze the old zone
        ZoneInfo zone_info;
        BYTE_ASSERT(index_->GetZoneInfo(writing_zone_id_, &zone_info));
        BYTE_ASSERT(zone_info.state() == storage::IndexLog::CREATED);
        zone_info.set_zone_id(writing_zone_id_);
        zone_info.set_state(storage::IndexLog::FROZEN);
        zone_info.set_frozen_time_ms(GetCurrentTimeInMs());
        zone_info.set_total_bytes(zones_[writing_zone_id_]->stream->Stat().length);
        index_->UpdateZoneInfo(writing_zone_id_, std::move(zone_info));
        writing_zone_id_ = 0;
    }

    uint32_t new_zone_id = 0;
    // Reuse zone id that failed to create
    for (const auto& item : index_->MetaInfo().zones()) {
        if (item.second.state() ==
            storage::IndexLog::INIT) {  // reuse zones that are being initialized
            new_zone_id = item.first;
            break;
        }
    }
    if (new_zone_id == 0) {  // Find the smallest unused zone id
        new_zone_id = 1;
        storage::IndexLog::ZoneInfo zone_info;
        while (new_zone_id < static_cast<uint32_t>(FLAGS_storage_max_zone_count) &&
               index_->GetZoneInfo(new_zone_id, &zone_info)) {
            new_zone_id++;
        }
        BYTE_ASSERT(new_zone_id < static_cast<uint32_t>(FLAGS_storage_max_zone_count));
        zone_info.Clear();
        zone_info.set_zone_id(new_zone_id);
        zone_info.set_state(storage::IndexLog::INIT);
        zone_info.set_init_time_ms(GetCurrentTimeInMs());
        zone_info.set_version(index_->GetZoneVersion());
        index_->UpdateZoneInfo(new_zone_id,
                               std::move(zone_info));  // notify the index of this new zone
    }

    // Ensure meta info is persistent to avoid 'wild' zones
    Controller ctrl;
    SYNC_CALL(index_->Commit, &ctrl);
    BYTE_ASSERT(ctrl.status().ok() || ctrl.status().IsStreamAbort()) << ctrl.status();

    stream::Stream* stream = nullptr;
    Status status = partition_->OpenPartitionStream(GetZoneUri(new_zone_id), PARTITION_STREAM_PAGE,
                                                    new_zone_id, true, &stream);
    if (!status.ok()) {
        LOG_ERROR("Create stream failed")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Uri", GetZoneUri(new_zone_id))
            .put("Error", status);
        return status;
    }

    writing_zone_id_ = new_zone_id;
    zones_.resize(std::max(writing_zone_id_ + 1UL, zones_.size()));
    zones_[writing_zone_id_].reset(new ZoneOpenInfo(writing_zone_id_, stream, metrics_manager_));
    LOG_INFO("New Zone")
        .put("PartitionId", partition_->GetPartitionID())
        .put("ZoneId", writing_zone_id_);

    ZoneInfo zone_info;
    BYTE_ASSERT(index_->GetZoneInfo(writing_zone_id_, &zone_info));
    BYTE_ASSERT(zone_info.state() == storage::IndexLog::INIT);
    zone_info.set_state(storage::IndexLog::CREATED);  // update its state
    zone_info.set_created_time_ms(GetCurrentTimeInMs());
    index_->UpdateZoneInfo(writing_zone_id_, std::move(zone_info));

    ctrl.Reset();
    SYNC_CALL(index_->Commit, &ctrl);  // commit again
    BYTE_ASSERT(ctrl.status().ok() || ctrl.status().IsStreamAbort()) << ctrl.status();

    version_ = index_->GetZoneVersion();
    return Status::OK();
}

void PageStore::RecycledZone(uint32_t zone_id) {
    BYTE_ASSERT(zones_[zone_id].get() != nullptr);

    ZoneInfo zone_info;
    BYTE_ASSERT(index_->GetZoneInfo(zone_id, &zone_info));
    BYTE_ASSERT(zone_info.state() == storage::IndexLog::FROZEN);
    zone_info.set_state(storage::IndexLog::RECYCLED);
    zone_info.set_recycled_time_ms(GetCurrentTimeInMs());
    index_->UpdateZoneInfo(zone_id, std::move(zone_info));
    PARTITION_FAULT_INJECT("page_gc/gc_current_zone/in_recycle_zone");
    Controller ctrl;
    SYNC_CALL(index_->Commit, &ctrl);
    BYTE_ASSERT(ctrl.status().ok() || ctrl.status().IsStreamAbort()) << ctrl.status();

    LOG_INFO("RecycledZone")
        .put("PartitionId", partition_->GetPartitionID())
        .put("ZoneId", zone_id);
    version_ = index_->GetZoneVersion();
}

// invoked by PageGC
void PageStore::DestroyZone(uint32_t zone_id) {
    BYTE_ASSERT(zones_[zone_id].get() != nullptr);
    SYNC_CALL0(zones_[zone_id]->stream->Close);
    zones_[zone_id]
        .reset();  // remove from the zone map, which means its zone ID can be reused in the future

    Controller ctrl;
    stream::Env::DeleteOptions options;
    env_->DeleteStream(&ctrl, condition_, GetZoneUri(zone_id), options);
    if (!ctrl.status().ok()) {
        LOG_ERROR("Delete stream failed")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Uri", GetZoneUri(zone_id))
            .put("Error", ctrl.status());
        return;
    }

    ZoneInfo zone_info;
    BYTE_ASSERT(index_->GetZoneInfo(zone_id, &zone_info));
    BYTE_ASSERT(zone_info.state() == storage::IndexLog::RECYCLED);
    index_->DeleteZoneInfo(zone_id);

    ctrl.Reset();
    SYNC_CALL(index_->Commit, &ctrl);
    BYTE_ASSERT(ctrl.status().ok() || ctrl.status().IsStreamAbort()) << ctrl.status();

    LOG_INFO("DestroyZone").put("PartitionId", partition_->GetPartitionID()).put("ZoneId", zone_id);
    version_ = index_->GetZoneVersion();
}

// from Index
std::vector<std::pair<uint32_t, PageStore::ZoneInfo>> PageStore::GetZoneInfos() const {
    std::vector<std::pair<uint32_t, ZoneInfo>> zone_infos;
    for (const auto& item : index_->MetaInfo().zones()) {
        ZoneInfo zone_info = item.second;
        if (zones_[zone_info.zone_id()]) {
            zone_info.set_total_bytes(zones_[zone_info.zone_id()]->stream->Stat().length);
        }
        zone_infos.emplace_back(zone_info.zone_id(), zone_info);
    }
    return zone_infos;
}

void PageStore::WritePage(PageInfo* page) {
    BYTE_ASSERT(!page->header.page_in_log());
    std::string* page_data = &page->data;
    std::string compress_page_data;
    if (FLAGS_page_store_enable_compress &&
        page->data.size() >= FLAGS_page_store_compress_trigger_threshold) {
        compress_page_data.resize(LZ4_compressBound(page->data.size()));
        int compress_size = LZ4_compress_default(page->data.data(), &compress_page_data[0],
                                                 page->data.size(), compress_page_data.size());
        if (compress_size > 0) {
            compress_page_data.resize(compress_size);
            page_data = &compress_page_data;
            page->header.set_compress(storage::PageHeader::LZ4);
        } else {
            // just fall back to uncompress case
            LOG_ERROR("Write page compress failed")
                .put("PartitionId", partition_->GetPartitionID())
                .put("SlotId", page->header.slot_id())
                .put("ObjectId", page->header.object_id())
                .put("PageId", page->header.page_id());
        }
    }

    page->header.set_raw_data_size(page->data.size());
    page->header.set_data_size(page_data->size());

    double compress_ratio = page->data.size() > 0 ? (page_data->size() / page->data.size()) : 0.0;
    metrics_.page_size->get()->Set(page->data.size());
    metrics_.compressed_page_size->get()->Set(page_data->size());
    metrics_.compress_ratio->get()->Set(compress_ratio);

    LOG_DEBUG("Write page")
        .put("PartitionId", partition_->GetPartitionID())
        .put("Size", page->data.size())
        .put("PageHeader", page->header.ShortDebugString());

    std::string buffer;
    buffer.resize(sizeof(uint16_t) + page->header.ByteSize());
    *reinterpret_cast<uint16_t*>(&buffer[0]) = page->header.ByteSize();
    BYTE_ASSERT(
        page->header.SerializeToArray(&buffer[0] + sizeof(uint16_t), page->header.ByteSize()));

    std::vector<std::string> v(2);
    v[0] = std::move(buffer);
    v[1] = *page_data;
    page->size = v[0].size() + v[1].size();

    BYTE_ASSERT(!zones_.empty());
    auto zone = zones_[writing_zone_id_].get();
    uint64_t offset = 0;
    // TODO(wangtai.10): AppendV is not atomic
    zone->stream->AppendV(std::move(v), &offset);
    BYTE_ASSERT(offset < UINT32_MAX);
    page->address = MakeZoneAddress(writing_zone_id_, offset);

    zone->metrics.write_qps->get()->Increment();
    zone->metrics.write_throughput->get()->Add(page->size);
}

void PageStore::Commit(Controller* ctrl, Closure<void>* callback) {
    auto zone = zones_[writing_zone_id_].get();
    zone->stream->Commit(ctrl, callback);
}

void PageStore::ReadPage(Controller* ctrl, PageIndex index, PageInfo* page_info,
                         Closure<void>* callback) {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("ObjectId", static_cast<uint16_t>(index.object_id))
        .put("PageId", index.page_id)
        .put("Address", index.address)
        .put("Size", index.page_size);
    ReadPageTask* task = new ReadPageTask;
    task->ctrl = ctrl;
    task->page_index = index;
    task->page_info = page_info;
    task->callback = callback;
    task->buffer.resize(index.page_size);
    uint32_t zone_id = ExtractZoneId(index.address);
    BYTE_ASSERT(zones_.size() >= zone_id + 1);
    task->zone = zones_[zone_id].get();
    BYTE_ASSERT(task->zone != nullptr);
    if (FLAGS_enable_blockcache && blockcache_) {
        auto unique_id = GetUniqueID(&task->page_index);
        metrics_.blockcache_get_qps->get()->Increment();
        if (read_path_test_counters_) {
            ++read_path_test_counters_->blockcache_gets;
        }
        auto status = blockcache_->Get(unique_id, &task->buffer);
        if (status.ok()) {
            metrics_.blockcache_hit_qps->get()->Increment();
            if (read_path_test_counters_) {
                ++read_path_test_counters_->blockcache_hits;
            }
            task->page_in_cache = true;
            task->ctrl->set_status(status);
            byte::InvokeInCurrentThread(
                NewClosure(this, &PageStore::OnReadPageDone, task));  // prevent invoke in-place
            return;
        }
    }
    if (read_path_test_counters_) {
        ++read_path_test_counters_->persistent_reads;
    }
    metrics_.persistent_read_qps->get()->Increment();
    task->zone->metrics.read_qps->get()->Increment();
    task->zone->metrics.read_throughput->get()->Add(index.page_size);
    task->zone->stream->Read(ctrl, ExtractZoneOffset(index.address), &task->buffer[0],
                             index.page_size, NewClosure(this, &PageStore::OnReadPageDone, task));
}

void PageStore::OnReadPageDone(ReadPageTask* task) {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("ObjectId", static_cast<uint16_t>(task->page_index.object_id))
        .put("PageId", task->page_index.page_id)
        .put("Address", task->page_index.address)
        .put("Size", task->page_index.page_size)
        .put("Status", task->ctrl->status());
    task->zone->metrics.read_latency->get()->Set(task->timer.GetElapsedInUs());
    std::unique_ptr<ReadPageTask> _task(task);
    ScopedInvoker done(task->callback);
    if (!task->ctrl->status().ok()) {
        return;
    }
    // check and parse
    if (task->buffer.size() < sizeof(uint16_t)) {
        LOG_ERROR("Read page buffer size short")
            .put("PartitionId", partition_->GetPartitionID())
            .put("BufferSize", task->buffer.size())
            .put("PageAddress", task->page_index.address)
            .put("PageSize", task->page_index.page_size);
        task->ctrl->set_status(Status::DataLoss(""));
        return;
    }
    uint16_t header_size = *reinterpret_cast<uint16_t*>(&task->buffer[0]);
    if (task->buffer.size() < sizeof(uint16_t) + header_size) {
        LOG_ERROR("Read page buffer size short")
            .put("PartitionId", partition_->GetPartitionID())
            .put("BufferSize", task->buffer.size())
            .put("HeaderSize", header_size)
            .put("PageAddress", task->page_index.address)
            .put("PageSize", task->page_index.page_size);
        task->ctrl->set_status(Status::DataLoss(""));
        return;
    }
    storage::PageHeader header;
    if (!header.ParseFromArray(&task->buffer[sizeof(uint16_t)], header_size)) {
        LOG_ERROR("Read page parse header failed")
            .put("PartitionId", partition_->GetPartitionID())
            .put("BufferSize", task->buffer.size())
            .put("HeaderSize", header_size)
            .put("PageAddress", task->page_index.address)
            .put("PageSize", task->page_index.page_size);
        task->ctrl->set_status(Status::DataLoss(""));
        return;
    }

    uint32_t page_size_in_store = sizeof(uint16_t) + header_size + header.data_size();
    if (page_size_in_store != task->page_index.page_size) {
        LOG_ERROR("Read page check page_size failed")
            .put("PartitionId", partition_->GetPartitionID())
            .put("BufferSize", task->buffer.size())
            .put("HeaderSize", header_size)
            .put("PageAddress", task->page_index.address)
            .put("PageSize", task->page_index.page_size)
            .put("PageSizeInStore", page_size_in_store);
        task->ctrl->set_status(Status::DataLoss(""));
        return;
    }

    std::string page_data = task->buffer.substr(sizeof(uint16_t) + header_size);
    if (header.compress() == storage::PageHeader::LZ4) {
        std::string raw_data;
        raw_data.resize(header.raw_data_size());
        int decompress_size =
            LZ4_decompress_safe(page_data.data(), &raw_data[0], page_data.size(), raw_data.size());
        if (decompress_size < 0 || static_cast<size_t>(decompress_size) != raw_data.size()) {
            LOG_ERROR("Read page decompress failed")
                .put("PartitionId", partition_->GetPartitionID())
                .put("SlotId", header.slot_id())
                .put("ObjectId", header.object_id())
                .put("PageId", header.page_id())
                .put("PageAddress", task->page_index.address)
                .put("PageSize", task->page_index.page_size);
            task->ctrl->set_status(Status::Internal("Decompress failed"));
            return;
        }
        page_data = std::move(raw_data);
    }
    if ((!task->page_in_cache) && FLAGS_enable_blockcache && blockcache_) {
        auto unique_id = GetUniqueID(&task->page_index);
        LOG_CALL_DEBUG()
            .put("PartitionId", partition_->GetPartitionID())
            .put("UniqueID", unique_id)
            .put("PageId", task->page_index.page_id)
            .put("Address", task->page_index.address)
            .put("Size", task->page_index.page_size)
            .put("HeaderSize", header_size)
            .put("buffer size", task->buffer.size());
        blockcache_->Put(unique_id, task->buffer);
    }

    task->page_info->header = std::move(header);
    task->page_info->address = task->page_index.address;
    task->page_info->size = task->page_index.page_size;
    task->page_info->data = page_data;
}

std::string PageStore::GetZoneUri(uint32_t zone_id) {
    return byte::StringPrint(stream_uri_pattern_.c_str(), zone_id);
}

std::string PageStore::GetUniqueID(PageIndex* page) {
    auto zone_id = ExtractZoneId(page->address);
    storage::IndexLog::ZoneInfo zone_info;
    BYTE_ASSERT(index_->GetZoneInfo(zone_id, &zone_info));
    std::string uniqueID = byte::StringPrint("%" PRIu64 ":%" PRIu64 ":%" PRIu64, partition_id_,
                                             page->address, zone_info.version());
    LOG_DEBUG("GetUniqueID")
        .put("PartitionId", partition_->GetPartitionID())
        .put("partition_id_:", partition_id_)
        .put("zone id:", zone_id)
        .put("zone version", zone_info.version())
        .put("uniqueID", uniqueID);
    return uniqueID;
}

void PageStore::ReadRawStream(uint32_t zone_id, Controller* ctrl, uint64_t id, void* data,
                              size_t size, Closure<void>* callback) {
    if (zone_id >= zones_.size() || zones_[zone_id] == nullptr) {
        ctrl->set_status(Status::NotFound("Page store zone not found"));
        byte::InvokeInCurrentThread(callback);
        return;
    }
    zones_[zone_id]->stream->Read(ctrl, id, data, size, callback);
}

stream::ScopedIterator PageStore::NewRawStreamIterator(uint32_t zone_id, uint64_t start_id,
                                                       uint64_t end_id) {
    if (zone_id >= zones_.size() || zones_[zone_id] == nullptr) {
        return nullptr;
    }
    return zones_[zone_id]->stream->NewIterator(start_id, end_id);
}

// from zones_
PageStoreInfo PageStore::GetInfo() const {
    PageStoreInfo info;
    for (uint32_t zone_id = 1; zone_id < zones_.size(); ++zone_id) {
        if (zones_[zone_id] == nullptr) {
            continue;
        }

        ZoneInfo zone_storage_info;
        if (!index_->GetZoneInfo(zone_id, &zone_storage_info)) {
            continue;
        }

        auto new_zone_info = info.add_zones();
        new_zone_info->set_zone_id(zone_id);
        *new_zone_info->mutable_stream_info() = zones_[zone_id]->stream->GetInfo();
        *new_zone_info->mutable_zone_storage_info() = std::move(zone_storage_info);
    }
    info.set_writting_zone_id(writing_zone_id_);
    info.set_version(version_);
    return info;
}

Status PageStore::RestoreInfo(const PageStoreInfo& pagestore_info) {
    // NOTE: pagestore_info may be in a newer snapshot

    std::vector<const StreamInfo*> remote_zone_stream_info;
    std::vector<const ZoneInfo*> remote_zone_info;
    for (int i = 0; i < pagestore_info.zones_size(); ++i) {
        const bcache2::ZoneInfo& zone_info = pagestore_info.zones(i);
        remote_zone_stream_info.resize(
            std::max(remote_zone_stream_info.size(), zone_info.zone_id() + 1UL), nullptr);
        remote_zone_info.resize(std::max(remote_zone_info.size(), zone_info.zone_id() + 1UL),
                                nullptr);
        remote_zone_stream_info[zone_info.zone_id()] = &zone_info.stream_info();
        remote_zone_info[zone_info.zone_id()] = &zone_info.zone_storage_info();
    }

    for (uint32_t zone_id = 1; zone_id < zones_.size(); ++zone_id) {
        if (zones_[zone_id] == nullptr) {
            continue;
        }

        ZoneInfo zone_info;
        BYTE_ASSERT(index_->GetZoneInfo(zone_id, &zone_info)) << zone_id;
        if (zone_id >= remote_zone_stream_info.size() ||
            remote_zone_stream_info[zone_id] == nullptr) {
            if (UNLIKELY(zone_info.state() != storage::IndexLog::RECYCLED)) {
                // since the RECYCLED state will remain for a long time,
                // we should be in the RECYCLED state when the zone_id does not exist at the remote,
                // or something unexpected might have happened
                LOG_ERROR("Zone id not found at the remote")
                    .put("PartitionId", partition_->GetPartitionID())
                    .put("ZoneId", zone_id);
                return Status::InvalidArgument("Zone id not found at the remote");
            }
            continue;
        }

        BYTE_ASSERT(remote_zone_info[zone_id]);
        if (zone_info.version() > remote_zone_info[zone_id]->version()) {
            // we expect remote(master) zone version is always newer than current(slave)
            LOG_ERROR("Invalid zone version")
                .put("PartitionId", partition_->GetPartitionID())
                .put("ZoneId", zone_id)
                .put("ZoneInfo", zone_info.ShortDebugString())
                .put("RemoteZoneInfo", remote_zone_info[zone_id]->ShortDebugString());
            return Status::InvalidArgument("Invalid zone version");
        }
        if (zone_info.version() < remote_zone_info[zone_id]->version()) {
            // remote zone is reused, so we can't(and not necessary) update straem info
            LOG_INFO("Zone reused in remote")
                .put("PartitionId", partition_->GetPartitionID())
                .put("ZoneId", zone_id)
                .put("ZoneVersion", zone_info.version())
                .put("RemoteZoneVersion", remote_zone_info[zone_id]->version());
            continue;
        }

        Status status = zones_[zone_id]->stream->RestoreInfo(*remote_zone_stream_info[zone_id]);
        if (!status.ok()) {
            LOG_ERROR("Failed to replay zone stream info")
                .put("PartitionId", partition_->GetPartitionID())
                .put("ZoneId", zone_id)
                .put("Status", status)
                .put("ZoneStreamInfo", remote_zone_stream_info[zone_id]->ShortDebugString());
            return status;
        }
    }

    return Status::OK();
}

}  // namespace partition
}  // namespace bcache2
