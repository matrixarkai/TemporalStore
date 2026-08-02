#include "zone_manager.h"

namespace mtcache {

ZoneManagerLargeModeImpl::ZoneManagerLargeModeImpl(
    std::shared_ptr<DeviceHandle> dev, bool using_existing_db)
    : ZoneManager(std::move(dev)),
      zones_(nullptr),
      group_(nullptr),
      data_zone_(nullptr),
      meta_zone_(nullptr),
      header_size(dev_->info.page_size),
      footer_size(dev_->info.page_size) {
  dev_->InitZones(&zones_);

  if (!using_existing_db || PickRecoverableGroupsForReopen() != 0) {
    // We re-initialize the store under these two conditions:
    // 1) Caller explicitly requested to discard existing data via gflags
    // 2) Fail to pick recoverable groups
    // All previously written data is lost
    for (size_t i = 0; i < dev_->info.nr_groups_in_device; i++) {
      ZoneGroup* g = new ZoneGroup(dev_.get(), i, zones_ + i);
      g->Reset();
      group_set_.push_back(g);
      free_list_.push_back(i);
    }
  }
  assert(free_list_.size() > 0);
  int gid = free_list_.front();
  free_list_.pop_front();
  group_ = group_set_[gid];
  data_zone_ = group_->AllocateZone(DataType::DATA);

  // The sequence is not used at the moment.
  sequence++;
  AddHeader(data_zone_, sequence, magic_number);
  if (meta_zone_) {
    delete meta_zone_;
    meta_zone_ = nullptr;
  }
  meta_zone_ = group_->AllocateZone(DataType::META_LOG);
}

ZoneManagerLargeModeImpl::~ZoneManagerLargeModeImpl() {
  delete[] zones_;

  if (meta_zone_) {
    delete meta_zone_;
    meta_zone_ = nullptr;
  }

  for (size_t i = 0; i < dev_->info.nr_groups_in_device; i++) {
    assert(group_set_[i] != nullptr);
    delete group_set_[i];
  }
  LOG(INFO) << "Close zone manager"
            << ", gc_list.size(used zones): " << gc_list_.size()
            << ", free_list.size: " << free_list_.size();
  dev_->Close();
}

int ZoneManagerLargeModeImpl::Append(const char* buf, int size, DataType type,
                                     uint64_t* offset) {
  ZonedStoreMetrics::summaryAdd(
      ZonedStoreMetrics::zone_manager_append_batch_size, size);
  ZonedStoreMetrics::counterAdd(ZonedStoreMetrics::zone_manager_append_qps);
  ZonedStoreMetrics::counterAdd(
      ZonedStoreMetrics::zone_manager_append_throughput, size);
  ZonedStoreMetrics::ScopedLatency latency(
      ZonedStoreMetrics::zone_manager_append_latency);
  assert(!(reinterpret_cast<uint64_t>(buf) & 0xFFF));
  assert(!(size % 4096));
  assert(size >= 4096 && size <= dev_->info.zone_size);
  assert(is_ensure);

  if (type == DataType::DATA) {
    assert(data_zone_->AvailBytes() >= size);

    uint64_t off_in_dev = data_zone_->wp_;
    if (offset) {
      *offset = off_in_dev;
    }
    int ret = dev_->Write(buf, size, off_in_dev);
    if (ret != 0) {
      LOG(WARNING) << "Failed to write DATA into device!" << strerror(errno);
      // TODO(guokuankuan) Need to reset the writer pointer of the device
      return -1;
    }
    data_zone_->wp_ += size;
    data_zone_->valid_bytes_ += size;

    // Update meta zone's start and write pointer.
    assert(data_zone_->wp_ % 4096 == 0);
    meta_zone_->start_ = data_zone_->wp_;
    meta_zone_->wp_ = meta_zone_->start_;

    return 0;
  }

  assert(type == DataType::META_LOG);
  assert(meta_zone_->AvailBytes() >= size);
  assert(is_appned_once);

  uint64_t off_in_dev = meta_zone_->start_;
  if (offset) {
    *offset = off_in_dev;
  }
  int ret = dev_->Write(buf, size, off_in_dev);
  if (ret != 0) {
    // TODO(guokuankuan) Need to reset the writer pointer of the device
    LOG(WARNING) << "Failed to write META into device!" << strerror(errno);
    return -1;
  }

  // Record meta data offset.
  group_->SetMetaOffset(off_in_dev);

  // Record meta data size.
  group_->SetMetaSize(size);

  // Update meta_zone's valid bytes
  meta_zone_->valid_bytes_ += size;

  // Update data_zone's write pointer
  data_zone_->wp_ += size;

  {
    uint64_t page_size = dev_->info.page_size;

    assert(data_zone_->AvailBytes() >= footer_size);
    // Fill zone until footer
    uint64_t padding_size = data_zone_->AvailBytes() - footer_size;
    if (padding_size > 0) {
      assert(padding_size % page_size == 0);
      uint64_t left = padding_size;
      uint64_t max_flush_size = 1ul << 20;
      char* buf = reinterpret_cast<char*>(memalign(page_size, max_flush_size));
      memset(buf, 0, max_flush_size);
      while (left > 0) {
        uint64_t flush_size = (max_flush_size < left) ? max_flush_size : left;
        dev_->Write(buf, flush_size, data_zone_->wp_);
        // only increase wp
        data_zone_->wp_ += flush_size;
        left -= flush_size;
      }
      assert(left == 0);
      free(buf);
    }

    // Add footer
    AddFooter(data_zone_, group_->GetMetaOffset(), group_->GetMetaSize());
    LOG(INFO) << "Add Zone Footer "
              << ", zone offset: " << data_zone_->start_
              << ", meta offset: " << group_->GetMetaOffset()
              << ", meta size: " << group_->GetMetaSize();
    assert(data_zone_->AvailBytes() == 0);
  }

  is_appned_once = false;
  return 0;
}

int ZoneManagerLargeModeImpl::Read(char* buf, uint64_t offset, int size) {
  ZonedStoreMetrics::counterAdd(ZonedStoreMetrics::zone_manager_read_qps);
  ZonedStoreMetrics::counterAdd(ZonedStoreMetrics::zone_manager_read_throughput,
                                size);
  ZonedStoreMetrics::ScopedLatency latency(
      ZonedStoreMetrics::zone_manager_read_latency);
  int ret = dev_->Read(buf, size, offset);
  if (ret < 0) {
    LOG(FATAL) << "Failed to Read from the device!" << strerror(errno);
  }
  return 0;
}

int ZoneManagerLargeModeImpl::EnsureAvailableSpace(int data_size,
                                                   int meta_size) {
  is_ensure = true;
  // Update meta zone's start and write pointer.
  uint64_t page_size = dev_->info.page_size;
  uint64_t data_padding_bytes =
      (page_size - (data_size % page_size)) % page_size;
  assert(data_padding_bytes == 0);
  uint64_t meta_padding_bytes =
      (page_size - (meta_size % page_size)) % page_size;
  assert(meta_padding_bytes == 0);
  uint64_t need_bytes =
      data_size + data_padding_bytes + meta_size + meta_padding_bytes;
  if (need_bytes <= data_zone_->AvailBytes() - footer_size) {
    return 1;
  } else {
    return 0;
  }
}

int ZoneManagerLargeModeImpl::FinishGroup() {
  is_ensure = false;
  is_appned_once = true;

  group_->CloseZone(data_zone_);

  // Put the group into gc list.
  if (group_) {
    std::lock_guard<std::mutex> lg(gc_list_lk_);
    gc_list_.push_back(group_);
    LOG(INFO) << "Finish group, gc_list size: " << gc_list_.size();
  }

  // Allocate a new group
  assert(gc_list_.size() > 0 || free_list_.size() > 0);
  if (free_list_.size() == 0) {
    LOG(WARNING) << "No enough Free Groups, gc_list_.size(): " << gc_list_.size()
                 << ", free_list_.size(): " << free_list_.size()
                 << ", Reclaim GCGroup immediately.";
  }

  // We should wait until there's a free group released from GC.
  std::unique_lock<std::mutex> lk(free_list_lk_);
  free_list_cv_.wait(lk, [this] { return free_list_.size() > 0; });

  int gid = free_list_.front();
  free_list_.pop_front();
  group_ = group_set_[gid];
  data_zone_ = group_->AllocateZone(DataType::DATA);

  // Until now, sequence number is useless, may be used in the future.
  sequence++;
  AddHeader(data_zone_, sequence, magic_number);
  if (meta_zone_) {
    delete meta_zone_;
    meta_zone_ = nullptr;
  }
  meta_zone_ = group_->AllocateZone(DataType::META_LOG);
  return 0;
}

void ZoneManagerLargeModeImpl::TrimBytes(uint64_t off_in_dev, int size) {
  uint32_t gid = off_in_dev / dev_->info.group_size;
  if (gid > dev_->info.nr_groups_in_device) {
    LOG(INFO) << "Invalid Offset!";
    assert(false);
  }
  ZoneGroup* group = group_set_[gid];
  uint64_t off_in_group = off_in_dev % dev_->info.group_size;
  group->Trim(off_in_group, size);
}

std::pair<int16_t, GCMode> ZoneManagerLargeModeImpl::FindGCGroup() {
  uint64_t used = GetUsedSpace();
  uint64_t capacity = GetCapacity();
  uint64_t gc_zone_cnt_throttle = 10;
  // GC will not be started until there's no enough space.
  // e.g. less than 10 zones or 1GB or 10% total space
  uint64_t enough_free_space_min_limit =
              std::min(gc_zone_cnt_throttle * dev_->info.zone_capacity,
                      std::max(capacity / 10, 1UL << 30));
  if (capacity - used > enough_free_space_min_limit) {
    LOG(INFO) << "FindGCGroup skip, enough free space"
              << ", capacity: " << capacity
              << ", used: " << used
              << ", zone_size: " << dev_->info.zone_capacity
              << ", capacity - used: " << (capacity - used)
              << ", enough_free_space_min_limit " << enough_free_space_min_limit
              << ", gc_list.size(): " << gc_list_.size()
              << ", free_list.size(): " << free_list_.size();

    return {-1, LOSSY};
  }

  // Update GC list
  std::lock_guard<std::mutex> lg(gc_list_lk_);
  if (gc_list_.empty()) {
    LOG(WARNING) << "Failed to find a GC group, device capacity: " << capacity
                 << ", device usage: " << used 
                 << ", zone_size: " << dev_->info.zone_size;
    return {-1, LOSSY};
  }
  LOG(INFO) << "FindGCGroup, gc_list_ size: " << gc_list_.size()
            << ", device cap: " << capacity
            << ", device usage: " << used;

  gc_list_.sort([](ZoneGroup*& a, ZoneGroup*& b) -> bool {
    return a->GetGarbageRate() > b->GetGarbageRate();
  });

  ZoneGroup* g = gc_list_.front();
  return {g->GroupID(), LOSSY};
}

int ZoneManagerLargeModeImpl::ResetGroup(uint16_t group_id) {
  bool group_found = false;
  {
    std::lock_guard<std::mutex> lg(gc_list_lk_);
    for (auto it = gc_list_.begin(); it != gc_list_.end(); it++) {
      if ((*it)->GroupID() == group_id) {
        gc_list_.erase(it);
        group_found = true;
        break;
      }
    }
  }

  if (group_found) {
    ZoneGroup* group = group_set_[group_id];

    // Reset group
    group->Reset();

    // Add into free list.
    {
      std::lock_guard<std::mutex> lg(free_list_lk_);
      free_list_.push_back(group_id);
      LOG(INFO) << "Reset Group and add to free_list_, group_id: " << group_id;

      ZonedStoreMetrics::counterAdd(ZonedStoreMetrics::zoned_store_used,
                                    -dev_->info.group_size);
      int64_t used =
          ZonedStoreMetrics::counterGet(ZonedStoreMetrics::zoned_store_used);
      double write_amplification = static_cast<double>(GetUsedSpace()) / used;
      ZonedStoreMetrics::counterSet(
          ZonedStoreMetrics::zoned_store_write_amplification,
          static_cast<int>(write_amplification * 100));
    }
    free_list_cv_.notify_all();
    return 0;
  } else {
    LOG(WARNING) << "Cannot find target group to reset, group_id: " << group_id;
  }

  return -1;
}

int ZoneManagerLargeModeImpl::LoadMetaData(int group_id,
                                           GCWorker::LoadMetaCallback meta_cb) {
  ZoneGroup* group = group_set_[group_id];
  // meta data start
  uint64_t meta_offset = group->GetMetaOffset();
  // meta data size
  uint64_t meta_size = group->GetMetaSize();

  assert(meta_size > 0);
  char* meta_buf = reinterpret_cast<char*>(memalign(4096, meta_size));
  if (meta_buf == nullptr) {
    LOG(WARNING) << "Failed to memalign memory for group metadata reload.";
    return -1;
  }
  int ret = dev_->Read(meta_buf, meta_size, meta_offset);
  if (ret < 0) {
    LOG(WARNING) << "Failed to read group metadata, errno: " << strerror(errno);
    free(meta_buf);
    return -1;
  }
  meta_cb(meta_buf);
  free(meta_buf);
  return 0;
}

int ZoneManagerLargeModeImpl::PickRecoverableGroupsForReopen() {
  // header
  uint64_t header_offset = 0;
  char* header_buf = reinterpret_cast<char*>(memalign(4096, header_size));

  // footer
  uint64_t footer_offset = dev_->info.zone_capacity - footer_size;
  char* footer_buf = reinterpret_cast<char*>(memalign(4096, footer_size));

  for (uint32_t zid = 0; zid < dev_->info.nr_zones_in_device; zid++) {
    ZoneGroup* g = new ZoneGroup(dev_.get(),
                                 zid,
                                 zones_ + (zid * dev_->info.nr_zones_in_group));
    group_set_.push_back(g);

    int ret =
        dev_->Read(header_buf, header_size, zones_[zid].start_ + header_offset);
    if (ret < 0) {
      LOG(ERROR) << "Failed to Load Header!" << strerror(errno);
      goto error_case;
    }
    bool is_valid = false;
    {
      uint64_t seq_num = 0;
      memcpy(&seq_num, header_buf, sizeof(uint64_t));
      if (seq_num > 0) {
        is_valid = true;
      }
    }

    if (is_valid) {
      int ret = dev_->Read(footer_buf, footer_size,
                           zones_[zid].start_ + footer_offset);
      if (ret < 0) {
        LOG(ERROR) << "Failed to Load Footer!" << strerror(errno);
        goto error_case;
      }
      uint64_t meta_offset = 0;
      uint64_t meta_size = 0;

      // get offset
      memcpy(&meta_offset, footer_buf, sizeof(uint64_t));
      g->SetMetaOffset(meta_offset);
      // get size
      memcpy(&meta_size, footer_buf + sizeof(uint64_t), sizeof(uint64_t));
      g->SetMetaSize(meta_size);
      LOG(INFO) << "Check Recoverable Zone Footer(metadata) "
                << ", zid: " << zid
                << ", meta_offset: " << meta_offset
                << ", meta_size: " << meta_size
                << ", zone start offset: " << zones_[zid].start_
                << ", zone end offset: " << zones_[zid].start_ + zones_[zid].capacity_
                << ", pagesize: " << dev_->info.page_size;
      if (meta_offset > zones_[zid].start_ &&
          meta_offset < zones_[zid].start_ + zones_[zid].capacity_ &&
          meta_size > 0 &&
          meta_size <= zones_[zid].start_ + zones_[zid].capacity_ -
                           meta_offset - dev_->info.page_size) {
        recovery_list_.push_back(g);
      } else {
        free_list_.push_back(zid);
      }
    } else {
      free_list_.push_back(zid);
    }
  }

  // We should kep at least one free ZoneGroup
  if (free_list_.size() == 0 && recovery_list_.size() > 0) {
    ZoneGroup* g = recovery_list_.front();
    free_list_.push_back(g->GroupID());
    recovery_list_.pop_front();
  }

  LOG(INFO) << "PickRecoverZones, recovery_list size: " << recovery_list_.size()
            << ", gc_list_.size(): " << gc_list_.size()
            << ", free_list_.size(): " << free_list_.size();
  free(header_buf);
  free(footer_buf);
  return 0;

error_case:
  free(header_buf);
  free(footer_buf);
  recovery_list_.clear();
  free_list_.clear();
  for (auto* g : group_set_) {
    delete g;
  }
  group_set_.clear();
  return 1;
}

void ZoneManagerLargeModeImpl::Recovery(
    std::function<int(const char* buf)> index_cb) {
  LOG(INFO) << "Start ZonedStore Recovery Process";
  // Traverse recovery_list;
  char* meta_buf =
      reinterpret_cast<char*>(memalign(4096, dev_->info.zone_size));
  while (!recovery_list_.empty()) {
    ZoneGroup* group = recovery_list_.front();
    LOG(INFO) << "Recovering ZoneGroup, gid: " << group->GroupID()
              << ", meta size: " << group->GetMetaSize()
              << ", meta offset: " << group->GetMetaOffset();

    int ret =
        dev_->Read(meta_buf, group->GetMetaSize(), group->GetMetaOffset());
    if (ret < 0) {
      group->SetGarbageBytes(dev_->info.zone_size);
      LOG(WARNING) << "Failed to load metadata for recovery, group_id: "
                   << group->GroupID() << ", errno: " << strerror(errno);
    } else {
      uint64_t valid_bytes = index_cb(meta_buf);
      group->SetGarbageBytes(dev_->info.zone_size - valid_bytes);
      LOG(INFO) << "\tTotal bytes recovered: " << valid_bytes;
    }
    gc_list_.push_back(group);
    recovery_list_.pop_front();
  }
  free(meta_buf);
  LOG(INFO) << "Finish ZonedStore Recovery Process";
}

void ZoneManagerLargeModeImpl::AddHeader(Zone* z, uint64_t seq_num,
                                         uint64_t magic) {
  uint64_t page_size = dev_->info.page_size;
  char* header_buf = reinterpret_cast<char*>(memalign(page_size, header_size));
  memset(header_buf, 0, header_size);

  // add seq_num
  memcpy(header_buf, &seq_num, sizeof(uint64_t));
  // LOG(INFO) << "Add Header: " << seq_num;
  // add magic
  memcpy(header_buf + sizeof(uint64_t), &magic, sizeof(uint64_t));

  assert(z->wp_ == z->start_);
  int ret = dev_->Write(header_buf, header_size, z->wp_);
  if (ret != 0) {
    LOG(FATAL) << "Failed to write header";
  }
  z->wp_ += header_size;
  z->valid_bytes_ += header_size;
  free(header_buf);
}

void ZoneManagerLargeModeImpl::AddFooter(Zone* z, uint64_t offset,
                                         uint64_t size) {
  uint64_t page_size = dev_->info.page_size;
  char* footer_buf = reinterpret_cast<char*>(memalign(page_size, footer_size));
  memset(footer_buf, 0, footer_size);

  // add offset
  memcpy(footer_buf, &offset, sizeof(uint64_t));
  // add size
  memcpy(footer_buf + sizeof(uint64_t), &size, sizeof(uint64_t));
  assert(z->wp_ == (z->start_ + z->capacity_ - page_size));
  int ret = dev_->Write(footer_buf, footer_size, z->wp_);
  if (ret != 0) {
    LOG(FATAL) << "Failed to write footer";
  }
  z->wp_ += footer_size;
  z->valid_bytes_ += footer_size;
  free(footer_buf);
}

bool ZoneManagerLargeModeImpl::GetProperty(std::string property,
                                           std::string* result) {
  result->clear();
  char buf[1024];
  memset(buf, 0, sizeof(buf));
  if (property == "device") {
    snprintf(buf, sizeof(buf),
             "Device Size (B): %ld\n"
             "Group Size (B): %ld\n"
             "Zone Size (B): %ld\n"
             "Zone Capacity (B): %ld\n"
             "Groups in Device: %ld\n"
             "Zones in Device: %ld\n",
             dev_->info.device_capacity, dev_->info.group_size,
             dev_->info.zone_size, dev_->info.zone_capacity,
             dev_->info.nr_groups_in_device, dev_->info.nr_zones_in_device);
    result->append(buf);
    return true;
  } else if (property == "group") {
    snprintf(buf, sizeof(buf),
             "Used Groups: %ld\n"
             "Free Groups: %ld\n",
             gc_list_.size(), free_list_.size());
    result->append(buf);
    return true;
  } else if (property == "garbage") {
    snprintf(buf, sizeof(buf), "GID     Garbage Ratio\n");
    result->append(buf);
    for (auto it = gc_list_.begin(); it != gc_list_.end(); it++) {
      memset(buf, 0, sizeof(buf));
      snprintf(buf, sizeof(buf), "%7d %7f\n", (*it)->GroupID(),
               (*it)->GetGarbageRate());
      result->append(buf);
    }
    return true;
  }
  return false;
}

// TODO(guokuankuan) Change this to a global counter.
uint64_t ZoneManagerLargeModeImpl::GetGarbageBytes() {
  std::lock_guard<std::mutex> lg(gc_list_lk_);
  uint64_t total = 0;
  for (const auto* group : gc_list_) {
    total += group->GetGarbageBytes();
  }
  return total;
}

uint64_t ZoneManagerLargeModeImpl::GetUsedSpace() {
  std::lock_guard<std::mutex> lg(gc_list_lk_);
  uint64_t group_capacity =
      dev_->info.nr_zones_in_group * dev_->info.zone_capacity;
  // all groups in the gc_list should be considered used
  return gc_list_.size() * group_capacity;
}

}  // namespace mtcache
