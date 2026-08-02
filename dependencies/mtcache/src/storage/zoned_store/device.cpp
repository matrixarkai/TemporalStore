#include "device.h"

#include "common/logging.h"
#include "zone_manager.h"

#ifdef HAS_BBTHREAD
#include "bbthread/bthread_cpp.h"
#endif

#include <gflags/gflags.h>
#include <linux/fs.h>
#include <sys/ioctl.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <string.h>
#include <unistd.h>
#include <utility>

namespace mtcache {

DECLARE_bool(zonedstore_use_async_read);

#define MIN_DEVICE_CAPACITY (4UL << 30)

int DeviceHandle::Open(const char* filename, uint64_t user_cap, int mode) {
  uint64_t capacity = user_cap;
  if (user_cap < MIN_DEVICE_CAPACITY) {
    capacity = MIN_DEVICE_CAPACITY;
    LOG(INFO) << "device capacity is smaller than requirement:" << MIN_DEVICE_CAPACITY
              << ", actual: " << user_cap
              << ", expand to: " << capacity;
  }
  // bytedisk can't allocate a namespace for whos capacity < 1 zone.
  // zone size is 1GB in bytedisk by default.
  // uint64_t reserved_space = 2UL << 30;

  // If target file is not a block device and doesn't exist, we create a new
  // one. We simple check if the filename is prefixed by `/dev/` to distinguish
  // whether its a block device or not
  std::filesystem::path filepath(filename);
  is_block_dev_ = std::filesystem::is_block_file(filepath);
  if (!is_block_dev_) {
    if (!std::filesystem::exists(filepath)) {
      LOG(INFO) << "device file doesn't exist, create one";
      filepath = filepath.lexically_normal() / "ZoneStoreDevFile";
      std::error_code ec;
      std::filesystem::create_directories(filepath.parent_path().string(), ec);
      if (ec) {
        LOG(FATAL) << "Fail to create dirctory for ssd cache, path=["
                   << filepath.string()
                   << "], error msg is: " << google::StrError(ec.value());
      }
    }
    if (std::filesystem::is_directory(filepath)) {
      filepath = filepath.lexically_normal() / "ZoneStoreDevFile";
    }
    auto f =
        open(filepath.c_str(), O_RDWR | O_DIRECT | O_CREAT, S_IRUSR | S_IWUSR);
    if (f == -1) {
      LOG(FATAL) << "Failed to create file: " << filepath.c_str() << " : "
                 << strerror(errno);
      return -1;
    }

    // Allocate file space, the actual device capacity should always larger
    // than user's requirement, here we set allocate an extra space.
    //
    // TODO(guokuankuan) We should remove all hard coded values soon.
    // int ret = fallocate(f, 0, 0, capacity + reserved_space);
    int ret = fallocate(f, 0, 0, capacity);
    LOG(INFO) << "fallocate device file size: " << capacity;
    if (ret != 0) {
      std::filesystem::remove(filepath);
      LOG(FATAL) << "Failed to fallocate " << filepath.c_str() << " : "
                 << strerror(errno);
      return -1;
    }
    close(f);
  }

  // For using pread.
  dev_fd_ = open(filepath.c_str(), O_RDWR | O_DIRECT);
  if (dev_fd_ == -1) {
    LOG(FATAL) << "Failed to POSIX::open " << filepath.c_str() << " : "
               << strerror(errno);
  }

  // Device Name
  std::string dev_name;
  dev_name.append(filepath).append("?async=libaio");

  // Device Handle
  dev_handle_ = bytedisk_open_dev(dev_name.c_str());
  if (dev_handle_ == 0) {
    LOG(FATAL) << "Failed to open ByteDisk device! " << strerror(errno);
  }

  // Device Type
  dev_type_ = bytedisk_get_dev_type(dev_handle_);

  // Device Capacity
  uint64_t zone_size = bytedisk_get_dev_zone_size(dev_handle_);
  uint64_t zone_cnt = bytedisk_get_dev_zone_cnt(dev_handle_);

  /*
  if (is_block_dev_ && capacity > 0) {
    zone_cnt = (capacity + reserved_space) / zone_capacity;
  }
  */

  info.device_size = zone_size * zone_cnt;


  LOG(INFO) << "expected avaliable device size: " << info.device_size
  	        << ", actual bytedisk device size: " 
            << bytedisk_get_dev_size(dev_handle_);
  if (info.device_size > bytedisk_get_dev_size(dev_handle_)) {
    LOG(FATAL) << "expected avaliable device size too large";
    return -1;
  }

  // Open namesapce.
  ns_handle_ = bytedisk_allocate_namespace(dev_handle_, 0, info.device_size);
  ns_handle_ = bytedisk_reset_namespace(ns_handle_);
  if (ns_handle_ == 0) {
    LOG(FATAL) << "Failed to open ByteDisk namespace! " << strerror(errno);
  }

  // Device Information.
  info.page_size = 4096;
  info.nr_zones_in_device = bytedisk_get_ns_nr_zones(ns_handle_);
  info.zone_size = bytedisk_get_dev_zone_size(dev_handle_);
  info.zone_capacity = bytedisk_get_dev_zone_cap(dev_handle_);
  info.group_size = info.zone_size;
  info.nr_groups_in_device = info.nr_zones_in_device;
  info.nr_zones_in_group = info.group_size / info.zone_size;
  info.device_capacity = info.zone_capacity * zone_cnt;;

  zone_mode_ = ZoneMode::LARGE;

  LOG(INFO) << "Open device success!"
            << ", zone_cnt: " << zone_cnt
            << ", nr_zones_in_device: " << info.nr_zones_in_device
            << ", device_cap: " << info.device_capacity
            << ", device_size: " << info.device_size;
  return 0;
}

int DeviceHandle::Close() {
  // free namespace
  bytedisk_free_namespace(ns_handle_);

  // close device
  bytedisk_close_dev(dev_handle_);

  close(dev_fd_);
  return 0;
}

int DeviceHandle::OpenZone(off_t offset) {
  uint64_t zone_id = offset / info.zone_size;

  // Open Zone.
  bytedisk_zone_handle_t zone_handle = bytedisk_zone_get(ns_handle_, zone_id);
  int ret = bytedisk_zone_exp_open(zone_handle);
  if (ret != 0) {
    LOG(FATAL) << "Failed to open zone! " << strerror(errno);
  }

  // check state
  bytedisk_zone_state zone_state = bytedisk_get_zone_state(zone_handle);
  if (zone_state != bytedisk_zone_state::BYTEDISK_ZND_STATE_EOPEN) {
    LOG(FATAL) << "Zone is not opened! " << zone_id;
  }
  return 0;
}

int DeviceHandle::CloseZone(off_t offset) {
  uint64_t zone_id = offset / info.zone_size;
  LOG(INFO) << "Close Zone: " << zone_id
            << ", Zone wp: " << (*zones_)[zone_id].wp_
            << ", Zone valid bytes: " << (*zones_)[zone_id].valid_bytes_;

  auto zone_handle = bytedisk_zone_get(ns_handle_, zone_id);

  // Check state
  bytedisk_zone_state zone_state = bytedisk_get_zone_state(zone_handle);
  if (bytedisk_zone_state::BYTEDISK_ZND_STATE_FULL == zone_state ||
      bytedisk_zone_state::BYTEDISK_ZND_STATE_EMPTY == zone_state) {
    return 0;
  }

  // Close zone
  int ret = bytedisk_zone_close(zone_handle);
  if (ret != 0) {
    LOG(FATAL) << "Failed to close zone! " << strerror(errno);
  }

  // Check state
  zone_state = bytedisk_get_zone_state(zone_handle);
  if (bytedisk_zone_state::BYTEDISK_ZND_STATE_CLOSED != zone_state) {
    LOG(FATAL) << "Zone is not closed! " << zone_id;
  }

  // Finish zone
  ret = bytedisk_zone_finish(zone_handle);
  if (ret != 0) {
    LOG(FATAL) << "Failed to finish zone! " << strerror(errno);
  }

  // Check state
  zone_state = bytedisk_get_zone_state(zone_handle);
  if (bytedisk_zone_state::BYTEDISK_ZND_STATE_FULL != zone_state) {
    LOG(FATAL) << "Zone is not filled! " << zone_id;
  }

  return 0;
}

int DeviceHandle::ResetZone(off_t offset) {
  uint64_t zone_id = offset / info.zone_size;
  bytedisk_zone_handle_t zone_handle = bytedisk_zone_get(ns_handle_, zone_id);

  // Reset zone
  int ret = bytedisk_zone_reset(zone_handle);
  if (ret != 0) {
    LOG(FATAL) << "Failed to reset zone! " << strerror(errno);
  }

  // Check state
  bytedisk_zone_state zone_state = bytedisk_get_zone_state(zone_handle);
  if (zone_state != bytedisk_zone_state::BYTEDISK_ZND_STATE_EMPTY) {
    LOG(FATAL) << "Zone is not reset! " << zone_id;
  }
  return 0;
}

int DeviceHandle::Read(char* buf, int size, off_t offset) {
  // Read size is limited
  uint64_t left = size;
  uint64_t max_read_size = 1ul << 20;
  char* p = buf;
  [[maybe_unused]] char* limit = p + size;
  while (left > 0) {
    uint64_t read_size = (max_read_size < left) ? max_read_size : left;

#ifdef HAS_BBTHREAD
    if (FLAGS_zonedstore_use_async_read) {
      while (true) {
        folly::Promise<bytedisk_io_status_code> read_promise;
        int ret = bytedisk_async_read(
            ns_handle_, offset, p, read_size,
            [](bytedisk_io_status_code status, void* cb_arg) {
              auto* promise =
                  reinterpret_cast<folly::Promise<bytedisk_io_status_code>*>(
                      cb_arg);
              promise->setValue(status);
            },
            &read_promise);
        if (ret == -EBUSY || ret == -EAGAIN) {
          continue;
        }
        if (ret < 0) {
          LOG(FATAL) << "Failed to read data! " << ret;
        }
        bytedisk_io_status_code read_status =
            bbthread::future_get(read_promise.getFuture());
        if (read_status != BYTEDISK_IO_SC_SUCCESS) {
          LOG(FATAL) << "Failed to read data! " << int(read_status);
        }
        break;
      }
    } else {
#else
    {
#endif
      // int ret = bytedisk_zone_sync_read(ns_handle_, offset, p, read_size);
      int ret = pread(dev_fd_, p, read_size, offset);
      if (ret != read_size) {
        LOG(FATAL) << "Failed to read data! " << strerror(errno);
      }
    }

    offset += read_size;
    p += read_size;
    left -= read_size;
  }
  assert(p == limit);
  return 0;
}

int DeviceHandle::Write(const char* buf, int size, off_t offset) {
  uint64_t zone_id = offset / info.zone_size;
  bytedisk_zone_handle_t zone_handle = bytedisk_zone_get(ns_handle_, zone_id);

  // Check state
  bytedisk_zone_state zone_state = bytedisk_get_zone_state(zone_handle);
  if (zone_state != bytedisk_zone_state::BYTEDISK_ZND_STATE_EOPEN) {
    LOG(FATAL) << "Zone is not opened! " << zone_id;
  }

  // Check write pointer
  /*
  if (offset != bytedisk_get_zone_writepointer(zone_handle)) {
    LOG(FATAL) << "Offset is not equal to write pointer! " << zone_id;
  }
  */

  // Write size is limited.
  uint64_t left = size;
  uint64_t max_flush_size = 1ul << 20;
  char* p = const_cast<char*>(buf);
  [[maybe_unused]] char* limit = p + size;
  while (left > 0) {
    uint64_t flush_size = (max_flush_size < left) ? max_flush_size : left;
    // int ret = bytedisk_zone_sync_write(ns_handle_, offset, p, flush_size);
    int ret = pwrite(dev_fd_, p, flush_size, offset);
    if (ret != flush_size) {
      LOG(FATAL) << "Failed to write data! " << strerror(errno);
    }
    offset += flush_size;
    p += flush_size;
    left -= flush_size;
  }
  assert(p == limit);
  return 0;
}

int DeviceHandle::InitZones(Zone** zones) {
  LOG(INFO) << "Initial Zones ...";
  *zones = new Zone[info.nr_zones_in_device];
  for (int i = 0; i < info.nr_zones_in_device; i++) {
    bytedisk_zone_handle_t zone_handle = bytedisk_zone_get(ns_handle_, i);
    (*zones)[i].start_ = bytedisk_get_zone_start(zone_handle);
    (*zones)[i].wp_ = bytedisk_get_zone_writepointer(zone_handle);
    (*zones)[i].capacity_ = bytedisk_get_zone_capacity(zone_handle);
    (*zones)[i].size_ = bytedisk_get_dev_zone_size(dev_handle_);
    (*zones)[i].valid_bytes_ = 0;
  }
  zones_ = zones;
  return 0;
}

ZoneMode DeviceHandle::GetZoneMode() { return zone_mode_; }

// TODO(guokuankuan) We need to remove this helper function and large/small zone
// abstraction.
std::shared_ptr<DeviceHandle> NewDevice(const char* path, uint64_t capacity,
                                        int mode) {
  auto dev = std::make_shared<DeviceHandle>();
  dev->Open(path, capacity, mode);
  return dev;
}

std::shared_ptr<ZoneManager> NewZoneManager(std::shared_ptr<DeviceHandle> dev,
                                            bool using_existing_db) {
  // TODO(guokuankuan) We use LargeZone mode as default for now, need to remove
  // small zone mode.
  return std::make_shared<ZoneManagerLargeModeImpl>(std::move(dev),
                                                    using_existing_db);
}

}  // namespace mtcache
