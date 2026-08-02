#pragma once

#include "storage/zoned_store/gc.h"
#include "storage/zoned_store/metrics.h"

#include <folly/io/IOBuf.h>
#include <gflags/gflags.h>
#include <noodle/base/result.h>

#include <atomic>
#include <functional>
#include <libbytedisk.h>
#include <list>
#include <mutex>
#include <vector>

namespace mtcache {

class WriteBuffer;
class ZoneManager;
class ZoneManagerLargeMode;

// ZoneManager supports two zone modes:
//
// Small zone mode: one group contains multiple zones
// Large zone mode: one group contains one zone.
//
// Note that, in the large zone mode, one zone is partitioned into two regions.
// 1st region is used to store user data.
// 2nd region is used to store meta data.
//
// Currently, there are two types of zns devices. For WD ZNS, the zone size is
// large (e.g., 1 GB) and large mode is used. For Samsung ZNS, the zone size is
// relatively small (e.g., 128 MB) and small mode is used.
//
// ZoneManager determines the zone mode according to the zone size.
//
enum ZoneMode : uint8_t { SMALL = 1, LARGE = 10 };

// When append data into a ZoneGroup, we should first identify whether it is
// user data or op log.
enum DataType { DATA = 1, META_LOG = 2 };

// EMPTY --(write)--> OPEN --(write or finish)--> FULL --(reset)--> EMPTY
enum ZoneStatus { EMPTY = 1, OPEN = 2, FULL = 3, CLOSE = 4, OFFLINE = 10 };

//  ZoneManager supports two gc modes.
// `LOSSY` means that when reseting a zone group, it is directly erased.
// `LOSSLESS` means that pinned data needs to be rewritten.
enum GCMode : uint8_t { LOSSY = 1, LOSSLESS = 10 };

class Zone {
 public:
  // Return the size of available free space in the zone.
  int AvailBytes() { return capacity_ - (wp_ - start_); }

  ZoneStatus status() { return status_; }

  // Zone's LBA
  uint64_t start_ = 0;

  // Zone size
  uint64_t size_ = 0;

  // Zone writeable size
  uint64_t capacity_ = 0;

  // Zone write point(byte)
  uint64_t wp_ = 0;

  // capacity_ and valid_bytes_ indicate garbage info
  uint64_t valid_bytes_ = 0;

  // Zone status
  ZoneStatus status_ = ZoneStatus::EMPTY;
};

/**
 * This class is used to store information of device and provide access to
 * physical devices.
 *
 * All devices are seen as zoned devices in this system.
 */
class DeviceHandle {
  friend class ZoneManager;
  friend class ZoneManagerLargeMode;
  friend class ZoneManagerSmallMode;

 public:
  DeviceHandle() {}

  ~DeviceHandle() {}

  struct DeviceInfo {
    // Size in bytes of a device.
    uint64_t device_size;

    // Capacity in bytes of a device.
    uint64_t device_capacity;

    // Size in bytes of a page.
    uint64_t page_size;

    // Size in bytes of a zone.
    uint64_t zone_size;

    // Capacity in bytes of a zone.
    uint64_t zone_capacity;

    // Number of zones in a device.
    uint64_t nr_zones_in_device;

    // Size in bytes of a group.
    uint64_t group_size;

    // Number of groups in a device.
    uint64_t nr_groups_in_device;

    // Number of zones in a group.
    uint64_t nr_zones_in_group;
  };

  // Open a device specified by name, capacity and mode.
  //
  // The length of `filename` must be less than 64B. If `filename` is prefixed
  // with '/dev/', we open the raw device. If not, we open the file. `capacity`
  // is physical size (not LBA). If `capacity` == 0, we use the entire device.
  // For now, `mode` is not used.
  //
  // Return 0 if success.
  int Open(const char* filename, uint64_t capacity, int mode);

  // Close the device
  // Return 0 if success.
  int Close();

  // Open a zone specified by offset.
  // Return 0 if success, and -1 otherwise.
  int OpenZone(off_t offset);

  // Close a zone specified by offset.
  // Return 0 if success, and -1 otherwise.
  int CloseZone(off_t offset);

  // Reset a zone specified by offset.
  // Return 0 if success, and -1 otherwise.
  int ResetZone(off_t offset);

  // Read data into 'buf'.
  // Return 0 if success, and -1 otherwise.
  int Read(char* buf, int size, off_t offset);

  // Write data into disk.
  // Return 0 if success, and -1 otherwise.
  int Write(const char* buf, int size, off_t offset);

  // Initialize zones
  // Return 0 if success, and -1 otherwise.
  int InitZones(Zone** zones);

  // Return zone mode.
  ZoneMode GetZoneMode();

 public:
  // Device information
  DeviceInfo info;

 private:
  // device handle
  bytedisk_dev_handle_t dev_handle_;

  // namespace handle
  bytedisk_ns_handle_t ns_handle_;

  // file simulation
  bool is_block_dev_;

  // device type
  bytedisk_device_type dev_type_;

  // zone mode
  ZoneMode zone_mode_;

  // posix API need this
  int dev_fd_;

  // Total zones within current device
  Zone** zones_ = nullptr;
};


// Create a new device
extern std::shared_ptr<DeviceHandle> NewDevice(const char* path,
                                               uint64_t capacity, int mode);

// Create an instance of the zone manager.
extern std::shared_ptr<ZoneManager> NewZoneManager(
    std::shared_ptr<DeviceHandle> dev, bool using_existing_db);

}  // namespace mtcache
