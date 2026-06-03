#pragma once

#include "device.h"
#include <condition_variable>

namespace mtcache {

/**
 * ZoneManager is used to manager metadata of zones and provide read/write api.
 * Furthermore, ZoneManager provides messages for gc thread.
 */
class ZoneManager {
 public:
  explicit ZoneManager(std::shared_ptr<DeviceHandle> dev) : dev_(dev) {}

  virtual ~ZoneManager() {}

  // Append buffer to the disk and set the written LBA offset to `offset`.
  // Return 0 if success.
  virtual int Append(const char* buf, int sz, DataType type,
                     uint64_t* offset) = 0;

  // Read data into buf. Note that the passed-in `buf` should be 4KB aligned and
  // pre-allocated, Return 0 if success
  virtual int Read(char* buf, uint64_t offset, int sz) = 0;

  // Check if current group has enough space for both data & related meta.
  // Note that even the caller ensured the capacity, it may not append data
  // immedately, so we should tracke the claimed space here.
  // Return 1 if success & ready for data writing.
  // Return 0 if no enough space in current group.
  virtual int EnsureAvailableSpace(int data_size, int meta_size) = 0;

  // Finish current group and open a new group.
  // Return 0 if success, and -1 otherwise.
  virtual int FinishGroup() = 0;

  // Locate the Zone with 'offset' and subtract its valid bytes with 'size'.
  virtual void TrimBytes(uint64_t offset, int size) = 0;

  // Return a group that has largest garbage ratio.
  // If there is no group in gc_list, return -1.
  virtual std::pair<int16_t, GCMode> FindGCGroup() = 0;

  // Reset the specified group
  // Return 0 if success.
  virtual int ResetGroup(uint16_t group_id) = 0;

  // Load the all zone metadata in the specified group into 'buf'.
  virtual int LoadMetaData(int group_id,
                           GCWorker::LoadMetaCallback meta_callback) = 0;
  // Return the property via this method.
  // If 'property' can be understood, fill '*result' and return true.
  //
  // Properties include:
  // "device", "group", "garbage"
  virtual bool GetProperty(std::string property, std::string* result) = 0;

  // Recover zone manager.
  virtual void Recovery(std::function<int(const char* buf)> meta_cb) = 0;

  // Return zone mode
  virtual ZoneMode GetZoneMode() const { return dev_->GetZoneMode(); }

  // Return zone size
  virtual uint64_t GetZoneSize() const { return dev_->info.zone_size; }

  // Return zone capacity
  virtual uint64_t GetZoneCapacity() const { return dev_->info.zone_capacity; }

  // Return group size
  virtual uint64_t GetGroupSize() const { return dev_->info.group_size; }

  // Return used size
  virtual uint64_t GetUsedSpace() = 0;

  // Return garbage bytes
  virtual uint64_t GetGarbageBytes() { return 0; };

  // Return total capacity
  virtual uint64_t GetCapacity() const { return dev_->info.device_capacity; }

 protected:
  // device handle
  std::shared_ptr<DeviceHandle> dev_;
};

class ZoneManagerLargeModeImpl : public ZoneManager {
  /**
   * One group contains one large zone.
   * One large zone is partitioned into two part.
   * | data part | meta part |
   *
   * Note, for consistency,
   *  data part is called as data zone.
   *  meta part is called as meta zone.
   */
  class ZoneGroup {
   public:
    ZoneGroup(DeviceHandle* dev, uint32_t gid, Zone* zones)
        : dev_(dev),
          group_id_(gid),
          zones_(zones),
          zone_size_(dev->info.zone_size),
          zone_capacity_(dev->info.zone_capacity),
          meta_offset_(0),
          meta_size_(0),
          garbage_bytes_(0) {}

    virtual ~ZoneGroup() {}

    // Return the group id.
    uint16_t GroupID() { return group_id_; }

    Zone* AllocateZone(DataType type) {
      if (type == DataType::DATA) {
        dev_->OpenZone(zones_[0].start_);
        return &zones_[0];
      } else {
        assert(type == DataType::META_LOG);
        Zone* z = new Zone();
        z->capacity_ = 128UL << 20;
        z->size_ = z->capacity_;
        z->start_ = 0;
        z->wp_ = 0;
        z->valid_bytes_ = 0;
        return z;
      }
    }

    void CloseZone(Zone* z) { dev_->CloseZone(z->start_); }

    // Locate the Zone with 'offset' and subtract its valid bytes with 'size'.
    void Trim(uint64_t off_in_group, int size) {
      uint32_t zid = off_in_group / zone_size_;
      zones_[zid].valid_bytes_ -= size;

      // update garbage bytes.
      garbage_bytes_ += size;
    }

    // Return current group's garbage rate.
    double GetGarbageRate() const {
      return (static_cast<double>(garbage_bytes_) / zone_size_);
    }

    uint64_t GetGarbageBytes() const { return garbage_bytes_; }

    // Set garbage bytes (only used in recovery)
    void SetGarbageBytes(uint64_t nbytes) { garbage_bytes_ = nbytes; }

    // Reset the zone.
    void Reset() {
      zones_[0].wp_ = zones_[0].start_;
      zones_[0].valid_bytes_ = 0;
      dev_->ResetZone(zones_[0].start_);
    }

    // Return the meta's start location.
    uint64_t GetMetaOffset() { return meta_offset_; }

    // Set the meta's start location.
    void SetMetaOffset(uint64_t offset) { meta_offset_ = offset; }

    // Return the meta's size.
    uint64_t GetMetaSize() { return meta_size_; }

    // Set the meta's start location.
    void SetMetaSize(uint64_t size) { meta_size_ = size; }

   protected:
    // Device
    DeviceHandle* dev_;

    // ZoneGroup uinque id.
    uint16_t group_id_;

    // Increasing order based on their LBA
    Zone* zones_;

    uint64_t zone_size_;

    uint64_t zone_capacity_;

    uint64_t meta_offset_;

    uint64_t meta_size_;

    // Size of grabage.
    uint64_t garbage_bytes_;
  };

 private:
  // all zones.
  Zone* zones_;

  // all groups.
  std::vector<ZoneGroup*> group_set_;

  // Used to sync free_list
  std::mutex free_list_lk_;

  std::condition_variable free_list_cv_;

  // Used to sync gc_list
  std::mutex gc_list_lk_;

  // This list is used to store the ids of free zone groups.
  std::list<int> free_list_;

  // Zone Groups sorted by their garbage ratio (higher first).
  std::list<ZoneGroup*> gc_list_;

  std::list<ZoneGroup*> recovery_list_;

  // Current available group
  ZoneGroup* group_;

  // Current available zone for user data.
  Zone* data_zone_;

  // Current available zone for meta data.
  Zone* meta_zone_;

  const uint64_t header_size;

  const uint64_t footer_size;

  // check if meta zone is appended more than once.
  bool is_appned_once = true;

  // check if 'EnsureAppendSpace' is called before 'Append'.
  // Note that 'FinishGroup' will reset it.
  bool is_ensure = false;

  // sequence number
  uint64_t sequence = 0;

  uint64_t magic_number = 20220209;

 public:
  explicit ZoneManagerLargeModeImpl(std::shared_ptr<DeviceHandle> dev,
                                    bool using_existing_db);

  ~ZoneManagerLargeModeImpl();

  int Append(const char* buf, int size, DataType type,
             uint64_t* offset) override;

  int Read(char* buf, uint64_t offset, int size) override;

  int EnsureAvailableSpace(int data_size, int meta_size) override;

  int FinishGroup() override;

  void TrimBytes(uint64_t off_in_dev, int size) override;

  std::pair<int16_t, GCMode> FindGCGroup() override;

  int ResetGroup(uint16_t group_id) override;

  int LoadMetaData(int group_id,
                   GCWorker::LoadMetaCallback meta_callback) override;

  // Initialize recovery_list_ & free_list_;
  // Return 0 if succees, otherwise -1.
  int PickRecoverableGroupsForReopen();

  void Recovery(std::function<int(const char* buf)> index_cb) override;

  // add a header (4 KB) at the head of zone.
  void AddHeader(Zone* z, uint64_t seq_num, uint64_t magic);

  // add a footer (4 KB) at the tail of zone.
  void AddFooter(Zone* z, uint64_t offset, uint64_t size);

  bool GetProperty(std::string property, std::string* result) override;

  uint64_t GetGarbageBytes() override;

  uint64_t GetUsedSpace() override;
};
}  // namespace mtcache
