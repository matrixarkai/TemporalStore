#pragma once

#include "storage/zoned_store/buffer_manager.h"

#include <folly/io/IOBuf.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace mtcache {

class ZoneManager;
class IndexUpdater;
class GCWorker {
 public:
  using LoadMetaCallback = std::function<int(const char* buf)>;

 public:
  GCWorker(std::shared_ptr<BufferManager> bm_ptr,
           std::shared_ptr<ZoneManager> zone_manager_ptr,
           std::shared_ptr<BufferEncoder> encoder,
           std::shared_ptr<IndexUpdater> index_updater, int max_record_length);
  ~GCWorker();

  // Begin to do background gc job.
  void Start();

  // Stop doing background gc job.
  void Stop();

  // Interface for other class to mannually ask `GCWorker` to do gc.
  int Notify();

  // Return Whether gc system is running.
  bool GCEnabled() { return enabled_gc_.load(std::memory_order_acquire); }

  // Run in background thread.
  // When wake up by timer or other class, it tries to do gc work on given
  // zone.
  // gc work is divided into:
  // - lossy
  // - lossless
  // GC => ProcessMetadata => ConstructSingleRecord.
  int GC();

  // @oplogs: oplog sequences, refer to codec.h for layout. Memory area
  // to which oplogs points only contains one zone data.
  // Process op log one by one, optionally recreate user data and
  // push it to `BufferManager`.
  // ------
  // TODO(fangliming): migrate according to entry's state, currently we
  // remove all valid records from zonedstore's index.
  // `kSoftDel` => must drop.
  // `kNormal` => drop if zone's recycling mode is lossy.
  // `kPinned` => never drop.
  int ProcessMetadata(const char* oplogs, bool is_lossy) const;

  // Sometimes we only need to extract keys and remove them from index.
  const char* ConstructSingleKey(const char* oplog, std::string& key) const;

  // Construct single user data from oplog and push to queue.
  // Return next oplog pointer starting address.
  // We don't double check `checksum`.
  const char* ConstructSingleRecord(const char* oplog,
                                    WriteBuffer::BufferDataType& data) const;

 private:
  std::shared_ptr<BufferManager> buffer_manager_;

  std::shared_ptr<BufferEncoder> encoder_;

  std::shared_ptr<ZoneManager> zone_manager_;

  std::shared_ptr<IndexUpdater> index_updater_;

  std::thread background_thread_;

  // Set by `Start`.
  std::atomic<bool> enabled_gc_;

  std::mutex notify_lock_;
  std::condition_variable notify_cv_;

  // Reused in `ConstructSingleRecord`.
  char* record_buf_;
};

}  // namespace mtcache
