#pragma once

#include "storage/storage_engine.h"

namespace mtcache {

class RecoverDataCallbackMock : public StorageEngine::RecoverDataCallback {
 public:
  RecoverDataCallbackMock() = default;
  ~RecoverDataCallbackMock() = default;

  void OnRecoverData(const std::string& key,
                     CacheBufferSharedPtr buffer) override {
    LOG(INFO) << "callback key=" << key;
    last_recover_key_ = key;
    recovered_record_cnt_.fetch_add(1);
  }

  std::string GetLastRecoverKey() { return last_recover_key_; }
  int64_t GetRecoveredRecordCnt() { return recovered_record_cnt_.load(); }

 private:
  std::string last_recover_key_{""};
  std::atomic<int64_t> recovered_record_cnt_{0};
};

}  // namespace mtcache
