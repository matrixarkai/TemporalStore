#include "cache_wrapper.h"

#include "common/logging.h"

DECLARE_int32(ssd_engine_type);
DECLARE_bool(cache_enable_eviction_handler);

namespace mtcache {
memcached_st* MemcachedWrapper::get_client() {
  std::thread::id id = std::this_thread::get_id();

  auto it = clients_.find(id);
  if (it != clients_.end()) {
    return it->second;
  }

  std::lock_guard<std::mutex> lock(m_);

  memcached_return rc;
  memcached_st* memc = memcached_create(NULL);

  if (memcached_server_list_count(servers_) == 0) {
    printf("Init new server address\n");
    servers_ = memcached_server_list_append(servers_, "localhost", 11211, &rc);
  }
  rc = memcached_server_push(memc, servers_);

  if (rc == MEMCACHED_SUCCESS) {
    clients_[id] = memc;
    std::ostringstream oss;
    oss << id << std::endl;
    printf("Init memcached client successfully! id = %s\n", oss.str().c_str());
  } else {
    printf("Couldn't add server: %s\n", memcached_strerror(memc, rc));
    exit(0);
  }
  return memc;
}

void MemcachedWrapper::Insert(const std::string& key, std::string value,
                              size_t size) {
  auto memc = get_client();
  memcached_return rc;
  rc = memcached_set(memc, key.data(), key.size(), value.data(), value.size(),
                     (time_t)0, (uint32_t)0);
  if (rc != MEMCACHED_SUCCESS) {
    printf("Couldn't store key: %s\n", memcached_strerror(memc, rc));
    exit(0);
  }
}

std::optional<std::string> MemcachedWrapper::Lookup(const std::string& key) {
  auto memc = get_client();
  memcached_return rc;
  uint64_t vsize = 0;
  uint32_t flags = 0;
  char* retrieved_value =
      memcached_get(memc, key.data(), key.size(), &vsize, &flags, &rc);
  if (rc == MEMCACHED_SUCCESS) {
    std::string value = std::string(retrieved_value, vsize);
    free(retrieved_value);
    return value;
  }
  return std::nullopt;
}

FlexibleCache::FlexibleCache(uint64_t capacity, const std::string& policy,
                             const std::string& engine,
                             const std::vector<std::string>& pmem_paths,
                             const std::vector<std::string>& ssd_paths) {
  // Init different CacheInstance based on passed-in policy and engine.
  static std::map<std::string, ReplacementPolicyType> polices{
      {"slru", ReplacementPolicyType::kSLRU},
      {"fifo", ReplacementPolicyType::kFIFO}};

  static std::map<std::string, StorageEngineType> engines{
      {"dram", StorageEngineType::kDRAMStorageEngine},
      {"pmem", StorageEngineType::kPMEMStorageEngine},
      {"ssd_terarkdb", StorageEngineType::kSSDTerarkDBStorageEngine},
      {"simple", StorageEngineType::kSimpleStorageEngine},
      {"multissd", StorageEngineType::kMultiSSDStorageEngine},
      {"ssd_zonedstore", StorageEngineType::kSSDZonedStoreStorageEngine}};

  std::map<std::string, std::vector<std::string>> storage_paths{
      {"dram", {""}},
      {"ssd_terarkdb", {ssd_paths[0] + "/mtcache_bench_ssd"}},
      {"ssd_zonedstore", {ssd_paths[0]}},
      {"simple", {""}}};

  if (engines[engine] == StorageEngineType::kPMEMStorageEngine) {
    for (const auto& path : pmem_paths) {
      paths_.push_back(path + "/mtcache_bench_pmem");
    }
  } else if (engines[engine] == StorageEngineType::kMultiSSDStorageEngine) {
    for (const auto& path : ssd_paths) {
      // Default: Multi ZonedStore may run on raw device
      paths_.push_back(path);
    }
  } else {
    paths_ = storage_paths[engine];
  }

  if (polices.find(policy) == polices.end() ||
      engines.find(engine) == engines.end()) {
    std::cout << "Wrong policy or engine type!" << std::endl;
    exit(1);
  }

  // Init Registry
  registry_ = noodle::GetMetricRegistry("ti.mtcache.bench");
  zoned_store_registry_ = noodle::GetMetricRegistry("ti.mtcache.zonedstore");
#ifdef BUILD_SSD_CACHE
  ZonedStoreMetrics::instance()->Start(zoned_store_registry_);
#endif

  engine_ = engines[engine];
  instance_ = std::make_unique<CacheInstance>(capacity, polices[policy],
                                              engines[engine], paths_);
  instance_->SetMetricRegistry(registry_);
  instance_->Start();
}

void FlexibleCache::Insert(const std::string& key, std::string value,
                           size_t size) {
  DEBUG_TIME_TRACE_START("1. FlexCache::Insert::bufCopy");
  std::unique_ptr<folly::IOBuf> val_buf = folly::IOBuf::copyBuffer(value);
  DEBUG_TIME_TRACE_END("1. FlexCache::Insert::bufCopy");

  DEBUG_TIME_TRACE_START("2. FlexCache::Insert::instance_::Put()");
  auto rst = instance_->Put(key, std::move(*val_buf));
  DEBUG_TIME_TRACE_END("2. FlexCache::Insert::instance_::Put()");
  if (!rst.IsOk()) {
    printf("Insert duplicate keys, exit!\n");
    exit(0);
  }
}

std::optional<std::string> FlexibleCache::Lookup(const std::string& key) {
  auto rst = instance_->Get(key);
  if (!rst.IsOk()) {
    return std::nullopt;
  }
  auto item_ptr = rst.Get();
  if (item_ptr == nullptr) {
    return std::nullopt;
  }
  return std::string(item_ptr->Data(), item_ptr->Size());
}

void FlexibleCache::CalculateSpaceAmplification() const {
  uint64_t used_size = 0;
  uint64_t valid_size = 0;

  if (Size() == 0) {
    printf("No Write operation\n");
    return;
  }
  if (engine_ == StorageEngineType::kSSDTerarkDBStorageEngine) {
    std::string cmd("du -sB1 ");
    cmd.append(paths_[0]);
    cmd.append(" | cut -f1");

    // execute above command and get the output
    FILE* stream = popen(cmd.c_str(), "r");
    if (stream) {
      const int max_size = 256;
      char readbuf[max_size];
      if (fgets(readbuf, max_size, stream) != NULL) {
        char* end;
        used_size = strtoull(readbuf, &end, 10);
        valid_size = Size();
      }
      pclose(stream);
    }
  } else if (engine_ == StorageEngineType::kSSDZonedStoreStorageEngine) {
    used_size = dynamic_cast<StorageEngineZonedStore*>(
                    instance_->TEST_GetStorageEngine())
                    ->GetDiskUsedSpace();
    valid_size = Size();
  } else {
    printf("Space amplification is not calculated for this engine!");
  }

  if (used_size > 0 && valid_size > 0) {
    printf("Disk Used (MB): %lu, Valid Size(MB): %zu\n", used_size >> 20,
           valid_size >> 20);
    printf("Space amplification: %.2f\n", (double)used_size / valid_size);
  }
}

MultiTierCache::MultiTierCache(
    uint64_t dram_capacity, uint64_t pmem_capacity, uint64_t ssd_capacity,
    const std::string& policy, const std::vector<std::string>& pmem_paths,
    const std::vector<std::string>& ssd_paths,
    std::string& dram_pmem_data_placement_type, bool enable_eviction,
    size_t side_by_side_dram_pmem_placement_threshold,
    std::string& ssd_storage_engine) {
  // Init different UnifiedCache based on passed-in policy and
  // cache_data_placement_types.
  static std::map<std::string, std::string> polices{{"slru", "SLRU"},
                                                    {"fifo", "FIFO"}};

  static std::map<std::string, std::string> cache_data_placement_types{
      {"sidebyside", "SideBySide"}, {"tiered", "Tiered"}};

  static std::map<std::string, int32_t> ssd_engines{{"ssd_terarkdb", 0},
                                                    {"ssd_zonedstore", 1}};

  if (polices.find(policy) == polices.end() ||
      cache_data_placement_types.find(dram_pmem_data_placement_type) ==
          cache_data_placement_types.end() ||
      ssd_engines.find(ssd_storage_engine) == ssd_engines.end()) {
    std::cout << "Wrong policy or dram_pmem_data_placement_type or ssd_engine!"
              << " policy = " << policy << " dram_pmem_data_placement_type = "
              << dram_pmem_data_placement_type
              << " ssd_storage_engine = " << ssd_storage_engine << std::endl;
    exit(1);
  }

  for (const auto& path : pmem_paths) {
    pmem_paths_.push_back(path + "/mtcache_bench_pmem");
  }

  for (const auto& path : ssd_paths) {
    ssd_paths_.push_back(path);
  }

  CacheOptions multi_cache_opts{
      .dram_capacity = dram_capacity,
      .pmem_capacity = pmem_capacity,
      .ssd_capacity = ssd_capacity,
      .pmem_paths = pmem_paths_,
      .ssd_paths = ssd_paths_,
      .cache_dram_replacement_policy = polices[policy],
      .cache_pmem_replacement_policy = polices[policy],
      .cache_ssd_replacement_policy = polices[policy],
      .cache_dram_pmem_data_placement_type =
          cache_data_placement_types[dram_pmem_data_placement_type],
      .cache_dram_pmem_data_placement_threshold =
          side_by_side_dram_pmem_placement_threshold,
      .metric_id_prefix = "ti.mtcache.cache_wrapper"};

  FLAGS_ssd_engine_type = ssd_engines[ssd_storage_engine],
  FLAGS_cache_enable_eviction_handler = enable_eviction;

  cache_ = std::make_unique<UnifiedCache>(multi_cache_opts);
  LOG(INFO) << "dram_capacity = " << dram_capacity
            << " pmem_capacity = " << pmem_capacity
            << " ssd_capacity = " << ssd_capacity;
  auto start_res = cache_->Start();
  if (start_res) {
    LOG(INFO) << "MultiTierCache instance starts!";
  }
  // Remove All Data if it has
  cache_->RemoveAll();
}

MultiTierCache::~MultiTierCache() {
  LOG(INFO) << "MultiTierCache instance test completes!";
  PrintMeasurement();
  auto stop_res = cache_->Stop();
  if (stop_res) {
    LOG(INFO) << "MultiTierCache instance stops!";
  }
}

void MultiTierCache::PrintLatency(noodle::SummarySnapshot* snapshot,
                                  std::string comments) {
  if (snapshot) {
    LOG(INFO) << comments << " LatencySummarySnapshot: "
              << "p25: " << snapshot->Get25thPercentile()
              << ", p50: " << snapshot->GetMedian()
              << ", p75: " << snapshot->Get75thPercentile()
              << ", p99: " << snapshot->Get99thPercentile()
              << ", p999: " << snapshot->Get999thPercentile()
              << ", max: " << snapshot->GetMax();
  } else {
    LOG(INFO) << "No " << comments << " LatencySummary!";
  }
}

void MultiTierCache::PrintCacheStats(std::string metrics,
                                     std::string comments) {
  auto res = noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
      noodle::MetricId(metrics));
  if (res) {
    LOG(INFO) << comments << ": " << res->GetValue();
  } else {
    LOG(INFO) << "No " << comments;
  }
}

void MultiTierCache::PrintMeasurement() {
  auto query_snapshot = cache_->GetLookupLatencySummarySnapshot();
  auto dram_lookup_snapshot = cache_->GetInstanceLookupLatencySummarySnapshot(
      UnifiedCache::CacheInstanceType::kDRAM);
  auto pmem_lookup_snapshot = cache_->GetInstanceLookupLatencySummarySnapshot(
      UnifiedCache::CacheInstanceType::kPMEM);
  auto ssd_lookup_snapshot = cache_->GetInstanceLookupLatencySummarySnapshot(
      UnifiedCache::CacheInstanceType::kSSD);
  PrintLatency(query_snapshot.get(), "UnifiedCacheLookupLatency");
  PrintLatency(dram_lookup_snapshot.get(), "DramCacheLookupLatency");
  PrintLatency(pmem_lookup_snapshot.get(), "PmemCacheLookupLatency");
  PrintLatency(ssd_lookup_snapshot.get(), "SsdCacheLookupLatency");
  PrintCacheStats("ti.mtcache.unified.puts", "UnifiedCachePuts");
  PrintCacheStats("ti.mtcache.unified.acquires", "UnifiedCacheAcquires");
  PrintCacheStats("ti.mtcache.unified.deletes", "UnifiedCacheDeletes");
  PrintCacheStats("ti.mtcache.dram.puts", "DramCachePuts");
  PrintCacheStats("ti.mtcache.pmem.puts", "PmemCachePuts");
  PrintCacheStats("ti.mtcache.ssd.puts", "SsdCachePuts");
  PrintCacheStats("ti.mtcache.unified.hits", "UnifiedCacheHits");
  PrintCacheStats("ti.mtcache.unified.misses", "UnifiedCacheMiss");
  PrintCacheStats("ti.mtcache.dram.hits", "DramCacheHits");
  PrintCacheStats("ti.mtcache.dram.misses", "DramCacheMiss");
  PrintCacheStats("ti.mtcache.pmem.hits", "PmemCacheHits");
  PrintCacheStats("ti.mtcache.pmem.misses", "PmemCacheMiss");
  PrintCacheStats("ti.mtcache.ssd.hits", "SsdCacheHits");
  PrintCacheStats("ti.mtcache.ssd.misses", "SsdCacheMiss");
  PrintCacheStats("ti.mtcache.dram.evicts", "DramCacheEvicts");
  PrintCacheStats("ti.mtcache.pmem.evicts", "PmemCacheEvicts");
  PrintCacheStats("ti.mtcache.ssd.evicts", "SsdCacheEvicts");
}

void MultiTierCache::Insert(const std::string& key, std::string value,
                            size_t size) {
  auto val_buf = folly::IOBuf::wrapBufferAsValue(value.data(), value.size());
  cache_->Insert(key, std::move(val_buf), value.size());
}

std::optional<std::string> MultiTierCache::Lookup(const std::string& key) {
  auto handle = cache_->Acquire(key);
  if (handle == nullptr) {
    return std::nullopt;
  }
  std::string res =
      std::string(reinterpret_cast<const char*>(handle->value().data()),
                  handle->value().length());
  cache_->Release(handle);
  return res;
}

void MultiTierCache::Remove(const std::string& key) { cache_->Remove(key); }

void MultiTierCache::RemoveAll() { cache_->RemoveAll(); }

size_t MultiTierCache::Capacity() const { return cache_->Capacity(); }

void MultiTierCache::SetCapacity(size_t capacity) {
  cache_->SetCapacity(capacity);
}

size_t MultiTierCache::Size() const { return cache_->Capacity(); };

}  // namespace mtcache
