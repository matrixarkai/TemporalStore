#include "blockcache/blockcache.h"
#include "blockcache/flags.h"

#include <gflags/gflags.h>

#include <iostream>
#include <string>

DEFINE_bool(start_stop_only, false, "Only start and stop the MtCache-backed blockcache");
DEFINE_bool(check_miss, false, "Also check a cache miss status");
DEFINE_bool(use_ssd, false, "Enable SSD cache for this smoke test");
DEFINE_string(smoke_ssd_path, "/tmp/temporalstore-blockcache-smoke-ssd",
              "SSD cache path for this smoke test");
DEFINE_uint64(smoke_ssd_capacity, 64 * 1024 * 1024, "SSD cache capacity for this smoke test");
DEFINE_bool(smoke_clear_ssd_folder, false,
            "Clear the SSD cache folder when the smoke test starts and stops");

namespace {

int CheckStatus(const char* op, const bcache2::Status& status) {
    if (!status.ok()) {
        std::cerr << op << " failed: " << status.ToString() << std::endl;
        return 1;
    }
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    FLAGS_blockcache_dram_capacity = 8 * 1024 * 1024;
    FLAGS_blockcache_pmem_capacity = 0;
    FLAGS_blockcache_ssd_capacity = FLAGS_use_ssd ? FLAGS_smoke_ssd_capacity : 0;
    FLAGS_blockcache_ssd_path = FLAGS_smoke_ssd_path;
    FLAGS_blockcache_ssd_instance_only = FLAGS_use_ssd;
    FLAGS_blockcache_dram_replacement_policy = "SLRU";
    FLAGS_blockcache_ssd_replacement_policy = "SLRU";
    FLAGS_blockcache_enable_metrics = false;
    FLAGS_blockcache_clear_ssd_folder = FLAGS_smoke_clear_ssd_folder;

    bcache2::blockcache::BlockCache cache;
    if (int rc = CheckStatus("Start", cache.Start()); rc != 0) {
        return rc;
    }

    if (!FLAGS_start_stop_only) {
        const std::string key = "mtcache-smoke-key";
        const std::string value = "mtcache-smoke-value";
        if (int rc = CheckStatus("Put", cache.Put(key, value)); rc != 0) {
            cache.Stop();
            return rc;
        }

        std::string loaded;
        if (int rc = CheckStatus("Get", cache.Get(key, &loaded)); rc != 0) {
            cache.Stop();
            return rc;
        }
        if (loaded != value) {
            std::cerr << "Get returned wrong value: " << loaded << std::endl;
            cache.Stop();
            return 1;
        }

        if (FLAGS_check_miss) {
            std::string miss;
            auto miss_status = cache.Get("mtcache-smoke-missing-key", &miss);
            if (miss_status.ok()) {
                std::cerr << "missing key unexpectedly hit" << std::endl;
                cache.Stop();
                return 1;
            }
        }
    }

    if (int rc = CheckStatus("Stop", cache.Stop()); rc != 0) {
        return rc;
    }

    std::cout << "blockcache MtCache smoke passed" << std::endl;
    return 0;
}
