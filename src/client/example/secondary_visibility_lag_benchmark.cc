#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "client/client.h"
#include "common/status.h"

namespace {

struct Summary {
    int64_t samples = 0;
    int64_t errors = 0;
    int64_t avg_us = 0;
    int64_t p50_us = 0;
    int64_t p95_us = 0;
    int64_t p99_us = 0;
    int64_t min_us = 0;
    int64_t max_us = 0;
};

uint64_t NowNs() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::steady_clock::now().time_since_epoch())
                                     .count());
}

int64_t Percentile(const std::vector<int64_t>& sorted, double pct) {
    if (sorted.empty()) {
        return 0;
    }
    const double pos = pct * static_cast<double>(sorted.size() - 1);
    return sorted[static_cast<size_t>(pos + 0.5)];
}

Summary Summarize(std::vector<int64_t> samples, int64_t errors) {
    std::sort(samples.begin(), samples.end());
    Summary s;
    s.samples = static_cast<int64_t>(samples.size());
    s.errors = errors;
    if (samples.empty()) {
        return s;
    }
    const int64_t sum = std::accumulate(samples.begin(), samples.end(), int64_t{0});
    s.avg_us = sum / static_cast<int64_t>(samples.size());
    s.p50_us = Percentile(samples, 0.50);
    s.p95_us = Percentile(samples, 0.95);
    s.p99_us = Percentile(samples, 0.99);
    s.min_us = samples.front();
    s.max_us = samples.back();
    return s;
}

void PrintSummary(const std::string& phase, const Summary& s, int threads, int64_t total_ms) {
    std::cout << "phase,threads,samples,errors,total_ms,avg_us,p50_us,p95_us,p99_us,min_us,max_us"
              << std::endl;
    std::cout << phase << "," << threads << "," << s.samples << "," << s.errors << ","
              << total_ms << "," << s.avg_us << "," << s.p50_us << "," << s.p95_us << ","
              << s.p99_us << "," << s.min_us << "," << s.max_us << std::endl;
}

bool OpenTable(const std::string& metaserver, const std::string& idc,
               const std::string& namespace_name, const std::string& table_name,
               bcache2::client::PartitionPickOptions::Policy policy,
               const std::string& affinity_vdc,
               std::unique_ptr<bcache2::client::Client>* client,
               std::unique_ptr<bcache2::client::Table>* table) {
    bcache2::client::ClientOptions client_options;
    client_options.af = bcache2::client::AddressFamily::kIp4;
    client_options.master_addr = metaserver;
    client_options.idc = idc;
    client_options.host = "127.0.0.1";
    client_options.psm = "secondary.visibility.lag.benchmark";
    client_options.log_level = bcache2::client::LogLevel::kWarning;
    client_options.partition_pick_opts.policy = policy;
    client_options.partition_pick_opts.affinity_vdc = affinity_vdc;

    bcache2::client::Client* raw_client = nullptr;
    bcache2::Status status = bcache2::client::Client::Create(client_options, &raw_client);
    if (!status.ok()) {
        std::cerr << "Client::Create failed: " << status.ToString() << std::endl;
        return false;
    }
    client->reset(raw_client);

    bcache2::client::TableOptions table_options;
    table_options.io_timeout_ms = 2000;
    table_options.connect_timeout_ms = 2000;
    bcache2::client::Table* raw_table = nullptr;
    status = (*client)->OpenTable(namespace_name, table_name, table_options, &raw_table);
    if (!status.ok()) {
        std::cerr << "OpenTable failed: " << status.ToString() << std::endl;
        return false;
    }
    table->reset(raw_table);
    return true;
}

std::string MakeValue(int value_bytes, int64_t ordinal) {
    std::string value = "value-" + std::to_string(ordinal) + "-";
    if (value_bytes > static_cast<int>(value.size())) {
        value.append(static_cast<size_t>(value_bytes - value.size()), 'x');
    }
    return value;
}

bool WaitSeedVisibleOnSecondary(const std::string& metaserver, const std::string& idc,
                                const std::string& namespace_name,
                                const std::string& table_name, const std::string& prefix,
                                int value_bytes, int seed_count, int max_wait_ms) {
    std::unique_ptr<bcache2::client::Client> client;
    std::unique_ptr<bcache2::client::Table> table;
    if (!OpenTable(metaserver, idc, namespace_name, table_name,
                   bcache2::client::PartitionPickOptions::Policy::kVdcAffinity,
                   "force-secondary-read", &client, &table)) {
        return false;
    }
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(std::max(0, max_wait_ms));
    do {
        bool all_visible = true;
        for (int i = 0; i < seed_count; ++i) {
            const std::string key = prefix + ":seed:" + std::to_string(i);
            std::string got;
            const bcache2::Status status = table->Get(key, &got);
            if (!status.ok() || got != MakeValue(value_bytes, i)) {
                all_visible = false;
                break;
            }
        }
        if (all_visible) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
}

bool GetExpectedValueWithRetry(bcache2::client::Table* table, const std::string& key,
                               const std::string& expected, int max_wait_ms) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(std::max(0, max_wait_ms));
    do {
        std::string got;
        const bcache2::Status status = table->Get(key, &got);
        if (status.ok() && got == expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
}

struct LoadStats {
    std::atomic<int64_t> writes{0};
    std::atomic<int64_t> reads{0};
    std::atomic<int64_t> write_errors{0};
    std::atomic<int64_t> read_errors{0};
};

void StartBackgroundLoad(const std::string& metaserver, const std::string& idc,
                         const std::string& namespace_name, const std::string& table_name,
                         const std::string& prefix, int value_bytes, int writer_threads,
                         int reader_threads, int reader_max_wait_ms, std::atomic<bool>* stop,
                         LoadStats* stats, std::vector<std::thread>* workers) {
    for (int t = 0; t < writer_threads; ++t) {
        workers->emplace_back([=] {
            std::unique_ptr<bcache2::client::Client> client;
            std::unique_ptr<bcache2::client::Table> table;
            if (!OpenTable(metaserver, idc, namespace_name, table_name,
                           bcache2::client::PartitionPickOptions::Policy::kPrimary, "", &client,
                           &table)) {
                stats->write_errors.fetch_add(1);
                return;
            }
            int64_t i = 0;
            while (!stop->load(std::memory_order_acquire)) {
                const std::string key =
                    prefix + ":load:w:" + std::to_string(t) + ":" + std::to_string(i++);
                const bcache2::Status status = table->Set(key, MakeValue(value_bytes, i));
                if (status.ok()) {
                    stats->writes.fetch_add(1, std::memory_order_relaxed);
                } else {
                    stats->write_errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (int t = 0; t < reader_threads; ++t) {
        workers->emplace_back([=] {
            std::unique_ptr<bcache2::client::Client> client;
            std::unique_ptr<bcache2::client::Table> table;
            if (!OpenTable(metaserver, idc, namespace_name, table_name,
                           bcache2::client::PartitionPickOptions::Policy::kVdcAffinity,
                           "force-secondary-read", &client, &table)) {
                stats->read_errors.fetch_add(1);
                return;
            }
            int64_t i = 0;
            while (!stop->load(std::memory_order_acquire)) {
                const int64_t slot = i++ % 1024;
                const std::string key = prefix + ":seed:" + std::to_string(slot);
                if (GetExpectedValueWithRetry(table.get(), key, MakeValue(value_bytes, slot),
                                              reader_max_wait_ms)) {
                    stats->reads.fetch_add(1, std::memory_order_relaxed);
                } else {
                    stats->read_errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 5 || argc > 11) {
        std::cout << "usage: " << argv[0]
                  << " <metaserver_host:port> <idc> <namespace> <table> [probe_ops=100]"
                     " [probe_threads=1] [value_bytes=128] [max_wait_ms=30000]"
                     " [background_writer_threads=0] [background_reader_threads=0]"
                  << std::endl;
        return 2;
    }

    const std::string metaserver = argv[1];
    const std::string idc = argv[2];
    const std::string namespace_name = argv[3];
    const std::string table_name = argv[4];
    const int probe_ops = argc > 5 ? std::atoi(argv[5]) : 100;
    const int probe_threads = std::max(1, argc > 6 ? std::atoi(argv[6]) : 1);
    const int value_bytes = argc > 7 ? std::atoi(argv[7]) : 128;
    const int max_wait_ms = argc > 8 ? std::atoi(argv[8]) : 30000;
    const int background_writer_threads = argc > 9 ? std::atoi(argv[9]) : 0;
    const int background_reader_threads = argc > 10 ? std::atoi(argv[10]) : 0;
    const std::string prefix = "visibility_lag_" + std::to_string(NowNs());

    std::unique_ptr<bcache2::client::Client> seed_client;
    std::unique_ptr<bcache2::client::Table> seed_table;
    if (!OpenTable(metaserver, idc, namespace_name, table_name,
                   bcache2::client::PartitionPickOptions::Policy::kPrimary, "", &seed_client,
                   &seed_table)) {
        return 1;
    }
    constexpr int kSeedCount = 1024;
    for (int i = 0; i < kSeedCount; ++i) {
        const std::string key = prefix + ":seed:" + std::to_string(i);
        const bcache2::Status status = seed_table->Set(key, MakeValue(value_bytes, i));
        if (!status.ok()) {
            std::cerr << "seed Set failed: " << status.ToString() << std::endl;
            return 1;
        }
    }
    if (background_reader_threads > 0 &&
        !WaitSeedVisibleOnSecondary(metaserver, idc, namespace_name, table_name, prefix,
                                    value_bytes, kSeedCount, max_wait_ms)) {
        std::cerr << "seed keys did not become visible on secondary before background load"
                  << std::endl;
        return 1;
    }

    std::atomic<bool> stop_background{false};
    LoadStats load_stats;
    std::vector<std::thread> background_workers;
    StartBackgroundLoad(metaserver, idc, namespace_name, table_name, prefix, value_bytes,
                        background_writer_threads, background_reader_threads, max_wait_ms,
                        &stop_background, &load_stats, &background_workers);

    std::vector<int64_t> lag_samples(static_cast<size_t>(probe_ops), 0);
    std::vector<int64_t> attempts_samples(static_cast<size_t>(probe_ops), 0);
    std::vector<uint8_t> success(static_cast<size_t>(probe_ops), 0);
    std::atomic<int> next_probe{0};
    std::atomic<int64_t> errors{0};
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(probe_threads));

    for (int t = 0; t < probe_threads; ++t) {
        workers.emplace_back([&, t] {
            std::unique_ptr<bcache2::client::Client> writer_client;
            std::unique_ptr<bcache2::client::Table> writer_table;
            std::unique_ptr<bcache2::client::Client> secondary_client;
            std::unique_ptr<bcache2::client::Table> secondary_table;
            if (!OpenTable(metaserver, idc, namespace_name, table_name,
                           bcache2::client::PartitionPickOptions::Policy::kPrimary, "",
                           &writer_client, &writer_table) ||
                !OpenTable(metaserver, idc, namespace_name, table_name,
                           bcache2::client::PartitionPickOptions::Policy::kVdcAffinity,
                           "force-secondary-read", &secondary_client, &secondary_table)) {
                errors.fetch_add(1);
                ready.fetch_add(1);
                return;
            }
            ready.fetch_add(1);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            for (;;) {
                const int op = next_probe.fetch_add(1);
                if (op >= probe_ops) {
                    break;
                }
                const std::string key =
                    prefix + ":probe:" + std::to_string(t) + ":" + std::to_string(op);
                const std::string value = MakeValue(value_bytes, op);
                const bcache2::Status set_status = writer_table->Set(key, value);
                if (!set_status.ok()) {
                    errors.fetch_add(1);
                    continue;
                }

                const auto begin = std::chrono::steady_clock::now();
                const auto deadline = begin + std::chrono::milliseconds(max_wait_ms);
                int64_t attempts = 0;
                bool visible = false;
                do {
                    ++attempts;
                    std::string got;
                    const bcache2::Status get_status = secondary_table->Get(key, &got);
                    if (get_status.ok() && got == value) {
                        visible = true;
                        break;
                    }
                } while (std::chrono::steady_clock::now() < deadline);

                if (!visible) {
                    errors.fetch_add(1);
                    continue;
                }
                const auto end = std::chrono::steady_clock::now();
                success[static_cast<size_t>(op)] = 1;
                lag_samples[static_cast<size_t>(op)] =
                    std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
                attempts_samples[static_cast<size_t>(op)] = attempts;
            }
        });
    }

    while (ready.load(std::memory_order_acquire) < probe_threads) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const auto begin = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);
    for (auto& worker : workers) {
        worker.join();
    }
    const auto end = std::chrono::steady_clock::now();

    stop_background.store(true, std::memory_order_release);
    for (auto& worker : background_workers) {
        worker.join();
    }

    std::vector<int64_t> successful_lag;
    std::vector<int64_t> successful_attempts;
    successful_lag.reserve(static_cast<size_t>(probe_ops));
    successful_attempts.reserve(static_cast<size_t>(probe_ops));
    for (int i = 0; i < probe_ops; ++i) {
        if (success[static_cast<size_t>(i)] != 0) {
            successful_lag.push_back(lag_samples[static_cast<size_t>(i)]);
            successful_attempts.push_back(attempts_samples[static_cast<size_t>(i)]);
        }
    }

    const int64_t total_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
    std::cout << "config,metaserver,idc,namespace,table,probe_ops,probe_threads,value_bytes,"
                 "max_wait_ms,background_writer_threads,background_reader_threads"
              << std::endl;
    std::cout << "config," << metaserver << "," << idc << "," << namespace_name << ","
              << table_name << "," << probe_ops << "," << probe_threads << "," << value_bytes
              << "," << max_wait_ms << "," << background_writer_threads << ","
              << background_reader_threads << std::endl;
    PrintSummary("secondary_visibility_lag_after_primary_set",
                 Summarize(std::move(successful_lag), errors.load()), probe_threads, total_ms);
    PrintSummary("secondary_visibility_poll_attempts",
                 Summarize(std::move(successful_attempts), errors.load()), probe_threads,
                 total_ms);
    std::cout << "background,writes,reads,write_errors,read_errors" << std::endl;
    std::cout << "background," << load_stats.writes.load() << "," << load_stats.reads.load()
              << "," << load_stats.write_errors.load() << ","
              << load_stats.read_errors.load() << std::endl;

    return errors.load() == 0 ? 0 : 1;
}
