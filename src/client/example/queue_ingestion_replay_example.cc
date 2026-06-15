#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace {

struct Options {
    std::string proxy_endpoint;
    std::string input_path;
    std::string namespace_name = "queue_ns";
    std::string table_name = "queue_table";
    std::string source = "kafka";
    int records = 1000;
    int batch_size = 128;
    int value_size = 128;
    int duplicate_every = 0;
    int dead_letter_every = 0;
    int fail_first_attempt_every = 0;
    int max_retries = 3;
    int retry_backoff_ms = 2;
    bool dry_run = true;
};

struct QueueRecord {
    std::string source;
    int partition = 0;
    int64_t offset = 0;
    std::string namespace_name;
    std::string table_name;
    std::string key;
    std::string value;
};

struct Metrics {
    int64_t input_records = 0;
    int64_t unique_records = 0;
    int64_t duplicate_records = 0;
    int64_t batches = 0;
    int64_t committed = 0;
    int64_t failed = 0;
    int64_t retries = 0;
    int64_t dead_letter_records = 0;
    int64_t checkpointed_partitions = 0;
    int64_t max_checkpoint_offset = -1;
    std::map<int, int64_t> checkpoint_offsets;
};

void Usage(const char* argv0) {
    std::cerr << "usage: " << argv0 << " [--dry_run=0|1] [--proxy=host:port] "
              << "[--input=csv] [--records=N] [--batch_size=N] [--value_size=N] "
              << "[--duplicate_every=N] [--dead_letter_every=N] "
              << "[--fail_first_attempt_every=N] [--namespace=ns] [--table=table] "
              << "[--source=kafka|flink|pubsub] [--max_retries=N] [--retry_backoff_ms=N]"
              << std::endl;
}

bool StartsWith(const std::string& value, const std::string& prefix) {
    return value.compare(0, prefix.size(), prefix) == 0;
}

bool ParseInt(const std::string& value, int* out) {
    char* end = nullptr;
    const long parsed = std::strtol(value.c_str(), &end, 10);
    if (end == value.c_str() || *end != '\0' || parsed < 0) {
        return false;
    }
    *out = static_cast<int>(parsed);
    return true;
}

bool ParseOptions(int argc, char** argv, Options* options) {
    for (int i = 1; i < argc; ++i) {
        const std::string arg(argv[i]);
        const auto eq = arg.find('=');
        if (!StartsWith(arg, "--") || eq == std::string::npos) {
            Usage(argv[0]);
            return false;
        }
        const std::string name = arg.substr(2, eq - 2);
        const std::string value = arg.substr(eq + 1);
        if (name == "proxy") {
            options->proxy_endpoint = value;
        } else if (name == "input") {
            options->input_path = value;
        } else if (name == "namespace") {
            options->namespace_name = value;
        } else if (name == "table") {
            options->table_name = value;
        } else if (name == "source") {
            options->source = value;
        } else if (name == "dry_run") {
            options->dry_run = value != "0";
        } else if (name == "records") {
            if (!ParseInt(value, &options->records)) return false;
        } else if (name == "batch_size") {
            if (!ParseInt(value, &options->batch_size)) return false;
        } else if (name == "value_size") {
            if (!ParseInt(value, &options->value_size)) return false;
        } else if (name == "duplicate_every") {
            if (!ParseInt(value, &options->duplicate_every)) return false;
        } else if (name == "dead_letter_every") {
            if (!ParseInt(value, &options->dead_letter_every)) return false;
        } else if (name == "fail_first_attempt_every") {
            if (!ParseInt(value, &options->fail_first_attempt_every)) return false;
        } else if (name == "max_retries") {
            if (!ParseInt(value, &options->max_retries)) return false;
        } else if (name == "retry_backoff_ms") {
            if (!ParseInt(value, &options->retry_backoff_ms)) return false;
        } else {
            Usage(argv[0]);
            return false;
        }
    }
    if (options->batch_size <= 0 || options->records <= 0 || options->value_size <= 0 ||
        options->max_retries < 0 || options->retry_backoff_ms < 0 ||
        options->duplicate_every < 0 || options->dead_letter_every < 0 ||
        options->fail_first_attempt_every < 0) {
        Usage(argv[0]);
        return false;
    }
    if (options->source != "api" && options->source != "kafka" && options->source != "flink" &&
        options->source != "pubsub" && options->source != "pulsar" &&
        options->source != "kinesis") {
        std::cerr << "unsupported ingestion source: " << options->source << std::endl;
        return false;
    }
    if (!options->dry_run && options->proxy_endpoint.empty()) {
        std::cerr << "--proxy is required when --dry_run=0" << std::endl;
        return false;
    }
    if (!options->dry_run) {
        std::cerr << "live proxy replay is not linked in this local parity tool; "
                  << "use proxy_ingestion_pressure_example for proxy RPC validation" << std::endl;
        return false;
    }
    return true;
}

std::vector<std::string> SplitCsvLine(const std::string& line) {
    std::vector<std::string> fields;
    std::stringstream ss(line);
    std::string field;
    while (std::getline(ss, field, ',')) {
        fields.push_back(field);
    }
    return fields;
}

bool ParseRecordLine(const std::string& line, QueueRecord* record) {
    const auto fields = SplitCsvLine(line);
    if (fields.size() != 7) {
        return false;
    }
    record->source = fields[0];
    record->partition = std::atoi(fields[1].c_str());
    record->offset = std::atoll(fields[2].c_str());
    record->namespace_name = fields[3];
    record->table_name = fields[4];
    record->key = fields[5];
    record->value = fields[6];
    return !record->source.empty() && !record->namespace_name.empty() &&
           !record->table_name.empty();
}

std::vector<QueueRecord> GenerateRecords(const Options& options) {
    std::vector<QueueRecord> records;
    records.reserve(options.records + options.records / std::max(1, options.duplicate_every));
    const std::string value(options.value_size, 'q');
    for (int i = 0; i < options.records; ++i) {
        QueueRecord record;
        record.source = options.source;
        record.partition = i % 8;
        record.offset = i;
        record.namespace_name = options.namespace_name;
        record.table_name = options.table_name;
        record.key = "queue_key_" + std::to_string(i);
        record.value = value;
        if (options.dead_letter_every > 0 && i > 0 && i % options.dead_letter_every == 0) {
            record.key.clear();
        }
        records.push_back(record);
        if (options.duplicate_every > 0 && i > 0 && i % options.duplicate_every == 0) {
            records.push_back(record);
        }
    }
    return records;
}

bool LoadRecords(const Options& options, std::vector<QueueRecord>* records) {
    if (options.input_path.empty()) {
        *records = GenerateRecords(options);
        return true;
    }
    std::ifstream input(options.input_path);
    if (!input) {
        std::cerr << "failed to open input: " << options.input_path << std::endl;
        return false;
    }
    std::string line;
    while (std::getline(input, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }
        QueueRecord record;
        if (!ParseRecordLine(line, &record)) {
            std::cerr << "bad record line: " << line << std::endl;
            return false;
        }
        records->push_back(record);
    }
    return true;
}

std::string DedupeKey(const QueueRecord& record) {
    return record.source + ":" + std::to_string(record.partition) + ":" +
           std::to_string(record.offset);
}

bool IsValidRecord(const QueueRecord& record) {
    return !record.source.empty() && !record.namespace_name.empty() && !record.table_name.empty() &&
           !record.key.empty() && record.offset >= 0 && record.partition >= 0;
}

bool ShouldFailFirstAttempt(const Options& options, const QueueRecord& record) {
    return options.fail_first_attempt_every > 0 && record.offset > 0 &&
           record.offset % options.fail_first_attempt_every == 0;
}

void UpdateCheckpoint(const QueueRecord& record, Metrics* metrics) {
    auto it = metrics->checkpoint_offsets.find(record.partition);
    if (it == metrics->checkpoint_offsets.end() || record.offset > it->second) {
        metrics->checkpoint_offsets[record.partition] = record.offset;
    }
}

bool FlushBatch(const Options& options, const std::vector<QueueRecord>& batch, Metrics* metrics) {
    ++metrics->batches;
    bool ok = true;
    for (const auto& record : batch) {
        if (!IsValidRecord(record)) {
            ++metrics->dead_letter_records;
            continue;
        }
        bool committed = false;
        for (int attempt = 0; attempt <= options.max_retries; ++attempt) {
            if (attempt == 0 && ShouldFailFirstAttempt(options, record)) {
                ++metrics->retries;
                if (options.retry_backoff_ms > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(options.retry_backoff_ms));
                }
                continue;
            }
            committed = options.dry_run;
            break;
        }
        if (committed) {
            ++metrics->committed;
            UpdateCheckpoint(record, metrics);
        } else {
            ++metrics->failed;
            ok = false;
        }
    }
    return ok;
}

bool Replay(const Options& options, const std::vector<QueueRecord>& input, Metrics* metrics) {
    std::unordered_set<std::string> seen_offsets;
    std::vector<QueueRecord> batch;
    batch.reserve(options.batch_size);
    bool ok = true;
    for (const auto& record : input) {
        ++metrics->input_records;
        if (!IsValidRecord(record)) {
            ++metrics->dead_letter_records;
            continue;
        }
        const auto dedupe_key = DedupeKey(record);
        if (!seen_offsets.insert(dedupe_key).second) {
            ++metrics->duplicate_records;
            continue;
        }
        ++metrics->unique_records;
        batch.push_back(record);
        if (static_cast<int>(batch.size()) >= options.batch_size) {
            ok = FlushBatch(options, batch, metrics) && ok;
            batch.clear();
        }
    }
    if (!batch.empty()) {
        ok = FlushBatch(options, batch, metrics) && ok;
    }
    metrics->checkpointed_partitions =
            static_cast<int64_t>(metrics->checkpoint_offsets.size());
    for (const auto& iter : metrics->checkpoint_offsets) {
        metrics->max_checkpoint_offset = std::max(metrics->max_checkpoint_offset, iter.second);
    }
    return ok;
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    if (!ParseOptions(argc, argv, &options)) {
        return 2;
    }
    std::vector<QueueRecord> records;
    if (!LoadRecords(options, &records)) {
        return 2;
    }
    Metrics metrics;
    const auto begin = std::chrono::steady_clock::now();
    const bool ok = Replay(options, records, &metrics);
    const auto end = std::chrono::steady_clock::now();
    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
    const double qps = elapsed_ms == 0 ? 0.0
                                      : static_cast<double>(metrics.committed) * 1000.0 / elapsed_ms;

    std::cout << "queue_ingestion_replay" << std::endl;
    std::cout << "source=" << options.source << std::endl;
    std::cout << "dry_run=" << (options.dry_run ? 1 : 0) << std::endl;
    std::cout << "input_records=" << metrics.input_records << std::endl;
    std::cout << "unique_records=" << metrics.unique_records << std::endl;
    std::cout << "duplicate_records=" << metrics.duplicate_records << std::endl;
    std::cout << "batches=" << metrics.batches << std::endl;
    std::cout << "committed=" << metrics.committed << std::endl;
    std::cout << "failed=" << metrics.failed << std::endl;
    std::cout << "retries=" << metrics.retries << std::endl;
    std::cout << "dead_letter_records=" << metrics.dead_letter_records << std::endl;
    std::cout << "checkpointed_partitions=" << metrics.checkpointed_partitions << std::endl;
    std::cout << "max_checkpoint_offset=" << metrics.max_checkpoint_offset << std::endl;
    std::cout << "elapsed_ms=" << elapsed_ms << std::endl;
    std::cout << "committed_qps=" << qps << std::endl;

    return ok && metrics.failed == 0 && metrics.committed == metrics.unique_records ? 0 : 1;
}
