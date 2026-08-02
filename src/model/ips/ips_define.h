// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once
#include <assert.h>
#include <byte/include/macros.h>
#include <spdlog/fmt/fmt.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <ctime>
#include <list>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace bcache2 {
namespace ips {

inline void PutIPSKeyToTreeKeyImpl(int64_t uid, int16_t slot, int16_t table, int16_t action_type,
                                   std::string* key_ptr) {
    fmt::format_int a(uid);
    fmt::format_int b(slot);
    fmt::format_int c(table);
    fmt::format_int d(action_type);
    std::string& key = *key_ptr;
    key.reserve(3 + a.size() + b.size() + c.size() + d.size());
    key.append(a.data(), a.size());
    key.push_back('_');
    key.append(b.data(), b.size());
    key.push_back('_');
    key.append(c.data(), c.size());
    key.push_back('_');
    key.append(d.data(), d.size());
}

inline void PutIPSKeyToTreeKey(int64_t uid, int16_t slot, int16_t table, int16_t action_type,
                               std::string* key_ptr) {
    key_ptr->clear();
    PutIPSKeyToTreeKeyImpl(uid, slot, table, action_type, key_ptr);
}

inline std::string IPSKeyToTreeKey(int64_t uid, int16_t slot, int16_t table, int16_t action_type) {
    std::string key;
    PutIPSKeyToTreeKeyImpl(uid, slot, table, action_type, &key);
    return key;
}

typedef int64_t UID;
typedef uint16_t SlotID;
typedef int16_t ActionType;
typedef int8_t FeatureStatType;
typedef int8_t DecayOperatorType;

struct IPSHeader {
    uint8_t version = 0;              // 1 byte
    int64_t last_compact_ts = 0;      // 8 byte
    int64_t compact_start_ts = 0;     // 8 byte
    int32_t compact_range_index = 0;  // 4 byte, since version 0
    int64_t base_ts = 0;              // us, 8 byte, since tree_version 1
} __attribute__((packed));

static const uint8_t kTreeMetaVersionOffset = 0;
static const uint8_t kTreeMetaLastCompactTsOffset = sizeof(uint8_t);
static const uint8_t kTreeMetaCompactStartTsOffset = sizeof(uint8_t) + sizeof(int64_t);
static const uint8_t kTreeMetaCompactRangeIndexOffset =
    sizeof(uint8_t) + sizeof(int64_t) + sizeof(int64_t);
static const uint8_t kTreeMetaBaseTsUsOffset =
    sizeof(uint8_t) + sizeof(int64_t) + sizeof(int64_t) + sizeof(int32_t);

static const uint8_t kVersionZeroMetaSize =
    sizeof(uint8_t) + sizeof(int64_t) + sizeof(int64_t) + sizeof(int32_t);
static const uint8_t kVersionOneMetaSize =
    sizeof(uint8_t) + sizeof(int64_t) + sizeof(int64_t) + sizeof(int32_t) + sizeof(int64_t);

static const uint8_t kCurExpectedTreeVersion = 0;

inline bool IsCompressCompactType(const std::string& compact_type) {
    return compact_type == "compress";
}

inline bool IsTruncateCompactType(const std::string& compact_type) {
    return compact_type == "truncate";
}

inline bool IsAbsoluteTimeTruncateType(const std::string& truncate_type) {
    return truncate_type == "absolute";
}

inline bool IsCountTruncateType(const std::string& truncate_type) {
    return truncate_type == "count";
}

inline bool IsRelativeTimeTruncateType(const std::string& truncate_type) {
    return truncate_type == "relative";
}

inline bool IsPairTable(const std::string& table_type) { return table_type == "pair"; }

inline bool IsListTable(const std::string& table_type) { return table_type == "list"; }

inline bool CheckSortOptor(const std::string& str) {
    if (str == "id" || str == "v1" || str == "v2" || str == "ratio" || str == "vx" ||
        str == "customize" || str == "wilson") {
        return true;
    }
    return false;
}

static std::string ConvertTimestampToReadableFormat(int64_t ts) {
    int64_t ts_sec = ts / (1000 * 1000);  // to seconds
    tm* ltm = std::localtime(&ts_sec);
    std::string year = std::to_string(1900 + ltm->tm_year);
    std::string month = std::to_string(1 + ltm->tm_mon);
    std::string day = std::to_string(ltm->tm_mday);
    std::string hour = std::to_string(ltm->tm_hour);
    std::string min = std::to_string(ltm->tm_min);
    std::string sec = std::to_string(ltm->tm_sec);
    return year + "-" + month + "-" + day + " " + hour + ":" + min + ":" + sec;
}

enum UpsreamType {
    READ = 0,
    WRITE = 1,
};

enum TopKType {
    TS = 0,
    FID = 1,
};

enum ReduceType {
    IP_REDUCE_SUM = 0,
    IP_REDUCE_MAX = 1,
    IP_REDUCE_NONE = 2,
    IP_REDUCE_SUM_MAX = 3,
};

// this table type corresponds to FeatureType, and must be consistent
enum TableType {
    PAIR = 0,
    LIST = 1,
};

enum FeatureType {
    IP_INT_PAIR = 0,
    IP_INT_LIST = 1,
};

enum TruncateType {
    IP_COUNT_TRUNCATE = 0,
    IP_ABSOLUTE_TRUNCATE = 1,
    IP_RELATIVE_TRUNCATE = 2,
};

enum DateType {
    IP_ABSOLUTE_TIME = 0,
    IP_RELATIVE_TIME = 1,
};

#define IPS_DATA_TYPE_CHECK(data_type, table_type, ret) \
    do {                                                \
        if (data_type != table_type) return ret;        \
    } while (0)

#define IP_TABLE_GET_CONFIG(document, name, type, default_value)                \
    do {                                                                        \
        rapidjson::Value::ConstMemberIterator itr = document.FindMember(#name); \
        if (itr == document.MemberEnd()) {                                      \
            name##_ = default_value;                                            \
        } else {                                                                \
            name##_ = itr->value.Get##type();                                   \
        }                                                                       \
    } while (0)

#define IP_TABLE_GET_ATOMIC_CONFIG(document, name, type, default_value)         \
    do {                                                                        \
        rapidjson::Value::ConstMemberIterator itr = document.FindMember(#name); \
        if (itr == document.MemberEnd()) {                                      \
            name##_.store(default_value);                                       \
        } else {                                                                \
            name##_.store(itr->value.Get##type());                              \
        }                                                                       \
    } while (0)

struct SortContext {
    int64_t sort_by_vx;

    const std::vector<int32_t>* feature_index = nullptr;
    const std::vector<int32_t>* feature_weight = nullptr;
    double wilson_z = 1.96;
};

class IPSFeatureData {
 public:
    IPSFeatureData() = default;

    IPSFeatureData(int64_t fid, std::vector<int64_t>&& vec, int64_t min_ts, int64_t max_ts)
        : fid_(fid), feature_data_vec_(std::move(vec)), min_ts_(min_ts), max_ts_(max_ts) {}

    IPSFeatureData(IPSFeatureData&& other) noexcept
        : fid_(other.fid_),
          feature_data_vec_(std::move(other.feature_data_vec_)),
          min_ts_(other.min_ts_),
          max_ts_(other.max_ts_) {}

    IPSFeatureData(const IPSFeatureData& other) = delete;
    IPSFeatureData& operator=(const IPSFeatureData& other) = delete;

    IPSFeatureData& operator=(IPSFeatureData&& other) noexcept {
        fid_ = other.fid_;
        feature_data_vec_ = std::move(other.feature_data_vec_);
        min_ts_ = other.min_ts_;
        max_ts_ = other.max_ts_;
        return *this;
    }

    const std::vector<int64_t>& GetFeatureDataVec() const { return feature_data_vec_; }

    size_t GetFeatureDataSize() const { return feature_data_vec_.size(); }

    std::vector<int64_t>* GetMutableFeatureDataVec() { return &feature_data_vec_; }

    int64_t GetDataAtIndex(size_t index) const {
        if (index >= feature_data_vec_.size()) {
            return 0;
        } else {
            return feature_data_vec_[index];
        }
    }

    int64_t GetMaxTs() const { return max_ts_; }

    int64_t GetMinTs() const { return min_ts_; }

    int64_t GetFid() const { return fid_; }

 private:
    int64_t fid_;
    std::vector<int64_t> feature_data_vec_;
    int64_t min_ts_;
    int64_t max_ts_;
};

static inline int64_t GetWilsonScore(int64_t v1, int64_t v2, double wilson_z = 1.96) {
    int64_t min_val = std::min(v1, v2);
    int64_t max_val = std::max(v1, v2);
    if (max_val == 0) {
        return 0;
    }

    double square_wilson_z = wilson_z * wilson_z;
    double cur_ratio = min_val * 1.0 / max_val;
    double score = (cur_ratio + (square_wilson_z / (2.0 * max_val)) -
                    ((wilson_z / (2.0 * max_val)) *
                     sqrt(4.0 * max_val * (1.0 - cur_ratio) * cur_ratio + square_wilson_z))) /
                   (1.0 + square_wilson_z / max_val);
    score *= 10000;
    return score > INT64_MAX ? INT64_MAX : static_cast<int64_t>(score);
}

// In the following GetSortWeight and its inheritance implementation
// should check internal data type consistency, as level is FATAL
struct GetSortWeight {
    virtual ~GetSortWeight() {}
    virtual int64_t GetCurrentFeatureSortWeight(const IPSFeatureData& feature) const = 0;
    virtual void SetSortContext(const SortContext& context) {}
};

struct GetSortWeightV1 : public GetSortWeight {
    int64_t GetCurrentFeatureSortWeight(const IPSFeatureData& feature) const {
        return feature.GetDataAtIndex(0);
    }
};

struct GetSortWeightV2 : public GetSortWeight {
    int64_t GetCurrentFeatureSortWeight(const IPSFeatureData& feature) const {
        return feature.GetDataAtIndex(1);
    }
};

struct GetSortWeightVX : public GetSortWeight {
    int64_t sort_by_vx;
    int64_t GetCurrentFeatureSortWeight(const IPSFeatureData& feature) const {
        return feature.GetDataAtIndex(sort_by_vx);
    }

    void SetSortContext(const SortContext& context) { sort_by_vx = context.sort_by_vx; }
};

struct GetSortWeightRatio : public GetSortWeight {
    int64_t GetCurrentFeatureSortWeight(const IPSFeatureData& feature) const {
        const std::vector<int64_t>& feature_data = feature.GetFeatureDataVec();
        if (feature_data.size() <= 1 || feature_data[1] == 0) {
            return 0;
        } else {
            return feature_data[0] / feature_data[1];
        }
    }
};

struct GetSortWeightWilson : public GetSortWeight {
    int64_t GetCurrentFeatureSortWeight(const IPSFeatureData& feature) const {
        const std::vector<int64_t>& feature_data = feature.GetFeatureDataVec();
        if (feature_data.empty()) {
            return 0;
        }
        int64_t v1 = feature_data[0];
        int64_t v2 = feature_data.size() < 2 ? 0 : feature_data[1];
        return GetWilsonScore(v1, v2, wilson_z_);
    }

    void SetSortContext(const SortContext& context) { wilson_z_ = context.wilson_z; }

 private:
    double wilson_z_;
};

struct GetSortWeightID : public GetSortWeight {
    int64_t GetCurrentFeatureSortWeight(const IPSFeatureData& feature) const override {
        return feature.GetFid();
    }
};

struct GetSortWeightCustomizeWeight : public GetSortWeight {
    int64_t GetCurrentFeatureSortWeight(const IPSFeatureData& feature) const {
        int64_t sort_weight = 0;
        //  const std::vector<int64_t>& feature_data = feature.GetFeatureDataVec();
        for (size_t i = 0; i < feature_index_->size(); ++i) {
            int64_t tmp = sort_weight;
            sort_weight += feature.GetDataAtIndex(i) * feature_weight_->at(i);
            if (UNLIKELY(sort_weight < tmp)) {
                return INT64_MAX;
            }
        }
        return sort_weight;
    }

    void SetSortContext(const SortContext& context) {
        feature_index_ = context.feature_index;
        feature_weight_ = context.feature_weight;
    }

 private:
    const std::vector<int32_t>* feature_index_ = nullptr;
    const std::vector<int32_t>* feature_weight_ = nullptr;
};

// T需要是std::pair类型, pair.second是排序权重，pair.first没有要求
template <typename T>
struct CustomsizeComparator {
    inline bool operator()(const T& a, const T& b) const { return a.second > b.second; }
};

// require: dst point to the start address that will store encode res
// return: the first idle address after store the encode value
static inline void EncodeInt32ToBigEndian(int32_t value, char* dst) {
    uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);

    buffer[0] = static_cast<uint8_t>(value >> 24);
    buffer[1] = static_cast<uint8_t>(value >> 16);
    buffer[2] = static_cast<uint8_t>(value >> 8);
    buffer[3] = static_cast<uint8_t>(value);
}

static inline int32_t DecodeInt32FromBigEndian(const char* ptr) {
    const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);

    return (static_cast<int32_t>(buffer[3])) | (static_cast<int32_t>(buffer[2]) << 8) |
           (static_cast<int32_t>(buffer[1]) << 16) | (static_cast<int32_t>(buffer[0]) << 24);
}

// require: dst point to the start address that will store encode res
// return: the first idle address after store the encode value
static inline void EncodeInt64ToBigEndian(int64_t value, char* dst) {
    uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);

    // Recent clang and gcc optimize this to a single mov / str instruction.
    buffer[0] = static_cast<uint8_t>(value >> 56);
    buffer[1] = static_cast<uint8_t>(value >> 48);
    buffer[2] = static_cast<uint8_t>(value >> 40);
    buffer[3] = static_cast<uint8_t>(value >> 32);
    buffer[4] = static_cast<uint8_t>(value >> 24);
    buffer[5] = static_cast<uint8_t>(value >> 16);
    buffer[6] = static_cast<uint8_t>(value >> 8);
    buffer[7] = static_cast<uint8_t>(value);
}

static inline int64_t DecodeInt64FromBigEndian(const char* ptr) {
    const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);

    // Recent clang and gcc optimize this to a single mov / ldr instruction.
    return (static_cast<int64_t>(buffer[7])) | (static_cast<int64_t>(buffer[6]) << 8) |
           (static_cast<int64_t>(buffer[5]) << 16) | (static_cast<int64_t>(buffer[4]) << 24) |
           (static_cast<int64_t>(buffer[3]) << 32) | (static_cast<int64_t>(buffer[2]) << 40) |
           (static_cast<int64_t>(buffer[1]) << 48) | (static_cast<int64_t>(buffer[0]) << 56);
}

static inline int64_t DecodeInt56FromBigEndian(const char* ptr) {
    const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);

    // Recent clang and gcc optimize this to a single mov / ldr instruction.
    return (static_cast<int64_t>(buffer[6])) | (static_cast<int64_t>(buffer[5]) << 8) |
           (static_cast<int64_t>(buffer[4]) << 16) | (static_cast<int64_t>(buffer[3]) << 24) |
           (static_cast<int64_t>(buffer[2]) << 32) | (static_cast<int64_t>(buffer[1]) << 40) |
           (static_cast<int64_t>(buffer[0]) << 48);
}

static inline void EncodeInt56ToBigEndian(int64_t value, char* dst) {
    uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);

    // Recent clang and gcc optimize this to a single mov / str instruction.
    buffer[0] = static_cast<uint8_t>(value >> 48);
    buffer[1] = static_cast<uint8_t>(value >> 40);
    buffer[2] = static_cast<uint8_t>(value >> 32);
    buffer[3] = static_cast<uint8_t>(value >> 24);
    buffer[4] = static_cast<uint8_t>(value >> 16);
    buffer[5] = static_cast<uint8_t>(value >> 8);
    buffer[6] = static_cast<uint8_t>(value);
}

// for debug
static inline std::string IPSValToString(const std::string& encode_val) {
    const char* data = encode_val.data();
    std::string res = "";
    int index = 0;
    res += ("head version: " + std::to_string(data[index++]) + "\n");
    res += ("ts: " + std::to_string(DecodeInt64FromBigEndian(data + index)) + "\n");
    res += "feature: \n";
    index += 8;
    int64_t feature_size = DecodeInt32FromBigEndian(data + index);
    index += 4;
    for (int i = 0; i < feature_size; ++i) {
        res += (std::to_string(DecodeInt64FromBigEndian(data + index)) + ", ");
        index += 8;
    }
    return res;
}
}  // namespace ips
}  // namespace bcache2
