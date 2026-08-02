// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "model/ips/ips_operator.h"

#include <absl/memory/memory.h>
#include <brpc/traceprintf.h>
#include <byte/include/macros.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <tuple>

#include "common/slice.h"
#include "common/status.h"
#include "model/ips/ips_define.h"
#include "model/ips/profile_find_top_k.h"
#include "model/ips/profile_time_dimension.h"
#include "model/ips/time_cost.h"
#include "model/ips/var_encode.h"
#include "extension/ips/interface.pb.h"

namespace bcache2 {
namespace ips {
static const char kEmptyString[] = "";
static const Slice kEmptySlice = Slice();
static const int64_t kCompactMaxRange = 5L * 365 * 24 * 60 * 60 * 1000 * 1000;

std::string IPSOperator::EncodeIpsDataVerisonOne(ReduceType reduce_type, int64_t insert_ts,
                                                 int64_t base_ts,
                                                 const std::vector<int64_t>& feature_vec) {
    size_t feature_size = feature_vec.size();
    // Metrics::GetInstance()->Emit<kMetricStore>("ips.feature.size", feature_size,
    // FLAGS_metrics_sample); var encode: 4 byte min_ts_sec  + feature_size * 8 byte;
    size_t encode_value_size = kMaxVarint32Length + feature_size * kMaxVarint64Length;
    std::string encode_data_buf;
    encode_data_buf.resize(encode_value_size);
    char* encode_data = &encode_data_buf.front();
    size_t encode_index = 0;
    if (reduce_type != ReduceType::IP_REDUCE_NONE) {
        // 4 byte min_ts endcode, sequence model don't need encode min_ts
        int32_t delta_ts = (insert_ts - base_ts) / 1000 / 1000; /* micro seconds -> seconds */
        size_t encode_size = EncodeVarsignedint32(encode_data, delta_ts);
        assert(encode_size > 0);
        encode_index += encode_size;
    }

    // feature data encode
    for (size_t i = 0; i < feature_size; ++i) {
        size_t cur_encode_size = EncodeVarsignedint64(encode_data + encode_index, feature_vec[i]);
        assert(cur_encode_size > 0);
        encode_index += cur_encode_size;
    }

    encode_data_buf.resize(encode_index);
    return encode_data_buf;
}

std::string IPSOperator::EncodeIpsDataVerisonZero(int64_t ts,
                                                  const std::vector<int64_t>& feature_vec) {
    if (UNLIKELY(kCurExpectedTreeVersion != 0)) {
        // BC_FATAL("invalid call func, kCurExpectedTreeVersion: {}", kCurExpectedTreeVersion);
    }
    uint32_t feature_size = feature_vec.size();
    // Metrics::GetInstance()->Emit<kMetricStore>("ips.feature.size", feature_size,
    // FLAGS_metrics_sample); version(1 byte) + min_ts(8 byte) + vec_size(4 byte) + feature_size *
    // 8;
    size_t encode_value_size = 13 + feature_size * 8;
    std::string encode_data_buf;
    encode_data_buf.resize(encode_value_size);
    char* encode_data = &encode_data_buf.front();
    uint64_t index = 0;

    // head version encode
    encode_data[index] = 0;  // default head version
    index += 1;
    // min_ts endcode
    EncodeInt64ToBigEndian(ts, encode_data + index);
    index += 8;
    // feature_size encode
    EncodeInt32ToBigEndian(feature_size, encode_data + index);
    index += 4;

    for (uint32_t i = 0; i < feature_size; ++i) {
        EncodeInt64ToBigEndian(feature_vec[i], encode_data + index);
        index += 8;
    }
    return encode_data_buf;
}

void IPSOperator::GetTreeKvMinAndMaxTs(ReduceType reduce_type, int64_t base_ts,
                                       uint8_t tree_version, const Slice& tree_k,
                                       const Slice& tree_val, int64_t* min_ts, int64_t* max_ts) {
    assert(tree_version == 0 || tree_version == 1);

    *max_ts = DecodeInt56FromBigEndian(tree_k.data());
    if (tree_version == 0) {
        *min_ts = DecodeInt64FromBigEndian(tree_val.data() + 1);
    } else if (tree_version == 1) {
        GetVersionOneTreeValTs(reduce_type, tree_version, base_ts, tree_k.data(), tree_val.data(),
                               min_ts);
    } else {
        // BC_FATAL("invalid tree_version: {}", tree_version);
    }
}

// encode ips feature data to kCurExpectedTreeVersion format
std::string IPSOperator::EncodeIpsData(ReduceType reduce_type, int64_t insert_ts, int64_t base_ts,
                                       const std::vector<int64_t>& feature_vec) {
    if (kCurExpectedTreeVersion == 0) {
        return EncodeIpsDataVerisonZero(insert_ts, feature_vec);
    } else if (kCurExpectedTreeVersion == 1) {
        return EncodeIpsDataVerisonOne(reduce_type, insert_ts, base_ts, feature_vec);
    } else {
        // BC_FATAL("invalid expected tree version: {}", kCurExpectedTreeVersion);
    }
}

// decode all version's ips data
bool IPSOperator::DecodeIpsData(const char* encode_key, const char* encode_val, size_t val_size,
                                uint8_t tree_version, int64_t base_ts, ReduceType reduce_type,
                                int64_t* min_ts, std::vector<int64_t>* feature_vec) {
    if (tree_version == 0) {
        return DecodeIpsVersionZeroData(encode_val, tree_version, min_ts, feature_vec);
    } else if (tree_version == 1) {
        return DecodeIpsVersionOneData(encode_key, encode_val, val_size, tree_version, base_ts,
                                       reduce_type, min_ts, feature_vec);
    } else {
        // BC_FATAL("invalid tree version: {}", tree_version);
    }
    return false;
}
// decode version 1 ips data
bool IPSOperator::DecodeIpsVersionOneData(const char* encode_key, const char* encode_val,
                                          size_t val_size, uint8_t tree_version, int64_t base_ts,
                                          ReduceType reduce_type, int64_t* min_ts,
                                          std::vector<int64_t>* feature_vec) {
    if (UNLIKELY(tree_version != 1)) {
        return false;
    }
    feature_vec->clear();

    const char* end_ptr = encode_val + val_size;
    size_t encode_size =
        GetVersionOneTreeValTs(reduce_type, tree_version, base_ts, encode_key, encode_val, min_ts);

    encode_val += encode_size;
    while (encode_val < end_ptr) {
        int64_t cur_feature_val;
        encode_size = DecodeVarsignedint64(encode_val, kMaxVarint64Length, &cur_feature_val);
        if (UNLIKELY(encode_size == 0)) {
            // BC_ERROR_DEFAULT_RATE_LIMIT("invalid version 1 tree data");
            return false;
        }
        feature_vec->push_back(cur_feature_val);
        encode_val += encode_size;
    }
    return true;
}

bool IPSOperator::DecodeIpsVersionZeroData(const char* encode_val, uint8_t tree_version,
                                           int64_t* min_ts, std::vector<int64_t>* feature_vec) {
    if (UNLIKELY(tree_version != 0)) {
        return false;
    }
    feature_vec->clear();

    size_t decode_index = 0;
    // decode head version
    uint8_t head_version = encode_val[decode_index];
    decode_index += 1;
    if (UNLIKELY(head_version != 0)) {
        std::string msg = fmt::format("IPS decode head version is not 0: {}", head_version);
        // BC_ERROR_DEFAULT_RATE_LIMIT(msg);
        return false;
    }
    // decode min_ts
    *min_ts = DecodeInt64FromBigEndian(encode_val + decode_index);
    decode_index += 8;
    // decode feature size
    uint32_t vec_size = DecodeInt32FromBigEndian(encode_val + decode_index);
    decode_index += 4;
    feature_vec->reserve(vec_size);
    for (size_t index = 0; index < vec_size; ++index) {
        int64_t cur_feature = DecodeInt64FromBigEndian(encode_val + decode_index);
        feature_vec->emplace_back(cur_feature);

        decode_index += 8;
    }
    return true;
}

Status IPSOperator::RangeGetLastCntEle(ReduceType reduce_type, model::IpsModel* IpsModel,
                                       int64_t base_ts, uint8_t tree_version,
                                       std::vector<IPSFeatureData>* res, int64_t cnt,
                                       int64_t* range_min_ts, int64_t* range_max_ts) {
    res->clear();
    Status iter_ret;
    *range_max_ts = INT64_MIN;
    *range_min_ts = INT64_MAX;
    PersistentMap<std::string, std::string>::IterateFunc iter_func =
        [range_min_ts, range_max_ts, reduce_type, &res, &iter_ret, &cnt, tree_version, base_ts](
            const std::string& key_str, const std::string& v_str) -> bool {
        const Slice key(key_str), v(v_str);
        if (UNLIKELY(IsInvalidTreeKV(key, v, tree_version))) {
            return true;
        }
        if (cnt <= 0) {
            return false;
        }
        // key decode: 7byte max_ts + 8 byte fid
        int64_t data_max_ts = DecodeInt56FromBigEndian(key.data());
        int64_t fid = DecodeInt64FromBigEndian(key.data() + 7);

        int64_t data_min_ts = 0;
        std::vector<int64_t> feature_vec;
        bool decode_ips_data_res = DecodeIpsData(key.data(), v.data(), v.size(), tree_version,
                                                 base_ts, reduce_type, &data_min_ts, &feature_vec);
        if (LIKELY(decode_ips_data_res)) {
            res->emplace_back(fid, std::move(feature_vec), data_min_ts, data_max_ts);
            --cnt;

            if (*range_max_ts < data_max_ts) {
                *range_max_ts = data_max_ts;
            }
            if (*range_min_ts > data_min_ts) {
                *range_min_ts = data_min_ts;
            }
        } else {
            iter_ret = Status::Internal("serialize error");
            // BC_ERROR_DEFAULT_RATE_LIMIT("decode_ips_data_from_big_endian failed");
            return false;
        }
        return true;
    };
    Status ret = IpsModel->OrSet().ScanBackward("", iter_func);
    if (UNLIKELY(!ret.ok() || !iter_ret.ok())) {
        return ret.ok() ? iter_ret : ret;
    }
    return Status::OK();
}

Status IPSOperator::RangeGet(model::IpsModel* ordered_tree, int64_t base_ts, uint8_t tree_version,
                             int64_t min_data_ts_micros, int64_t max_data_ts_micros,
                             std::vector<IPSFeatureData>* res, bool is_sort_by_ts,
                             int64_t* range_min_ts, int64_t* range_max_ts, ReduceType reduce_type) {
    res->clear();

    std::unique_ptr<std::unordered_map<int64_t, IPSFeatureData>> range_res = nullptr;
    if (!is_sort_by_ts) {
        range_res = absl::make_unique<std::unordered_map<int64_t, IPSFeatureData>>();
    }
    int64_t min_ts = INT64_MAX;
    int64_t max_ts = INT64_MIN;
    Status iter_ret;
    std::vector<int64_t> feature_vec;

    PersistentMap<std::string, std::string>::IterateFunc iter =
        [res, &min_ts, &max_ts, &range_res, is_sort_by_ts, &iter_ret, &feature_vec, reduce_type,
         max_data_ts_micros, min_data_ts_micros, tree_version,
         base_ts](const std::string& key_str, const std::string& v_str) -> bool {
        const Slice key(key_str), v(v_str);
        if (UNLIKELY(IsInvalidTreeKV(key, v, tree_version))) {
            return true;
        }

        // key decode: 7byte max_ts + 8 byte fid
        int64_t data_max_ts = DecodeInt56FromBigEndian(key.data());
        int64_t fid = DecodeInt64FromBigEndian(key.data() + 7);

        int64_t data_min_ts = 0;
        bool decode_ips_data_res = DecodeIpsData(key.data(), v.data(), v.size(), tree_version,
                                                 base_ts, reduce_type, &data_min_ts, &feature_vec);
        if (LIKELY(decode_ips_data_res)) {
            if (data_min_ts >= max_data_ts_micros) {
                return false;
            }
            if (data_max_ts < min_data_ts_micros) {
                return true;
            }

            if (max_ts < data_max_ts) {
                max_ts = data_max_ts;
            }
            if (min_ts > data_min_ts) {
                min_ts = data_min_ts;
            }

            if (is_sort_by_ts) {
                res->emplace_back(fid, std::move(feature_vec), data_min_ts, data_max_ts);
            } else {
                auto it = range_res->find(fid);
                if (it == range_res->end()) {
                    range_res->emplace(std::piecewise_construct, std::forward_as_tuple(fid),
                                       std::forward_as_tuple(fid, std::move(feature_vec),
                                                             data_min_ts, data_max_ts));
                } else {
                    // 特征的merge, 数据发生merge时，ts不太好更新，目前数据合并时，不更新ts
                    std::vector<int64_t>* dest_vec = it->second.GetMutableFeatureDataVec();
                    size_t dest_feature_size = dest_vec->size();
                    size_t cur_feature_size = feature_vec.size();
                    size_t max_size = std::max(cur_feature_size, dest_feature_size);
                    dest_vec->reserve(max_size);

                    for (size_t index = 0; index < cur_feature_size; ++index) {
                        if (index >= dest_feature_size) {
                            dest_vec->emplace_back(feature_vec[index]);
                        } else {
                            int64_t& dest = (*dest_vec)[index];
                            Status ret = DataMerge(feature_vec[index], dest, index, max_size,
                                                   reduce_type, &dest);
                            if (UNLIKELY(!ret.ok())) {
                                iter_ret = ret;
                                return false;
                            }
                        }
                    }
                }
            }
        } else {
            iter_ret = Status::Internal("serialize error");
            // BC_ERROR_DEFAULT_RATE_LIMIT("decode_ips_data_from_big_endian failed");
            return false;
        }
        return true;
    };

    std::string scan_start_key = GetRangeStrKey(min_data_ts_micros);
    Status ret = ordered_tree->OrSet().Scan(scan_start_key, iter);
    if (UNLIKELY(!ret.ok() || !iter_ret.ok())) {
        if (!ret.IsNotFound()) {
            // BC_ERROR_DEFAULT_RATE_LIMIT("scan failed, ret: {}", ret.ToString());
        }
        return ret.ok() ? iter_ret : ret;
    }

    if (!is_sort_by_ts) {
        res->reserve(range_res->size());
        for (auto iter = range_res->begin(); iter != range_res->end(); ++iter) {
            res->emplace_back(std::move(iter->second));
        }
    }
    *range_min_ts = min_ts;
    *range_max_ts = max_ts;
    return Status::OK();
}

Status IPSOperator::RangeGet(model::IpsModel* ordered_tree, int64_t base_ts, uint8_t tree_version,
                             int64_t cnt, std::vector<IPSFeatureData>* res, int64_t* range_min_ts,
                             int64_t* range_max_ts, ReduceType reduce_type) {
    if (cnt <= 0) {
        return Status::InvalidArgument("cnt <= 0");
    }
    res->clear();

    std::unique_ptr<std::unordered_map<int64_t, IPSFeatureData>> range_res =
        absl::make_unique<std::unordered_map<int64_t, IPSFeatureData>>();

    int64_t min_ts = INT64_MAX;
    int64_t max_ts = INT64_MIN;
    Status iter_ret;
    std::vector<int64_t> feature_vec;

    PersistentMap<std::string, std::string>::IterateFunc iter =
        [&min_ts, &max_ts, &range_res, &iter_ret, &feature_vec, reduce_type, tree_version, base_ts,
         &cnt](const std::string& key_str, const std::string& v_str) -> bool {
        const Slice key(key_str), v(v_str);
        if (UNLIKELY(IsInvalidTreeKV(key, v, tree_version))) {
            return true;
        }

        if (cnt-- <= 0) {
            return false;
        }

        // key decode: 7byte max_ts + 8 byte fid
        int64_t data_max_ts = DecodeInt56FromBigEndian(key.data());
        int64_t fid = DecodeInt64FromBigEndian(key.data() + 7);
        int64_t data_min_ts = 0;
        bool decode_ips_data_res = DecodeIpsData(key.data(), v.data(), v.size(), tree_version,
                                                 base_ts, reduce_type, &data_min_ts, &feature_vec);
        if (LIKELY(decode_ips_data_res)) {
            if (max_ts < data_max_ts) {
                max_ts = data_max_ts;
            }
            if (min_ts > data_min_ts) {
                min_ts = data_min_ts;
            }

            auto it = range_res->find(fid);
            if (it == range_res->end()) {
                range_res->emplace(
                    std::piecewise_construct, std::forward_as_tuple(fid),
                    std::forward_as_tuple(fid, std::move(feature_vec), data_min_ts, data_max_ts));
            } else {
                // 特征的merge, 数据发生merge时，ts不太好更新，目前数据合并时，不更新ts
                std::vector<int64_t>* dest_vec = it->second.GetMutableFeatureDataVec();
                size_t dest_feature_size = dest_vec->size();
                size_t cur_feature_size = feature_vec.size();
                size_t max_size = std::max(cur_feature_size, dest_feature_size);
                dest_vec->reserve(max_size);

                for (size_t index = 0; index < cur_feature_size; ++index) {
                    if (index >= dest_feature_size) {
                        dest_vec->emplace_back(feature_vec[index]);
                    } else {
                        int64_t& dest = (*dest_vec)[index];
                        Status ret = DataMerge(feature_vec[index], dest, index, max_size,
                                               reduce_type, &dest);
                        if (UNLIKELY(!ret.ok())) {
                            iter_ret = ret;
                            return false;
                        }
                    }
                }
            }

        } else {
            iter_ret = Status::Internal("serialize error");
            // BC_ERROR_DEFAULT_RATE_LIMIT("decode_ips_data_from_big_endian failed");
            return false;
        }
        return true;
    };

    Status ret = ordered_tree->OrSet().ScanBackward("", iter);
    if (UNLIKELY(!ret.ok() || !iter_ret.ok())) {
        if (!ret.IsNotFound()) {
            // BC_ERROR_DEFAULT_RATE_LIMIT("scan failed, ret: {}", ret.ToString());
        }
        return ret.ok() ? iter_ret : ret;
    }

    res->reserve(range_res->size());
    for (auto iter = range_res->begin(); iter != range_res->end(); ++iter) {
        res->emplace_back(std::move(iter->second));
    }

    *range_min_ts = min_ts;
    *range_max_ts = max_ts;
    return Status::OK();
}

//  filter by fid with last instance query
Status IPSOperator::RangeGetFilterByFid(model::IpsModel* ordered_tree, int64_t base_ts,
                                        uint8_t tree_version, int64_t cnt,
                                        const std::unordered_set<int64_t>& fid_set,
                                        std::vector<IPSFeatureData>* res, int64_t* range_min_ts,
                                        int64_t* range_max_ts, ReduceType reduce_type) {
    if (UNLIKELY(cnt <= 0)) {
        return Status::InvalidArgument("cnt <= 0");
    }
    res->clear();

    std::unique_ptr<std::unordered_map<int64_t, IPSFeatureData>> range_res =
        absl::make_unique<std::unordered_map<int64_t, IPSFeatureData>>();
    range_res->reserve(fid_set.size());

    int64_t min_ts = INT64_MAX;
    int64_t max_ts = INT64_MIN;
    Status iter_ret;
    std::vector<int64_t> feature_vec;

    PersistentMap<std::string, std::string>::IterateFunc iter =
        [&min_ts, &max_ts, &range_res, &iter_ret, &feature_vec, reduce_type, tree_version, base_ts,
         &cnt, &fid_set](const std::string& key_str, const std::string& v_str) -> bool {
        const Slice &key(key_str), &v(v_str);
        if (UNLIKELY(IsInvalidTreeKV(key, v, tree_version))) {
            return true;
        }

        // key decode: 7byte max_ts + 8 byte fid
        int64_t fid = DecodeInt64FromBigEndian(key.data() + 7);
        if (fid_set.find(fid) == fid_set.end()) {
            return true;
        }
        if (cnt-- <= 0) {
            return false;
        }

        int64_t data_max_ts = DecodeInt56FromBigEndian(key.data());
        int64_t data_min_ts = 0;
        bool decode_ips_data_res = DecodeIpsData(key.data(), v.data(), v.size(), tree_version,
                                                 base_ts, reduce_type, &data_min_ts, &feature_vec);
        if (LIKELY(decode_ips_data_res)) {
            if (max_ts < data_max_ts) {
                max_ts = data_max_ts;
            }
            if (min_ts > data_min_ts) {
                min_ts = data_min_ts;
            }

            auto it = range_res->find(fid);
            if (it == range_res->end()) {
                range_res->emplace(
                    std::piecewise_construct, std::forward_as_tuple(fid),
                    std::forward_as_tuple(fid, std::move(feature_vec), data_min_ts, data_max_ts));
            } else {
                // 特征的merge, 数据发生merge时，ts不太好更新，目前数据合并时，不更新ts
                std::vector<int64_t>* dest_vec = it->second.GetMutableFeatureDataVec();
                size_t dest_feature_size = dest_vec->size();
                size_t cur_feature_size = feature_vec.size();
                size_t max_size = std::max(cur_feature_size, dest_feature_size);
                dest_vec->reserve(max_size);

                for (size_t index = 0; index < cur_feature_size; ++index) {
                    if (index >= dest_feature_size) {
                        dest_vec->emplace_back(feature_vec[index]);
                    } else {
                        int64_t& dest = (*dest_vec)[index];
                        Status ret = DataMerge(feature_vec[index], dest, index, max_size,
                                               reduce_type, &dest);
                        if (UNLIKELY(!ret.ok())) {
                            iter_ret = ret;
                            return false;
                        }
                    }
                }
            }

        } else {
            iter_ret = Status::Internal("serialize error");
            // BC_ERROR_DEFAULT_RATE_LIMIT("decode_ips_data_from_big_endian failed");
            return false;
        }
        return true;
    };

    Status ret = ordered_tree->OrSet().ScanBackward("", iter);
    if (UNLIKELY(!ret.ok() || !iter_ret.ok())) {
        if (!ret.IsNotFound()) {
            // BC_ERROR_DEFAULT_RATE_LIMIT("scan failed, ret: {}", ret.ToString());
        }
        return ret.ok() ? iter_ret : ret;
    }

    if (range_res->empty()) {
        return Status::NotFound("NotFound");
    }

    res->reserve(range_res->size());
    for (auto iter = range_res->begin(); iter != range_res->end(); ++iter) {
        res->emplace_back(std::move(iter->second));
    }

    *range_min_ts = min_ts;
    *range_max_ts = max_ts;
    return Status::OK();
}

// filter by fid with time range query
Status IPSOperator::RangeGetFilterByFid(model::IpsModel* ordered_tree, int64_t base_ts,
                                        uint8_t tree_version, int64_t min_data_ts_micros,
                                        int64_t max_data_ts_micros,
                                        const std::unordered_set<int64_t>& fid_set,
                                        std::vector<IPSFeatureData>* res, int64_t* range_min_ts,
                                        int64_t* range_max_ts, ReduceType reduce_type) {
    assert(min_data_ts_micros <= max_data_ts_micros);
    res->clear();

    std::unique_ptr<std::unordered_map<int64_t, IPSFeatureData>> range_res =
        absl::make_unique<std::unordered_map<int64_t, IPSFeatureData>>();
    range_res->reserve(fid_set.size());

    int64_t min_ts = INT64_MAX;
    int64_t max_ts = INT64_MIN;
    Status iter_ret;
    std::vector<int64_t> feature_vec;

    PersistentMap<std::string, std::string>::IterateFunc iter =
        [res, &min_ts, &max_ts, &range_res, &iter_ret, &feature_vec, reduce_type,
         max_data_ts_micros, min_data_ts_micros, tree_version, base_ts,
         &fid_set](const std::string& key_str, const std::string& v_str) -> bool {
        const Slice &key(key_str), &v(v_str);
        if (UNLIKELY(IsInvalidTreeKV(key, v, tree_version))) {
            return true;
        }

        // key decode: 7byte max_ts + 8 byte fid
        int64_t fid = DecodeInt64FromBigEndian(key.data() + 7);

        int64_t data_max_ts = DecodeInt56FromBigEndian(key.data());
        int64_t data_min_ts = 0;
        bool decode_ips_data_res = DecodeIpsData(key.data(), v.data(), v.size(), tree_version,
                                                 base_ts, reduce_type, &data_min_ts, &feature_vec);
        if (LIKELY(decode_ips_data_res)) {
            if (data_min_ts > max_data_ts_micros) {
                return false;
            }
            if (data_max_ts < min_data_ts_micros) {
                return true;
            }
            if (fid_set.find(fid) == fid_set.end()) {
                return true;
            }

            if (max_ts < data_max_ts) {
                max_ts = data_max_ts;
            }
            if (min_ts > data_min_ts) {
                min_ts = data_min_ts;
            }

            auto it = range_res->find(fid);
            if (it == range_res->end()) {
                range_res->emplace(
                    std::piecewise_construct, std::forward_as_tuple(fid),
                    std::forward_as_tuple(fid, std::move(feature_vec), data_min_ts, data_max_ts));
            } else {
                // 特征的merge, 数据发生merge时，ts不太好更新，目前数据合并时，不更新ts
                std::vector<int64_t>* dest_vec = it->second.GetMutableFeatureDataVec();
                size_t dest_feature_size = dest_vec->size();
                size_t cur_feature_size = feature_vec.size();
                size_t max_size = std::max(cur_feature_size, dest_feature_size);
                dest_vec->reserve(max_size);

                for (size_t index = 0; index < cur_feature_size; ++index) {
                    if (index >= dest_feature_size) {
                        dest_vec->emplace_back(feature_vec[index]);
                    } else {
                        int64_t& dest = (*dest_vec)[index];
                        Status ret = DataMerge(feature_vec[index], dest, index, max_size,
                                               reduce_type, &dest);
                        if (UNLIKELY(!ret.ok())) {
                            iter_ret = ret;
                            return false;
                        }
                    }
                }
            }
        } else {
            iter_ret = Status::Internal("serialize error");
            // BC_ERROR_DEFAULT_RATE_LIMIT("decode_ips_data_from_big_endian failed");
            return false;
        }
        return true;
    };

    std::string scan_start_key = GetRangeStrKey(min_data_ts_micros);
    Status ret = ordered_tree->OrSet().Scan(scan_start_key, iter);
    if (UNLIKELY(!ret.ok() || !iter_ret.ok())) {
        if (!ret.IsNotFound()) {
            // BC_ERROR_DEFAULT_RATE_LIMIT("scan failed, ret: {}", ret.ToString());
        }
        return ret.ok() ? iter_ret : ret;
    }

    if (range_res->empty()) {
        return Status::NotFound("NotFound");
    }

    res->reserve(range_res->size());
    for (auto iter = range_res->begin(); iter != range_res->end(); ++iter) {
        res->emplace_back(std::move(iter->second));
    }
    *range_min_ts = min_ts;
    *range_max_ts = max_ts;
    return Status::OK();
}

// strong filter with range query
Status IPSOperator::RangeGetStrongFilter(model::IpsModel* ordered_tree, int64_t base_ts,
                                         uint8_t tree_version, int64_t min_data_ts_micros,
                                         int64_t max_data_ts_micros,
                                         std::vector<int32_t>* index_strong,
                                         std::vector<int32_t>* min_index_count, int64_t top_k,
                                         std::vector<IPSFeatureData>* res, int64_t* range_min_ts,
                                         int64_t* range_max_ts, ReduceType reduce_type) {
    assert(min_data_ts_micros <= max_data_ts_micros);

    // strong filter variable init
    int max_return_size = (*min_index_count)[0];
    for (auto count : (*min_index_count)) {
        max_return_size += count;
    }
    res->clear();
    res->reserve(max_return_size);
    // index_strong_fids[index]存储的是已经满足index这个强约束的fid
    std::unordered_map<int32_t, std::unordered_set<int64_t>> index_strong_fids;
    index_strong_fids.reserve(index_strong->size());
    for (int i = 0; i < index_strong->size(); ++i) {
        index_strong_fids[index_strong->at(i)].reserve(min_index_count->at(i));
    }
    int traversed_fid_count = 0;  // 记录server端遍历过的fid数量

    // index_strong中的[0, end_index)存储的是未满足的强约束
    int end_index = index_strong->size();
    // 返回前min_index_count[0]个不满足强约束的fid
    int not_index_strong_fid_count = min_index_count->front();
    int64_t fid_cnt = 0;

    int64_t min_ts = INT64_MAX;
    int64_t max_ts = INT64_MIN;
    Status iter_ret;
    std::vector<int64_t> feature_vec;
    PersistentMap<std::string, std::string>::IterateFunc iter =
        [res, &min_ts, &max_ts, &iter_ret, &feature_vec, reduce_type, max_data_ts_micros,
         min_data_ts_micros, tree_version, base_ts, &index_strong_fids, &end_index,
         &not_index_strong_fid_count, &traversed_fid_count, min_index_count, index_strong,
         top_k](const std::string& key_str, const std::string& v_str) -> bool {
        const Slice &key(key_str), &v(v_str);
        if (UNLIKELY(IsInvalidTreeKV(key, v, tree_version))) {
            return true;
        }

        // key decode: 7byte max_ts + 8 byte fid
        int64_t fid = DecodeInt64FromBigEndian(key.data() + 7);
        int64_t data_max_ts = DecodeInt56FromBigEndian(key.data());
        int64_t data_min_ts = 0;
        bool decode_ips_data_res = DecodeIpsData(key.data(), v.data(), v.size(), tree_version,
                                                 base_ts, reduce_type, &data_min_ts, &feature_vec);

        if (LIKELY(decode_ips_data_res)) {
            if (data_min_ts > max_data_ts_micros) {
                return true;
            }
            if (data_max_ts < min_data_ts_micros) {
                return false;
            }
            ++traversed_fid_count;
            // 数据过滤
            bool fit_any_index_strong = false;  // 当前fid是否满足任何一个强约束
            bool need_return_client = false;    // 当前fid特征是否需要返回给客户端
            for (int index = 0; index < end_index;) {
                int32_t cur_index_strong = (*index_strong)[index];

                if (cur_index_strong < feature_vec.size() &&
                    feature_vec[cur_index_strong] != 0) {  // 当前fid至少满足了一个强约束
                    fit_any_index_strong = true;
                    if (index_strong_fids[cur_index_strong].find(fid) ==
                        index_strong_fids[cur_index_strong].end()) {
                        need_return_client = true;  // 当前IndexedFeatureStat需要返回给client
                        index_strong_fids[cur_index_strong].insert(fid);
                        if (index_strong_fids[cur_index_strong].size() >=
                            (*min_index_count)[index]) {
                            // 当前的强约束已经满足，清空已满足强约束的辅助存储空间
                            index_strong_fids.erase(cur_index_strong);
                            // index_strong中[0, end_index)存储的是没有满足强约束的元素，
                            // 后续只会遍历index_strong中[0, end_index)索引区间的元素,
                            // 已经满足约束的元素会被移动到区间外
                            int32_t tmp = index_strong->at(index);
                            (*index_strong)[index] = (*index_strong)[--end_index];
                            (*index_strong)[end_index] = tmp;
                            // min_index_count需要和index_strong中的元素相对应
                            tmp = (*min_index_count)[index];
                            (*min_index_count)[index] = (*min_index_count)[end_index];
                            (*min_index_count)[end_index] = tmp;
                            // 因为在上面循环体中已经更改了index_strong在index位置的值，
                            // 所以此时不需要执行循环末尾的更新index语句，直接进入下次循环即可
                            continue;
                        }
                    }
                }
                ++index;
            }

            bool need_update_ts = false;
            if (need_return_client) {
                need_update_ts = true;
                res->emplace_back(fid, std::move(feature_vec), data_min_ts, data_max_ts);
            } else if (not_index_strong_fid_count > 0 && !fit_any_index_strong) {
                // 返回前not_index_strong_fid_count个不满足任何强约束的fid
                need_update_ts = true;
                res->emplace_back(fid, std::move(feature_vec), data_min_ts, data_max_ts);
                --not_index_strong_fid_count;
            }
            if (need_update_ts) {
                if (max_ts < data_max_ts) {
                    max_ts = data_max_ts;
                }
                if (min_ts > data_min_ts) {
                    min_ts = data_min_ts;
                }
            }
            if (UNLIKELY(end_index == 0 || traversed_fid_count >= top_k)) {
                return false;
            }
        } else {
            iter_ret = Status::Internal("serialize error");
            //  BC_ERROR_DEFAULT_RATE_LIMIT("decode_ips_data_from_big_endian failed");
            return false;
        }
        return true;
    };

    Status ret = ordered_tree->OrSet().ScanBackward("", iter);
    if (UNLIKELY(!ret.ok() || !iter_ret.ok())) {
        if (!ret.IsNotFound()) {
            //  BC_ERROR_DEFAULT_RATE_LIMIT("scan failed, ret: {}", ret.ToString());
        }
        return ret.ok() ? iter_ret : ret;
    }

    *range_min_ts = min_ts;
    *range_max_ts = max_ts;

    return Status::OK();
}

Status IPSOperator::RangeGetFilterByVx(model::IpsModel* ordered_tree, int64_t base_ts,
                                       uint8_t tree_version, int64_t min_data_ts_micros,
                                       int64_t max_data_ts_micros, int64_t cnt, int64_t vx,
                                       int64_t val, std::vector<IPSFeatureData>* res,
                                       int64_t* range_min_ts, int64_t* range_max_ts,
                                       ReduceType reduce_type) {
    if (UNLIKELY(cnt <= 0)) {
        return Status::InvalidArgument("cnt <= 0");
    }

    res->clear();
    Status iter_ret;
    *range_max_ts = INT64_MIN;
    *range_min_ts = INT64_MAX;
    std::vector<int64_t> feature_vec;
    PersistentMap<std::string, std::string>::IterateFunc iter_func =
        [min_data_ts_micros, max_data_ts_micros, range_min_ts, range_max_ts, reduce_type, res, &cnt,
         vx, val, &iter_ret, tree_version, base_ts,
         &feature_vec](const std::string& key_str, const std::string& v_str) -> bool {
        const Slice &key(key_str), &v(v_str);
        if (UNLIKELY(IsInvalidTreeKV(key, v, tree_version))) {
            return true;
        }
        if (cnt <= 0) {
            return false;
        }

        // key decode: 7byte max_ts + 8 byte fid
        int64_t data_max_ts = DecodeInt56FromBigEndian(key.data());
        int64_t fid = DecodeInt64FromBigEndian(key.data() + 7);

        int64_t data_min_ts = 0;
        bool decode_ips_data_res = DecodeIpsData(key.data(), v.data(), v.size(), tree_version,
                                                 base_ts, reduce_type, &data_min_ts, &feature_vec);
        if (LIKELY(decode_ips_data_res)) {
            if (data_min_ts > max_data_ts_micros) {
                return true;
            }
            if (data_max_ts < min_data_ts_micros) {
                return false;
            }

            if (feature_vec.size() <= vx) {
                return true;
            }
            if (feature_vec[vx] != val) {
                return true;
            }

            --cnt;
            res->emplace_back(fid, std::move(feature_vec), data_min_ts, data_max_ts);

            if (*range_max_ts < data_max_ts) {
                *range_max_ts = data_max_ts;
            }
            if (*range_min_ts > data_min_ts) {
                *range_min_ts = data_min_ts;
            }
        } else {
            iter_ret = Status::Internal("serialize error");
            // BC_ERROR_DEFAULT_RATE_LIMIT("decode_ips_data_from_big_endian failed");
            return false;
        }
        return true;
    };
    Status ret = ordered_tree->OrSet().ScanBackward("", iter_func);
    if (UNLIKELY(!ret.ok() || !iter_ret.ok())) {
        return ret.ok() ? iter_ret : ret;
    }
    if (res->empty()) {
        return Status::NotFound("NotFound");
    } else {
        return Status::OK();
    }
}

Status IPSOperator::RangeGetFilterByVx(model::IpsModel* ordered_tree, int64_t base_ts,
                                       uint8_t tree_version, int64_t cnt, int64_t vx, int64_t val,
                                       std::vector<IPSFeatureData>* res, int64_t* range_min_ts,
                                       int64_t* range_max_ts, ReduceType reduce_type) {
    if (UNLIKELY(cnt <= 0)) {
        return Status::InvalidArgument("cnt <= 0");
    }

    res->clear();
    Status iter_ret;
    *range_max_ts = INT64_MIN;
    *range_min_ts = INT64_MAX;
    std::vector<int64_t> feature_vec;
    PersistentMap<std::string, std::string>::IterateFunc iter_func =
        [range_min_ts, range_max_ts, reduce_type, res, &cnt, vx, val, &iter_ret, tree_version,
         base_ts, &feature_vec](const std::string& key_str, const std::string& v_str) -> bool {
        const Slice &key(key_str), &v(v_str);
        if (UNLIKELY(IsInvalidTreeKV(key, v, tree_version))) {
            return true;
        }
        if (cnt <= 0) {
            return false;
        }

        // key decode: 7byte max_ts + 8 byte fid
        int64_t data_max_ts = DecodeInt56FromBigEndian(key.data());
        int64_t fid = DecodeInt64FromBigEndian(key.data() + 7);

        int64_t data_min_ts = 0;

        bool decode_ips_data_res = DecodeIpsData(key.data(), v.data(), v.size(), tree_version,
                                                 base_ts, reduce_type, &data_min_ts, &feature_vec);
        if (LIKELY(decode_ips_data_res)) {
            if (feature_vec.size() <= vx) {
                return true;
            }
            if (feature_vec[vx] != val) {
                return true;
            }

            --cnt;
            res->emplace_back(fid, std::move(feature_vec), data_min_ts, data_max_ts);

            if (*range_max_ts < data_max_ts) {
                *range_max_ts = data_max_ts;
            }
            if (*range_min_ts > data_min_ts) {
                *range_min_ts = data_min_ts;
            }
        } else {
            iter_ret = Status::Internal("serialize error");
            //  BC_ERROR_DEFAULT_RATE_LIMIT("decode_ips_data_from_big_endian failed");
            return false;
        }
        return true;
    };
    Status ret = ordered_tree->OrSet().ScanBackward("", iter_func);
    if (UNLIKELY(!ret.ok() || !iter_ret.ok())) {
        return ret.ok() ? iter_ret : ret;
    }
    if (res->empty()) {
        return Status::NotFound("");
    } else {
        return Status::OK();
    }
}

// return min_ts encode len in val
size_t IPSOperator::GetVersionOneTreeValTs(ReduceType reduce_type, uint8_t tree_version,
                                           int64_t base_ts, const char* encode_key,
                                           const char* encode_val, int64_t* val_ts) {
    assert(tree_version == 1);
    if (LIKELY(tree_version == 1)) {
        if (reduce_type == ReduceType::IP_REDUCE_NONE) {
            *val_ts = DecodeInt56FromBigEndian(encode_key);
            return 0;
        } else {
            int32_t delta_ts;
            size_t encode_size = DecodeVarsignedint32(encode_val, kMaxVarint32Length, &delta_ts);
            assert(encode_size > 0);
            *val_ts = base_ts + delta_ts * 1000 * 1000;
            return encode_size;
        }
    } else {
        // BC_FATAL("invalid args, tree_version: {}", tree_version);
    }
    return 0;
}

// 返回完整覆盖在[min_data_ts_micros, max_data_ts_micros)区间的tree kv
Status IPSOperator::RangeGetByTime(ReduceType reduce_type, model::IpsModel* ordered_tree,
                                   int64_t base_ts, uint8_t tree_version,
                                   int64_t min_data_ts_micros, int64_t max_data_ts_micros,
                                   std::vector<std::pair<Slice, Slice>>* res, bool key_only) {
    res->clear();
    Status iter_ret;
    PersistentMap<std::string, std::string>::IterateFunc iter =
        [reduce_type, res, min_data_ts_micros, max_data_ts_micros, key_only, &iter_ret, base_ts,
         tree_version](const std::string& key_str, const std::string& v_str) -> bool {
        const Slice &key(key_str), &v(v_str);
        if (UNLIKELY(IsInvalidTreeKV(key, v, tree_version))) {
            return true;
        }
        int64_t cur_key_max_ts = DecodeInt56FromBigEndian(key.data());
        int64_t cur_key_min_ts;

        if (tree_version == 0) {
            cur_key_min_ts = DecodeInt64FromBigEndian(v.data() + 1);
        } else if (tree_version == 1) {
            if (reduce_type == ReduceType::IP_REDUCE_NONE) {
                cur_key_min_ts = cur_key_max_ts;
            } else {
                size_t decode_size = GetVersionOneTreeValTs(reduce_type, tree_version, base_ts,
                                                            key.data(), v.data(), &cur_key_min_ts);
                if (UNLIKELY(decode_size == 0)) {
                    //  BC_ERROR_DEFAULT_RATE_LIMIT("invalid tree value, key: {}, version:{}",
                    //  key.ToString(),
                    // tree_version);
                    return true;
                }
            }
        } else {
            //  BC_FATAL("unexpected tree version: {}", tree_version);
        }
        if (cur_key_min_ts < min_data_ts_micros) {
            return true;
        } else if (cur_key_max_ts >= max_data_ts_micros) {
            return false;
        } else if (key_only) {
            res->emplace_back(std::make_pair(key, kEmptySlice));
        } else {
            res->emplace_back(std::make_pair(key, v));
        }
        return true;
    };

    std::string scan_start_key = GetRangeStrKey(min_data_ts_micros);
    Status ret = ordered_tree->OrSet().Scan(scan_start_key, iter);
    if (UNLIKELY(!ret.ok() || !iter_ret.ok())) {
        return ret.ok() ? iter_ret : ret;
    } else {
        return Status::OK();
    }
}

Status IPSOperator::GetTreeValTs(ReduceType reduce_type, const Slice& tree_key,
                                 const Slice& tree_value, int64_t base_ts, uint8_t tree_version,
                                 int64_t* val_ts) {
    if (tree_version == 0) {
        *val_ts = DecodeInt64FromBigEndian(tree_value.data() + 1);
    } else if (tree_version == 1) {
        size_t decode_size = GetVersionOneTreeValTs(reduce_type, tree_version, base_ts,
                                                    tree_key.data(), tree_value.data(), val_ts);
        assert(decode_size > 0 ||
               ((decode_size == 0) && (reduce_type == ReduceType::IP_REDUCE_NONE)));
    } else {
        // BC_FATAL("unexpected tree version: {}", tree_version);
    }
    return Status::OK();
}

// scan tree key >= min_data_ts_micros && get cnt tree key
Status IPSOperator::RangeGetByCnt(ReduceType reduce_type, model::IpsModel* ordered_tree,
                                  int64_t base_ts, uint8_t tree_version, int64_t min_data_ts_micros,
                                  uint64_t cnt, std::vector<Slice>* res) {
    res->clear();
    Status iter_ret;
    PersistentMap<std::string, std::string>::IterateFunc iter =
        [reduce_type, res, min_data_ts_micros, &cnt, &iter_ret, base_ts, tree_version](
            const std::string& key_str, const std::string& v_str) -> bool {
        const Slice &key(key_str), &v(v_str);
        if (UNLIKELY(IsInvalidTreeKV(key, v, tree_version))) {
            return true;
        }
        if (cnt == 0) {
            return false;
        }
        int64_t cur_key_min_ts;
        iter_ret = GetTreeValTs(reduce_type, key, v, base_ts, tree_version, &cur_key_min_ts);
        if (UNLIKELY(!iter_ret.ok())) {
            return false;
        }
        if (cur_key_min_ts >= min_data_ts_micros) {
            res->emplace_back(key);
            --cnt;
        }
        return true;
    };
    std::string scan_start_key = GetRangeStrKey(min_data_ts_micros);
    Status ret = ordered_tree->OrSet().Scan(scan_start_key, iter);
    if (UNLIKELY(!ret.ok())) {
        // BC_ERROR_DEFAULT_RATE_LIMIT("range_get_by_cnt_: tree scan failed: {}", ret.ToString());
        res->clear();
        return ret;
    } else {
        return iter_ret;
    }
}

void IPSOperator::ReserveKElements(std::vector<IPSFeatureData>* res, uint64_t top_k, bool first_k) {
    if (!first_k) {
        std::reverse(res->begin(), res->end());
    }

    if (res->size() <= top_k) {
        return;
    }

    res->resize(top_k);
}

Status IPSOperator::CompactDataMerge_(ReduceType reduce_type, int64_t base_ts, uint8_t tree_version,
                                      const std::vector<std::pair<Slice, Slice>>& compact_data,
                                      std::vector<std::pair<IPSSlice, IPSSlice>>* compact_res,
                                      std::vector<IPSSlice>* key_gc) {
    int64_t data_size = compact_data.size();
    assert(data_size > 1);
    compact_res->reserve(data_size);
    key_gc->reserve(data_size);

    std::vector<IPSSlice> key_gc_helper;
    std::unordered_set<int64_t> merged_fid;
    key_gc_helper.reserve(data_size);
    merged_fid.reserve(data_size);
    std::unordered_map<int64_t, std::pair<IPSSlice, IPSSlice>> fid_to_kv_map;
    fid_to_kv_map.reserve(data_size);
    for (auto iter = compact_data.cbegin(); iter != compact_data.cend(); ++iter) {
        const Slice& key = iter->first;
        const Slice& val = iter->second;
        int64_t fid = DecodeInt64FromBigEndian(key.data() + 7);
        auto cur_iter_res = fid_to_kv_map.find(fid);
        if (cur_iter_res == fid_to_kv_map.end()) {
            std::pair<IPSSlice, IPSSlice> p = std::make_pair(IPSSlice(key), IPSSlice(val));
            fid_to_kv_map.emplace(fid, std::move(p));
        } else {
            std::pair<IPSSlice, IPSSlice>& kv = cur_iter_res->second;
            if (merged_fid.find(fid) == merged_fid.end()) {
                key_gc_helper.emplace_back(kv.first.GetSlice());
                merged_fid.emplace(fid);
            }
            key_gc_helper.emplace_back(key);

            // val merge
            std::string merged_val;
            Status ret = TreeValMerge(kv.first.GetSlice(), kv.second.GetSlice(), key, val,
                                      &merged_val, reduce_type, base_ts, tree_version);
            if (UNLIKELY(!ret.ok())) {
                return ret;
            } else {
                kv.second = IPSSlice(std::move(merged_val));
            }

            // key merge
            std::string merged_key;
            ret = TreeKeyMerge(key, kv.first.GetSlice(), &merged_key);
            if (UNLIKELY(!ret.ok())) {
                return ret;
            }
            kv.first = IPSSlice(std::move(merged_key));
        }
    }

    if (merged_fid.empty()) {
        return Status::NoAction("");
    }

    for (int64_t cur_fid : merged_fid) {
        compact_res->emplace_back(std::move(fid_to_kv_map.at(cur_fid)));
    }
    key_gc->insert(key_gc->end(), std::make_move_iterator(key_gc_helper.begin()),
                   std::make_move_iterator(key_gc_helper.end()));

    return Status::OK();
}

Status IPSOperator::CompactResHandle(CmdContext* ctx,
                                     const std::vector<std::pair<IPSSlice, IPSSlice>>& compact_res,
                                     const std::vector<IPSSlice>& gc_vec,
                                     model::IpsModel* ordered_tree) {
    if (UNLIKELY(gc_vec.empty() || compact_res.empty())) {
        return Status::NoAction("");
    }
    // GC
    TimeCost tc_compact;
    for (auto const& cur_gc_key : gc_vec) {
        Status ret = ordered_tree->OrSet().Del(ctx, cur_gc_key.ToString());
        if (UNLIKELY(!ret.ok())) {
            // BC_ERROR_DEFAULT_RATE_LIMIT("compact gc, tree Del failed: tree_root: {}, ret: {}",
            // ordered_tree->OrSet().GetTreeRootKey(), ret.ToString());
            return Status::Internal("batch del failed");
        }
    }

    // Metrics::GetInstance()->Emit<kMetricTimer>("compact.data_delete_latency",
    // tc_compact.GetElapsed(),
    //                                            FLAGS_metrics_sample);
    // Metrics::GetInstance()->Emit<kMetricStore>("compact.delete_kv_num", gc_vec.size(),
    //                                            FLAGS_metrics_sample);

    tc_compact.Reset();
    for (auto const& cur_slice_pair : compact_res) {
        Status ret = ordered_tree->OrSet().Set(ctx, cur_slice_pair.first.ToString(),
                                               cur_slice_pair.second.ToString());
        if (UNLIKELY(!ret.ok())) {
            // BC_ERROR_DEFAULT_RATE_LIMIT("compact batch add failed, tree_root: {}, ret: {}",
            // ordered_tree->OrSet().GetTreeRootKey(), ret.ToString());
            return Status::Internal("CompactFailed");
        }
    }
    // Metrics::GetInstance()->Emit<kMetricTimer>("compact.data_insert_latency",
    // tc_compact.GetElapsed(),
    //                                            FLAGS_metrics_sample);
    // Metrics::GetInstance()->Emit<kMetricStore>("compact.insert_kv_num", compact_res.size(),
    //                                            FLAGS_metrics_sample);
    return Status::OK();
}

Status IPSOperator::TreeKeyMerge(const Slice& key1, const Slice& key2, std::string* merged_res) {
    int64_t fid1 = DecodeInt64FromBigEndian(key1.data() + 7);
    int64_t fid2 = DecodeInt64FromBigEndian(key2.data() + 7);
    if (fid1 != fid2) {
        return Status::InvalidArgument("key merged failed");
    }

    int64_t ts1 = DecodeInt56FromBigEndian(key1.data());
    int64_t ts2 = DecodeInt56FromBigEndian(key2.data());
    int64_t max_ts = std::max(ts1, ts2);
    *merged_res = GenerateIpsTreeKey(max_ts, fid1);
    return Status::OK();
}

Status IPSOperator::DataMerge(int64_t val1, int64_t val2, size_t index, size_t max_size,
                              ReduceType reduce_type, int64_t* merged_res) {
    assert(index < max_size);
    switch (reduce_type) {
    case IP_REDUCE_SUM:
        *merged_res = val1 + val2;
        return Status::OK();
    case IP_REDUCE_MAX:
        *merged_res = std::max(val1, val2);
        return Status::OK();
    case IP_REDUCE_SUM_MAX:
        if (UNLIKELY(max_size > 2)) {
            // BC_ERROR_DEFAULT_RATE_LIMIT(
            //    "tree_val_merge_: data type conflict: list size: {}, sum_max in list table",
            //    max_size);
            return Status::Unmatched("DataTypeConflict");
        }
        if (index == 0) {
            *merged_res = val1 + val2;
        } else {
            assert(index == 1);
            *merged_res = std::max(val1, val2);
        }
        return Status::OK();
    case IP_REDUCE_NONE:
        // Metrics::GetInstance()->Emit<kMetricCounter>("add.reduce_close_error.count", 1,
        //                                              FLAGS_metrics_sample);
        return Status::Internal("insert duplicate ts instance to sequence table");
    default:
        // BC_ERROR_DEFAULT_RATE_LIMIT("tree_val_merge_: invlid reduce type: {}", reduce_type);
        return Status::InvalidArgument("invalid table conf: reduce_type value is illegal");
    }
}

Status IPSOperator::TreeValMerge(const Slice& old_key, const Slice& old_val,
                                 const Slice& insert_key, const Slice& insert_val,
                                 std::string* merge_res, ReduceType reduce_type, int64_t base_ts,
                                 uint8_t tree_version) {
    // decode insert val
    std::vector<int64_t> insert_feature;
    int64_t insert_min_ts;
    bool decode_ret = DecodeIpsData(insert_key.data(), insert_val.data(), insert_val.size(),
                                    kCurExpectedTreeVersion, base_ts, reduce_type, &insert_min_ts,
                                    &insert_feature);
    if (UNLIKELY(!decode_ret)) {
        return Status::Internal("serialize error");
    }
    size_t insert_vec_size = insert_feature.size();

    // decode old val
    std::vector<int64_t> old_feature;
    int64_t old_min_ts;
    decode_ret =
        DecodeIpsData(old_key.data(), old_val.data(), old_val.size(), kCurExpectedTreeVersion,
                      base_ts, reduce_type, &old_min_ts, &old_feature);
    if (UNLIKELY(!decode_ret)) {
        return Status::Internal("serialize error");
    }
    size_t old_vec_size = old_feature.size();

    // data merge
    int64_t merged_min_ts_res = std::min(insert_min_ts, old_min_ts);
    std::vector<int64_t> merged_feature_res;
    size_t merge_vec_size = std::max(old_vec_size, insert_vec_size);
    merged_feature_res.reserve(merge_vec_size);

    // feature data merge
    for (size_t i = 0; i < merge_vec_size; ++i) {
        int64_t old_vec_val = i >= old_vec_size ? 0 : old_feature[i];
        int64_t insert_vec_val = i >= insert_vec_size ? 0 : insert_feature[i];

        int64_t cur_merged_res = 0;
        Status ret =
            DataMerge(old_vec_val, insert_vec_val, i, merge_vec_size, reduce_type, &cur_merged_res);
        if (UNLIKELY(!ret.ok())) {
            return ret;
        }
        merged_feature_res.push_back(cur_merged_res);
    }

    *merge_res = EncodeIpsData(reduce_type, merged_min_ts_res, base_ts, merged_feature_res);
    return Status::OK();
}

bool IPSOperator::IsInvalidTreeKV(const Slice& key, const Slice& value, uint8_t tree_version) {
    bool key_valid = key.size() == 15;
    bool version_valid = (tree_version == 0) ? (value[0] == 0) : tree_version == 1;
    bool value_valid = tree_version == 0 ? value.size() >= 13 : tree_version == 1;
    if (LIKELY(key_valid && version_valid && value_valid)) {
        return false;
    } else {
        // BC_ERROR_DEFAULT_RATE_LIMIT(
        //    "invalid tree kv, key: {}, key_size: {}, value: {}, value_size: {}, value_version:
        //    {}", key.ToString(), key.size(), value.ToString(), value.size(), value[0]);
        return true;
    }
}

Status IPSOperator::UptateTreeValToExpectedVersion(model::IpsModel* ordered_tree,
                                                   uint8_t tree_version, int64_t base_ts,
                                                   ReduceType reduce_type) {
    if (tree_version == kCurExpectedTreeVersion) {
        return Status::OK();
    }

    std::vector<std::pair<Slice, Slice>> res;
    Status ret = RangeGetByTime(reduce_type, ordered_tree, base_ts, tree_version, INT64_MIN,
                                INT64_MAX, &res, false);
    if (ret.IsNotFound()) {
        return Status::OK();
    }

    if (!ret.ok()) {
        // BC_ERROR_DEFAULT_RATE_LIMIT(
        //    "tree range get faied,ret: {}, cur_tree_version: {}, expected tree_version: {},",
        //    ret.ToString(), tree_version, kCurExpectedTreeVersion);
        return ret;
    }

    std::vector<std::pair<std::string, std::string>> new_value_format;
    new_value_format.reserve(res.size());
    std::vector<int64_t> feature_vec;
    for (auto& kv_pair : res) {
        feature_vec.clear();

        std::string key = kv_pair.first.ToString();
        const Slice& value = kv_pair.second;

        // decode old format value
        int64_t min_ts;
        bool decode_ret = DecodeIpsData(key.data(), value.data(), value.size(), tree_version,
                                        base_ts, reduce_type, &min_ts, &feature_vec);
        if (!decode_ret) {
            return Status::Internal("serialize error");
        }

        // encode kCurExpectedTreeVersion value format
        std::string new_format_value = EncodeIpsData(reduce_type, min_ts, base_ts, feature_vec);
        new_value_format.emplace_back(std::make_pair(std::move(key), std::move(new_format_value)));
    }

    // ret = ordered_tree->OrSet().BatchAdd(new_value_format);
    if (UNLIKELY(!ret.ok())) {
        // BC_ERROR_DEFAULT_RATE_LIMIT("batch add failed, ret: {}", ret.ToString());
    }

    return ret;
}

Status IPSOperator::UpdateTreeMetaFromZeroVersionToOne(CmdContext* ctx,
                                                       model::IpsModel* ordered_tree,
                                                       uint8_t cur_version, int64_t base_ts) {
    if (UNLIKELY(cur_version != 0 || kCurExpectedTreeVersion != 1)) {
        // BC_ERROR_DEFAULT_RATE_LIMIT("invalid meta convert, cur_version: {},
        // kCurExpectedTreeVersion: {}",
        // cur_version, kCurExpectedTreeVersion);
        return Status::InvalidArgument("InvalidArgument");
    }

    std::string tree_meta;
    Status ret = ordered_tree->OrSet().GetUserMeta(&tree_meta);
    if (!ret.ok()) {
        return ret;
    }
    if (UNLIKELY(tree_meta.size() != kVersionZeroMetaSize)) {
        return Status::InvalidArgument("invalid meta size");
    }

    tree_meta[kTreeMetaVersionOffset] = kCurExpectedTreeVersion;
    std::string base_ts_encode(sizeof(int64_t), 0);
    EncodeInt64ToBigEndian(base_ts, &base_ts_encode.front());

    tree_meta.append(base_ts_encode);
    if (UNLIKELY(tree_meta.size() != kVersionOneMetaSize)) {
        // BC_ERROR_DEFAULT_RATE_LIMIT(
        //    "update tree meta failed: cur_version: {}, new_version: {}, new_meta_size: {}, "
        //    "expected_meta_size: {}",
        //    cur_version, kCurExpectedTreeVersion, tree_meta.size(), kVersionOneMetaSize);
        return Status::InvalidArgument("InvalidArgument");
    }
    return ordered_tree->OrSet().UpdateUserMeta(ctx, tree_meta);
}

Status IPSOperator::UpdateTreeMetaFromOneVersionToZero(CmdContext* ctx,
                                                       model::IpsModel* ordered_tree,
                                                       uint8_t cur_version) {
    if (UNLIKELY(cur_version != 1 || kCurExpectedTreeVersion != 0)) {
        // BC_ERROR_DEFAULT_RATE_LIMIT("invalid meta convert, cur_version: {},
        // kCurExpectedTreeVersion: {}",
        // cur_version, kCurExpectedTreeVersion);
        return Status::InvalidArgument("InvalidArgument");
    }
    std::string tree_meta;
    Status ret = ordered_tree->OrSet().GetUserMeta(&tree_meta);
    if (!ret.ok()) {
        return ret;
    }
    if (UNLIKELY(tree_meta.size() != kVersionOneMetaSize)) {
        return Status::InvalidArgument("invalid meta size in version zero");
    }

    tree_meta[kTreeMetaVersionOffset] = kCurExpectedTreeVersion;
    tree_meta.resize(kVersionZeroMetaSize);
    return ordered_tree->OrSet().UpdateUserMeta(ctx, tree_meta);
}

Status IPSOperator::CheckAndUpdateBaseTsMeta(CmdContext* ctx, model::IpsModel* ordered_tree,
                                             uint8_t cur_tree_version, int64_t base_ts) {
    assert(cur_tree_version == 1);
    assert(kCurExpectedTreeVersion == 1);

    std::string tree_meta;
    Status ret = ordered_tree->OrSet().GetUserMeta(&tree_meta);
    if (!ret.ok()) {
        return ret;
    }
    assert(tree_meta[kTreeMetaVersionOffset] == 1);
    assert(tree_meta.size() == kVersionOneMetaSize);

    int64_t cur_base_ts = DecodeInt64FromBigEndian(tree_meta.data() + kTreeMetaBaseTsUsOffset);
    if (cur_base_ts == base_ts) {
        return Status::OK();
    }
    EncodeInt64ToBigEndian(base_ts, &tree_meta.front() + kTreeMetaBaseTsUsOffset);
    return ordered_tree->OrSet().UpdateUserMeta(ctx, tree_meta);
}

Status IPSOperator::UpdateTreeMetaToExpectedVersion(CmdContext* ctx, model::IpsModel* ordered_tree,
                                                    uint8_t cur_tree_version, int64_t base_ts) {
    assert(kCurExpectedTreeVersion == 0 || kCurExpectedTreeVersion == 1);
    assert(cur_tree_version == 0 || cur_tree_version == 1);

    switch (cur_tree_version) {
    case 0:
        if (kCurExpectedTreeVersion == 1) {
            return UpdateTreeMetaFromZeroVersionToOne(ctx, ordered_tree, cur_tree_version, base_ts);
        } else {
            assert(kCurExpectedTreeVersion == 0);
            return Status::OK();
        }
    case 1:
        if (kCurExpectedTreeVersion == 0) {
            return UpdateTreeMetaFromOneVersionToZero(ctx, ordered_tree, cur_tree_version);
        } else {
            assert(kCurExpectedTreeVersion == 1);
            return CheckAndUpdateBaseTsMeta(ctx, ordered_tree, cur_tree_version, base_ts);
        }
        // default:
        // BC_FATAL("invalid tree version: {}, kCurExpectedTreeVersion: {}", cur_tree_version,
        //         kCurExpectedTreeVersion);
    }
    return Status::InvalidArgument("InvalidArgument");
}

// if tree_version is 0, *base_ts  = -1;
// if tree_version is 1 and tree is empty, *base_ts  = 0;
Status IPSOperator::GetQueryTreeVersionAndBaseTs(model::IpsModel* ordered_tree,
                                                 uint8_t* tree_version, int64_t* base_ts) {
    std::string tree_meta;
    Status ret = ordered_tree->OrSet().GetUserMeta(&tree_meta);
    if (!ret.ok()) {
        return ret;
    }
    *tree_version = tree_meta[kTreeMetaVersionOffset];
    assert(*tree_version == 0 || *tree_version == 1);
    if (*tree_version == 1) {
        assert(tree_meta.size() == kVersionOneMetaSize);
        assert(&tree_meta.front() + kTreeMetaBaseTsUsOffset <= &tree_meta.back());

        *base_ts = DecodeInt64FromBigEndian(tree_meta.data() + kTreeMetaBaseTsUsOffset);
        if (*base_ts == 0) {
            // empty tree
            uint64_t tree_size;
            ret = ordered_tree->OrSet().Size(&tree_size);
            if (UNLIKELY(!ret.ok())) {
                return ret;
            }
            if (UNLIKELY(tree_size != 0)) {
                return Status::InvalidArgument("InvalidArgument");
            }
        }
        assert(*base_ts >= 0);
    } else if (*tree_version == 0) {
        *base_ts = -1;
    } else {
        // BC_ERROR_DEFAULT_RATE_LIMIT("invalid tree version: {}, root_key: {}, tree_meta size: {}",
        //                             *tree_version, ordered_tree->OrSet().GetTreeRootKey(),
        //                             tree_meta.size());
        // Metrics::GetInstance()->Emit<kMetricCounter>("ips.invalid_tree_version", 1);
        return Status::Internal("serialize error");
    }
    return Status::OK();
}

Status IPSOperator::GetInsertTreeVersionAndBaseTs(ReduceType reduce_type,
                                                  model::IpsModel* ordered_tree, int64_t insert_ts,
                                                  uint8_t* tree_version, int64_t* base_ts) {
    if (UNLIKELY(kCurExpectedTreeVersion != 0 && kCurExpectedTreeVersion != 1)) {
        // BC_ERROR_DEFAULT_RATE_LIMIT("invalid kCurExpectedTreeVersion: {}",
        // kCurExpectedTreeVersion);
        return Status::InvalidArgument("unexpected error, contact oncall");
    }
    // get tree meta
    std::string tree_meta;
    Status ret = ordered_tree->OrSet().GetUserMeta(&tree_meta);
    if (!ret.ok()) {
        return ret;
    }

    if (UNLIKELY(tree_meta.size() != kVersionZeroMetaSize &&
                 tree_meta.size() != kVersionOneMetaSize)) {
        // BC_ERROR_DEFAULT_RATE_LIMIT(
        //     "invalid tree meta size, meta_size: {}, version 0 expected size: {},version "
        //    "1 expected size: {}",
        //     std::to_string(tree_meta.size()), kVersionZeroMetaSize, kVersionOneMetaSize);
        return Status::InvalidArgument("unexpected error, contact oncall");
    }

    // get tree version && base_ts
    *tree_version = tree_meta[kTreeMetaVersionOffset];
    if (UNLIKELY(*tree_version != 0 && *tree_version != 1)) {
        // BC_ERROR_DEFAULT_RATE_LIMIT("invalid tree_version: {}", std::to_string(*tree_version));
        return Status::InvalidArgument("unexpected error, contact oncall");
    }
    if (UNLIKELY((*tree_version == 0) ? (tree_meta.size() != kVersionZeroMetaSize)
                                      : (tree_meta.size() != kVersionOneMetaSize))) {
        // BC_ERROR_DEFAULT_RATE_LIMIT(
        //     "invalid tree meta size, tree_verison: {}, meta_size: {}, version 0 expected size:
        //     {},version " "1 expected size: {}", std::to_string(*tree_version),
        //     std::to_string(tree_meta.size()), kVersionZeroMetaSize, kVersionOneMetaSize);
        return Status::InvalidArgument("unexpected error, contact oncall");
    }

    *base_ts = 0;
    if (*tree_version == 0) {
        if (kCurExpectedTreeVersion == 0) {
            // version 0 do not have base_ts meta
            return Status::OK();
        } else {
            assert(kCurExpectedTreeVersion == 1);
            // tree min ts as tree's base_ts
            int64_t min_ts;
            ret = GetTreeMinKeyTs(&min_ts, ordered_tree, 0, *tree_version, reduce_type);
            if (UNLIKELY(ret.IsNotFound())) {  // empty tree
                *base_ts = insert_ts;
            } else if (UNLIKELY(!ret.ok())) {
                return ret;
            } else {
                *base_ts = min_ts;
            }
        }
    } else if (*tree_version == 1) {
        assert(&tree_meta.front() + kTreeMetaBaseTsUsOffset <= &tree_meta.back());
        *base_ts = DecodeInt64FromBigEndian(tree_meta.data() + kTreeMetaBaseTsUsOffset);
        if (*base_ts == 0) {
            // empty tree
            uint64_t tree_size;
            ret = ordered_tree->OrSet().Size(&tree_size);
            if (UNLIKELY(!ret.ok())) {
                return ret;
            }
            if (UNLIKELY(tree_size != 0)) {
                return Status::InvalidArgument("InvalidArgument");
            }

            if (kCurExpectedTreeVersion == 1) {
                *base_ts = insert_ts;
            }
        } else {
            assert(*base_ts > 0);
        }
    } else {
        // BC_FATAL("invalid tree version: {}", *tree_version)
    }

    return Status::OK();
}

Status IPSOperator::TtlHandler(CmdContext* ctx, model::IpsModel* ordered_tree,
                               int64_t min_valid_ts_us, int64_t* ttl_cnt, int64_t max_scan_ttl) {
    std::vector<Slice> ttl_keys;
    Status ret = ordered_tree->OrSet().Scan(
        "", [&](const std::string& k_str, const std::string& v_str) -> bool {
            const Slice k(k_str), v(v_str);
            int64_t kv_max_ts = GetTreeKvMaxTs(k, v);
            if (*ttl_cnt < max_scan_ttl && kv_max_ts < min_valid_ts_us) {
                ttl_keys.emplace_back(k);
                (*ttl_cnt)++;
                return true;
            } else {
                return false;
            }
        });
    if (UNLIKELY(!ret.ok())) {
        return ret;
    }
    if (ttl_keys.empty()) {
        return Status::NoAction("");
    }

    for (auto const& gc_key : ttl_keys) {
        const Slice k(gc_key), v;
        int64_t kv_max_ts = GetTreeKvMaxTs(k, v);
        ret = ordered_tree->OrSet().Del(ctx, gc_key.ToString());
        if (UNLIKELY(!ret.ok())) {
            return ret;
        }
    }
    return Status::OK();
}

Status IPSOperator::InsertFeatureStat(CmdContext* ctx, model::IpsModel* ordered_tree, int64_t ts,
                                      const FeatureStat32& fs, TableType table_type,
                                      ReduceType reduce_type, bool idempotent_add,
                                      uint8_t cur_tree_version, int64_t base_ts) {
    if (UNLIKELY(fs.type() != table_type)) {
        std::string msg = fmt::format("data type conflict, fs.type: {}, table type: {}",
                                      fs.type(), static_cast<int>(table_type));
        LOG_ERROR(msg.c_str());
        return Status::InvalidArgument(msg);
    }
    TimeCost tc_data_format_update;

    // update tree kv format to kCurExpectedTreeVersion format
    Status ret =
        UptateTreeValToExpectedVersion(ordered_tree, cur_tree_version, base_ts, reduce_type);
    if (UNLIKELY(!ret.ok())) {
        return ret;
    }
    // update tree meta to kCurExpectedTreeVersion format && update tree meta value
    ret = UpdateTreeMetaToExpectedVersion(ctx, ordered_tree, cur_tree_version, base_ts);
    if (UNLIKELY(!ret.ok())) {
        return ret;
    }
    // Metrics::GetInstance()->Emit<kMetricTimer>("add.format_update_latency",
    //                                            tc_data_format_update.GetElapsed(),
    //                                            FLAGS_metrics_sample);

    // now tree's val && meta is kCurExpectedTreeVersion format
    cur_tree_version = kCurExpectedTreeVersion;
    std::string encode_res;
    if (table_type == PAIR) {
        std::vector<int64_t> feature_data = {fs.int_pair().v1(), fs.int_pair().v2()};
        encode_res = EncodeIpsData(reduce_type, ts, base_ts, feature_data);
    } else if (table_type == LIST) {
        std::vector<int64_t> feature_vec;
        for (int i = 0; i < fs.int_list().v_list_size(); i++) {
            feature_vec.emplace_back(fs.int_list().v_list(i));
        }
        encode_res = EncodeIpsData(reduce_type, ts, base_ts, feature_vec);
    } else {
        LOG_ERROR("Invaild table type").put("TableType", table_type);
        return Status::InvalidArgument("illegal table conf: table_type value is illegal");
    }
    const std::string key = GenerateIpsTreeKey(ts, fs.id());

    bool need_merge = false;
    Slice tree_old_key, tree_old_val;
    ret = ordered_tree->OrSet().Scan(
        key, [&](const std::string& k_str, const std::string& v_str) -> bool {
            const Slice k(k_str), v(v_str);
            int64_t kv_min_ts, kv_max_ts;
            GetTreeKvMinAndMaxTs(reduce_type, base_ts, cur_tree_version, k, v, &kv_min_ts,
                                 &kv_max_ts);
            if (UNLIKELY(kv_max_ts < ts)) {
                LOG_ERROR("not keep lowbound promise").put("kv_max_ts", kv_max_ts)
                .put("ts", ts);
                return false;
            }
            assert(kv_max_ts >= kv_min_ts);
            if (kv_min_ts > ts) {
                return false;
            } else {
                int64_t old_fid = DecodeInt64FromBigEndian(k.data() + 7);
                int64_t insert_fid = fs.id();
                if (old_fid != insert_fid) {
                    // considering cpu cost, only try once to merge data to existed range
                    return false;
                } else {
                    need_merge = true;
                    tree_old_key = k;
                    tree_old_val = v;
                    return false;
                }
            }
        });
    if (UNLIKELY(!ret.ok() && !ret.IsNotFound())) {
        return ret;
    }

    if (need_merge && idempotent_add) {
        // Metrics::GetInstance()->Emit<kMetricCounter>("idempotent_add.failed.count", 1,
        //  FLAGS_metrics_sample);
        return Status::OK();
    }

    if (need_merge) {
        if (reduce_type == ReduceType::IP_REDUCE_NONE) {
            // Metrics::GetInstance()->Emit<kMetricCounter>("add.reduce_close_error.count", 1,
            //                                              FLAGS_metrics_sample);
            return Status::Internal("insert duplicate ts instance to sequence table");
        }

        const Slice insert_key(key);
        const Slice insert_val(encode_res);
        std::string val_merged_res;
        ret = TreeValMerge(tree_old_key, tree_old_val, insert_key, insert_val, &val_merged_res,
                           reduce_type, base_ts, cur_tree_version);
        if (UNLIKELY(!ret.ok())) {
            // BC_ERROR_DEFAULT_RATE_LIMIT("TreeValMerge error, ret: {}", ret.ToString());
            return ret;
        }

        ret = ordered_tree->OrSet().Set(ctx, tree_old_key.ToString(), val_merged_res);

    } else {
        ret = ordered_tree->OrSet().Set(ctx, key, encode_res);
    }
    return ret;
}

Status IPSOperator::InsertFeatureStatWithMaxTsAndMinTs(CmdContext* ctx,
                                      model::IpsModel* ordered_tree,
                                      int64_t max_ts, int64_t min_ts,
                                      const FeatureStat32& fs, TableType table_type,
                                      ReduceType reduce_type, bool idempotent_add,
                                      uint8_t cur_tree_version, int64_t base_ts) {
    if (UNLIKELY(fs.type() != table_type)) {
        std::string msg = fmt::format("data type conflict, fs.type: {}, table type: {}",
                                      fs.type(), static_cast<int>(table_type));
        LOG_ERROR(msg.c_str());
        return Status::InvalidArgument(msg);
    }
    TimeCost tc_data_format_update;

    // update tree kv format to kCurExpectedTreeVersion format
    Status ret =
        UptateTreeValToExpectedVersion(ordered_tree, cur_tree_version, base_ts, reduce_type);
    if (UNLIKELY(!ret.ok())) {
        return ret;
    }
    // update tree meta to kCurExpectedTreeVersion format && update tree meta value
    ret = UpdateTreeMetaToExpectedVersion(ctx, ordered_tree, cur_tree_version, base_ts);
    if (UNLIKELY(!ret.ok())) {
        return ret;
    }
    // Metrics::GetInstance()->Emit<kMetricTimer>("add.format_update_latency",
    //                                            tc_data_format_update.GetElapsed(),
    //                                            FLAGS_metrics_sample);

    // now tree's val && meta is kCurExpectedTreeVersion format
    cur_tree_version = kCurExpectedTreeVersion;
    std::string encode_res;
    if (table_type == PAIR) {
        std::vector<int64_t> feature_data = {fs.int_pair().v1(), fs.int_pair().v2()};
        encode_res = EncodeIpsData(reduce_type, min_ts, base_ts, feature_data);
    } else if (table_type == LIST) {
        std::vector<int64_t> feature_vec;
        for (int i = 0; i < fs.int_list().v_list_size(); i++) {
            feature_vec.emplace_back(fs.int_list().v_list(i));
        }
        encode_res = EncodeIpsData(reduce_type, min_ts, base_ts, feature_vec);
    } else {
        LOG_ERROR("Invaild table type").put("TableType", table_type);
        return Status::InvalidArgument("illegal table conf: table_type value is illegal");
    }
    const std::string key = GenerateIpsTreeKey(max_ts, fs.id());

    bool need_merge = false;
    Slice tree_old_key, tree_old_val;
    ret = ordered_tree->OrSet().Scan(
        key, [&](const std::string& k_str, const std::string& v_str) -> bool {
            const Slice k(k_str), v(v_str);
            int64_t kv_min_ts, kv_max_ts;
            GetTreeKvMinAndMaxTs(reduce_type, base_ts, cur_tree_version, k, v, &kv_min_ts,
                                 &kv_max_ts);
            if (UNLIKELY(kv_max_ts < max_ts)) {
                LOG_ERROR("not keep lowbound promise").put("kv_max_ts", kv_max_ts)
                .put("ts", max_ts);
                return false;
            }
            assert(kv_max_ts >= kv_min_ts);
            if (kv_min_ts > max_ts) {
                return false;
            } else {
                int64_t old_fid = DecodeInt64FromBigEndian(k.data() + 7);
                int64_t insert_fid = fs.id();
                if (old_fid != insert_fid) {
                    // considering cpu cost, only try once to merge data to existed range
                    return false;
                } else {
                    need_merge = true;
                    tree_old_key = k;
                    tree_old_val = v;
                    return false;
                }
            }
        });
    if (UNLIKELY(!ret.ok() && !ret.IsNotFound())) {
        return ret;
    }

    if (need_merge && idempotent_add) {
        // Metrics::GetInstance()->Emit<kMetricCounter>("idempotent_add.failed.count", 1,
        //  FLAGS_metrics_sample);
        return Status::OK();
    }

    if (need_merge) {
        if (reduce_type == ReduceType::IP_REDUCE_NONE) {
            // Metrics::GetInstance()->Emit<kMetricCounter>("add.reduce_close_error.count", 1,
            //                                              FLAGS_metrics_sample);
            return Status::Internal("insert duplicate ts instance to sequence table");
        }
        LOG_ERROR("IPS dumped data shouldn't merge").put("MaxTs", max_ts).put("MinTs", min_ts);
        const Slice insert_key(key);
        const Slice insert_val(encode_res);
        std::string val_merged_res;
        ret = TreeValMerge(tree_old_key, tree_old_val, insert_key, insert_val, &val_merged_res,
                           reduce_type, base_ts, cur_tree_version);
        if (UNLIKELY(!ret.ok())) {
            // BC_ERROR_DEFAULT_RATE_LIMIT("TreeValMerge error, ret: {}", ret.ToString());
            return ret;
        }

        ret = ordered_tree->OrSet().Set(ctx, tree_old_key.ToString(), val_merged_res);

    } else {
        ret = ordered_tree->OrSet().Set(ctx, key, encode_res);
    }
    return ret;
}

// delete tree kv when key in range [min_snap_ts_micros, max_data_ts_micros)
Status IPSOperator::TruncateTreeDataByTimeRange(CmdContext* ctx, ReduceType reduce_type,
                                                model::IpsModel* ordered_tree, int64_t base_ts,
                                                uint8_t tree_version, int64_t min_snap_ts_micros,
                                                int64_t max_data_ts_micros, uint64_t max_snap_cnt) {
    std::vector<std::pair<Slice, Slice>> res;
    Status ret = RangeGetByTime(reduce_type, ordered_tree, base_ts, tree_version,
                                min_snap_ts_micros, max_data_ts_micros, &res, true);
    if (UNLIKELY(!ret.ok())) {
        // BC_ERROR_DEFAULT_RATE_LIMIT("range in truncate_compact falied: {}", ret.ToString());
        return ret;
    }
    for (auto const& kv_pair : res) {
        ret = ordered_tree->OrSet().Del(ctx, kv_pair.first.ToString());
        if (UNLIKELY(!ret.ok())) {
            // BC_ERROR_DEFAULT_RATE_LIMIT("Del key failed:, tree_root: {}, ret: {}",
            // ordered_tree->OrSet().GetTreeRootKey(), ret.ToString());
            return ret;
        }
    }

    uint64_t tree_size = 0;
    ret = ordered_tree->OrSet().Size(&tree_size);
    if (LIKELY(ret.ok())) {
        if (tree_size > max_snap_cnt) {
            ret = TruncateTreeDataByCount(ctx, reduce_type, ordered_tree, base_ts, tree_version,
                                          max_snap_cnt, max_snap_cnt);
            if (UNLIKELY(!ret.ok())) {
                // BC_ERROR_DEFAULT_RATE_LIMIT("truncate by count in truncate by time failed: {}",
                // ret.ToString());
                return ret;
            }
        }
    } else {
        // BC_ERROR_DEFAULT_RATE_LIMIT("get tree size failed, tree_root: {}, ret: {}",
        // ordered_tree->OrSet().GetTreeRootKey(), ret.ToString());
        return ret;
    }

    return Status::OK();
}

// keep the max reserve_cnt key in tree
Status IPSOperator::TruncateTreeDataByCount(CmdContext* ctx, ReduceType reduce_type,
                                            model::IpsModel* ordered_tree, int64_t base_ts,
                                            uint8_t tree_version, uint64_t trigger_cnt,
                                            uint64_t reserve_cnt) {
    assert(trigger_cnt >= reserve_cnt);

    uint64_t tree_size = 0;
    Status ret = ordered_tree->OrSet().Size(&tree_size);
    if (UNLIKELY(!ret.ok())) {
        // BC_ERROR_DEFAULT_RATE_LIMIT("get tree size failed, ret: {}", ret.ToString());
        return ret;
    }
    if (UNLIKELY(tree_size < trigger_cnt)) {
        return Status::NoAction("");
    }

    std::vector<Slice> res;
    ret = RangeGetByCnt(reduce_type, ordered_tree, base_ts, tree_version, kMinIPSKey,
                        tree_size - reserve_cnt, &res);
    if (UNLIKELY(!ret.ok())) {
        return ret;
    }
    for (auto const& cur_key : res) {
        ret = ordered_tree->OrSet().Del(ctx, cur_key.ToString());
        if (UNLIKELY(!ret.ok())) {
            // BC_ERROR_DEFAULT_RATE_LIMIT("Del key failed, tree_root: {}, ret: {}",
            // ordered_tree->OrSet().GetTreeRootKey(), ret.ToString());
            return ret;
        }
    }

    return Status::OK();
}

Status IPSOperator::GetCurCompactTsRange(const TimeDimension& time_dimension,
                                         model::IpsModel* ordered_tree,
                                         IpsTimeRange* cur_compact_range, int64_t min_ts,
                                         int64_t max_ts, int64_t* compact_start_ts,
                                         int32_t* compact_range_index) {
    const std::pair<int64_t, int64_t> cur_range =
        time_dimension.GetCompactIntervals(*compact_range_index);
    if (UNLIKELY(cur_range.first == -1 || cur_range.second == -1)) {
        // BC_WARN_DEFAULT_RATE_LIMIT(
        //     "compact range failed, tree_root: {}, comapct_start_ts: {},compact_range_index: {}, "
        //     "compact_ramnge_size: {}, tree_min_ts: {}, tree_max_ts: {}",
        //     ordered_tree->OrSet().GetTreeRootKey(), *compact_start_ts, *compact_range_index,
        //     time_dimension.GetCompactRangeTotalSize(), min_ts, max_ts);

        CompactReachEndHandler(compact_start_ts, compact_range_index, max_ts);
        return Status::NoAction("");
    }

    int64_t cur_range_start = (*compact_start_ts) - cur_range.first;
    int64_t cur_range_end = (*compact_start_ts) - cur_range.second;
    if (min_ts >= cur_range_start) {
        CompactReachEndHandler(compact_start_ts, compact_range_index, max_ts);
        return Status::NoAction("");
    } else {
        cur_compact_range->set(cur_range_start, cur_range_end);
        if (min_ts < cur_range_end) {
            (*compact_range_index) += 1;
        } else {
            CompactReachEndHandler(compact_start_ts, compact_range_index, max_ts);
        }
        return Status::OK();
    }
}

Status IPSOperator::CompressCompactOnce(CmdContext* ctx, model::IpsModel* ordered_tree,
                                        int64_t base_ts, uint8_t tree_version,
                                        const TimeDimension& time_dimension, ReduceType reduce_type,
                                        TableType table_type, const TagList& cur_tag) {
    TimeCost tc_scan;
    uint64_t tree_size;
    Status ret = ordered_tree->OrSet().Size(&tree_size);
    if (UNLIKELY(!ret.ok())) {
        return ret;
    }

    int64_t tree_max_ts = -1;
    std::vector<std::tuple<int64_t, int64_t, Slice, Slice>> tree_kv;
    tree_kv.reserve(tree_size);
    PersistentMap<std::string, std::string>::IterateFunc iter =
        [&tree_kv, &tree_max_ts, tree_version, reduce_type, base_ts](
            const std::string& k_str, const std::string& v_str) -> bool {
        const Slice k(k_str), v(v_str);
        if (LIKELY(!IsInvalidTreeKV(k, v, tree_version))) {
            int64_t kv_min_ts, kv_max_ts;
            GetTreeKvMinAndMaxTs(reduce_type, base_ts, tree_version, k, v, &kv_min_ts, &kv_max_ts);
            assert(kv_min_ts <= kv_max_ts);
            tree_kv.emplace_back(std::make_tuple(kv_min_ts, kv_max_ts, k, v));
            if (tree_max_ts == -1) {
                tree_max_ts = kv_max_ts;
            } else if (UNLIKELY(kv_max_ts > tree_max_ts)) {
                // BC_FATAL("kv_max_ts > tree_max_ts");
            }
        }
        return true;
    };
    ret = ordered_tree->OrSet().ScanBackward("", iter);
    if (UNLIKELY(!ret.ok())) {
        return ret;
    }
    if (UNLIKELY(tree_kv.empty() || tree_max_ts == -1)) {
        return Status::InvalidArgument("InvalidArgument");
    }
    // Metrics::GetInstance()->Emit<kMetricTimer>("compact.collect_latency", tc_scan.GetElapsed(),
    // cur_tag,
    //                                            FLAGS_metrics_sample);

    std::vector<std::pair<IPSSlice, IPSSlice>> compact_res;
    std::vector<IPSSlice> garbage_key;
    compact_res.reserve(tree_size);
    garbage_key.reserve(tree_size);
    auto const time_ranges_ptr = time_dimension.GetCompactRange();
    auto const& time_ranges = *time_ranges_ptr.get();

    std::vector<std::pair<Slice, Slice>> compact_range_kv;
    auto tree_kv_iter = tree_kv.cbegin();

    int64_t start_compact_ts = tree_max_ts + 1;
    for (size_t i = 0; i < time_ranges.size(); ++i) {
        if (tree_kv_iter == tree_kv.cend()) {
            break;
        }
        int64_t range_max_ts = start_compact_ts - time_ranges[i].first;
        int64_t range_min_ts = start_compact_ts - time_ranges[i].second;
        assert(range_min_ts < range_max_ts);

        compact_range_kv.clear();
        for (; tree_kv_iter != tree_kv.cend(); ++tree_kv_iter) {
            int64_t kv_min_ts = std::get<0>(*tree_kv_iter);
            int64_t kv_max_ts = std::get<1>(*tree_kv_iter);
            const Slice& cur_key = std::get<2>(*tree_kv_iter);
            const Slice& cur_val = std::get<3>(*tree_kv_iter);

            if (UNLIKELY(kv_max_ts > range_max_ts)) {
                // BC_FATAL("kv_max_ts > range_max_ts");
            }
            if (kv_max_ts > range_min_ts) {
                if (kv_min_ts >= range_min_ts) {
                    compact_range_kv.emplace_back(std::make_pair(cur_key, cur_val));
                } else {
                    continue;
                }
            } else {
                break;
            }
        }

        if (compact_range_kv.size() <= 1) {
            continue;
        }

        ret = CompactDataMerge_(reduce_type, base_ts, tree_version, compact_range_kv, &compact_res,
                                &garbage_key);
        if (!ret.ok() && !ret.IsNoAction()) {
            return ret;
        }
    }

    return CompactResHandle(ctx, compact_res, garbage_key, ordered_tree);
}

Status IPSOperator::CompressCompact(CmdContext* ctx, model::IpsModel* ordered_tree, int64_t base_ts,
                                    uint8_t tree_version, const TimeDimension& time_dimension,
                                    ReduceType reduce_type, TableType table_type,
                                    const TagList& cur_tag,
                                    CompressCompactType compress_compact_type) {
    assert(tree_version == 0 || tree_version == 1);
    assert(tree_version == 0 || base_ts >= 0);
    if (compress_compact_type == CompressCompactType::OneTime) {
        return CompressCompactOnce(ctx, ordered_tree, base_ts, tree_version, time_dimension,
                                   reduce_type, table_type, cur_tag);
    }
    static const int64_t max_valid_ts = GetCurTsMicros() + kCompactMaxRange;
    static const int64_t min_valid_ts = GetCurTsMicros() - kCompactMaxRange;

    TimeCost tc_total;
    Status ret;
    std::vector<std::pair<Slice, Slice>> tree_kv;
    std::vector<std::pair<IPSSlice, IPSSlice>> compact_res;
    std::vector<IPSSlice> garbage_key;

    int64_t min_ts, max_ts;
    ret = GetTreeMinKeyTs(&min_ts, ordered_tree, base_ts, tree_version, reduce_type);
    if (UNLIKELY(!ret.ok())) {
        return ret;
    }

    ret = GetTreeMaxKeyTs(&max_ts, ordered_tree, tree_version);
    if (UNLIKELY(!ret.ok())) {
        return ret;
    }
    assert(min_ts >= 0);
    assert(max_ts >= min_ts);

    std::string tree_meta;
    ret = ordered_tree->OrSet().GetUserMeta(&tree_meta);
    if (UNLIKELY(!ret.ok())) {
        LOG_ERROR("Get tree meta failed").put("ErrorMsg", ret.ToString());
        return ret;
    }
    char* meta_data = &tree_meta.front();
    int64_t compact_start_ts = DecodeInt64FromBigEndian(meta_data + kTreeMetaCompactStartTsOffset);
    int32_t compact_range_index =
        DecodeInt32FromBigEndian(meta_data + kTreeMetaCompactRangeIndexOffset);

    assert(compact_start_ts >= 0);
    assert(compact_range_index >= 0);
    if (compact_start_ts < min_valid_ts || compact_start_ts > max_valid_ts) {
        CompactReachEndHandler(&compact_start_ts, &compact_range_index, max_ts);
    }
    int64_t loop_cnt = 0;
    while (true) {
        ++loop_cnt;
        tree_kv.clear();
        compact_res.clear();
        garbage_key.clear();

        IpsTimeRange cur_compact_range;
        ret = GetCurCompactTsRange(time_dimension, ordered_tree, &cur_compact_range, min_ts, max_ts,
                                   &compact_start_ts, &compact_range_index);
        if (!ret.ok()) {
            break;
        }
        int64_t comapct_start_ts = cur_compact_range.GetStartTsMicros();
        int64_t comapct_end_ts = cur_compact_range.GetEndTsMicros();
        assert(comapct_start_ts >= comapct_end_ts);
        TimeCost tc_compact;
        ret = RangeGetByTime(reduce_type, ordered_tree, base_ts, tree_version, comapct_end_ts,
                             comapct_start_ts, &tree_kv, false);
        if (UNLIKELY(!ret.ok() && !ret.IsNotFound())) {
            LOG_ERROR("Compress compact faield").put("ErrorMsg", ret.ToString());
            return Status::Internal("range get failed");
        }
        if (tree_kv.size() <= 1) {
            if (tc_total.GetElapsed() < 500 && loop_cnt < 100) {
                continue;
            } else {
                ret = Status::NoAction("");
                break;
            }
        }
        // Metrics::GetInstance()->Emit<kMetricTimer>("compact.collect_latency",
        // tc_compact.GetElapsed(),
        //                                            cur_tag, FLAGS_metrics_sample);
        // Metrics::GetInstance()->Emit<kMetricStore>("compact.collect_size", tree_kv.size(),
        // cur_tag,
        //                                            FLAGS_metrics_sample);

        tc_compact.Reset();
        ret = CompactDataMerge_(reduce_type, base_ts, tree_version, tree_kv, &compact_res,
                                &garbage_key);
        if (!ret.ok()) {
            if (ret.IsNoAction()) {
                break;
            } else {
                LOG_ERROR("compact date failed").put("ErrorMsg", ret.ToString());
                // BC_ERROR_DEFAULT_RATE_LIMIT("compact date failed, tree_root: {}, ret: {}",
                //                             ordered_tree->OrSet().GetTreeRootKey(),
                //                             ret.ToString());
                return ret;
            }
        }
        // Metrics::GetInstance()->Emit<kMetricTimer>("compact.data_merge_latency",
        // tc_compact.GetElapsed(),
        //                                            cur_tag, FLAGS_metrics_sample);
        tc_compact.Reset();
        ret = CompactResHandle(ctx, compact_res, garbage_key, ordered_tree);
        if (ret.IsNoAction()) {
            break;
        } else if (LIKELY(ret.ok())) {
            int64_t compute_latency = tc_compact.GetElapsed();
            // Metrics::GetInstance()->Emit<kMetricTimer>("compact.compute_latency",
            // compute_latency, cur_tag,
            //                                            FLAGS_metrics_sample);
        } else {
            LOG_ERROR("compact res handle failed").put("ErrorMsg", ret.ToString());
            // BC_ERROR_DEFAULT_RATE_LIMIT("compact res handle failed, tree_root: {}, ret: {}",
            //                             ordered_tree->OrSet().GetTreeRootKey(), ret.ToString());
        }
        break;
    }
    if (LIKELY(ret.ok() || ret.IsNoAction())) {
        EncodeInt64ToBigEndian(compact_start_ts, meta_data + kTreeMetaCompactStartTsOffset);
        EncodeInt32ToBigEndian(compact_range_index, meta_data + kTreeMetaCompactRangeIndexOffset);

        Status meta_ret = ordered_tree->OrSet().UpdateUserMeta(ctx, tree_meta);
        if (UNLIKELY(!meta_ret.ok())) {
            LOG_ERROR("failed to update user meta").put("ErrorMsg", ret.ToString());
            // BC_ERROR_DEFAULT_RATE_LIMIT("fail to update user meta, tree_root: {}, ret: {}",
            //                             ordered_tree->OrSet().GetTreeRootKey(),
            //                             meta_ret.ToString());
        }
    }
    return ret;
}

Status IPSOperator::GetTreeMinKeyTs(int64_t* min_ts, model::IpsModel* ordered_tree, int64_t base_ts,
                                    uint8_t tree_version, ReduceType reduce_type) {
    Status iter_ret;
    PersistentMap<std::string, std::string>::IterateFunc iter =
        [reduce_type, min_ts, &iter_ret, base_ts, tree_version](const std::string& key_str,
                                                                const std::string& v_str) -> bool {
        const Slice key(key_str), v(v_str);
        if (UNLIKELY(IsInvalidTreeKV(key, v, tree_version))) {
            iter_ret = Status::InvalidArgument("InvalidArgument");
            return false;
        }

        iter_ret = GetTreeValTs(reduce_type, key, v, base_ts, tree_version, min_ts);

        return false;
    };

    Status ret = ordered_tree->OrSet().Scan("", iter);
    if (ret.ok()) {
        return iter_ret;
    } else {
        return ret;
    }
}

Status IPSOperator::GetTreeMaxKeyTs(int64_t* max_ts, model::IpsModel* ordered_tree,
                                    uint8_t tree_version) {
    std::string max_k, max_v;
    Status ret = ordered_tree->OrSet().GetMaxItem(&max_k, &max_v);
    if (UNLIKELY(!ret.ok())) {
        LOG_ERROR("Get GetMaxItem Failed").put("ErrorMsg", ret.ToString());
        // BC_ERROR_DEFAULT_RATE_LIMIT("get GetMaxItem failed: {}", ret.ToString());
        return ret;
    }
    if (UNLIKELY(IsInvalidTreeKV(Slice(max_k), Slice(max_v), tree_version))) {
        return Status::Internal("serialize error");
    }

    *max_ts = DecodeInt56FromBigEndian(max_k.data());
    return Status::OK();
}

inline std::string IPSOperator::GetInt56BigEndianStringVal(int64_t value) {
    char data[7] = {0};
    EncodeInt56ToBigEndian(value, data);
    return std::string(data, 7);
}

inline std::string IPSOperator::GenerateIpsTreeKey(int64_t ts, int64_t fid) {
    char data[15];
    EncodeInt56ToBigEndian(ts, data);
    EncodeInt64ToBigEndian(fid, data + 7);
    return std::string(data, 15);
}

inline std::string IPSOperator::GetRangeStrKey(int64_t int_key) {
    if (int_key == kMinIPSKey) {
        // B+ 树range查询时，空key表示最key的边界：最大key和最小key
        return kEmptyString;
    }
    return GetInt56BigEndianStringVal(int_key);
}

std::string Instance2IPSKey(std::string ips_table, const ips::Instance& ins) {
    uint64_t uid = ins.uid();
    uint32_t table = ins.table();
    uint32_t slot = ins.feature_stat32_list(0).slot();
    uint32_t action_type = ins.action_type();
    std::string ips_obj_key =
        ips_table + std::string("_") + ips::IPSKeyToTreeKey(uid, slot, table, action_type);
    return ips_obj_key;
}

}  // namespace ips
}  // namespace bcache2
