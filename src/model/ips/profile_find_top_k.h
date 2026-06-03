// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

// #include "bcache/common/likely.h"
// #include "bcache/common/logger.h"
// #include "bcache/server/ips_interface/ips_define.h"
// #include "bcache/server/ips_interface/var_encode.h"
// #include "bcache/server/operator/ips_operator.h"

#include "model/ips/ips_define.h"
#include "model/ips/var_encode.h"

namespace bcache2 {
namespace ips {
const unsigned int kMaxVarint64Length = 10;
const unsigned int kMaxVarint32Length = 5;

struct ShrinkSortContext {
    int64_t sort_by_vx;
    uint8_t tree_version;
    TableType table_type;

    const std::vector<uint64_t>* feature_index = nullptr;
    const std::vector<uint64_t>* feature_weight = nullptr;
};

// tree_key: 7byte max ts + 8 byte fid
// tree_val:
// version(1 byte) + min_ts(8 byte) + vec_size(4 byte) + feature_size * 8;
//         0         1~8               9~12              13~20(1th)  21~28(2th)
//         ...
struct ShrinkGetSortWeight {
    virtual ~ShrinkGetSortWeight() {}
    virtual int64_t GetCurrentFeatureSortWeight(const Slice& tree_key,
                                                const Slice& tree_val) const = 0;
    void SetSortContext(const ShrinkSortContext& context) { context_ = &context; }

 protected:
    const ShrinkSortContext* context_;
};

// eg: for v1: index = 0, num: 1; for v2: index = 1, num: 1;
// for v1 && v2: index = 0, num: 2; for list[i]: index = i ,num: 1,
// get the feature list: feature[index].....feature[index + num -1]
static inline std::vector<int64_t> GetVersionOneTreeFeatureVal(const Slice& tree_val, size_t index,
                                                               size_t num) {
    assert(num != 0);
    const char* data = tree_val.data();
    const char* end_ptr = data + tree_val.size();
    // size_t decode_index = 0;

    std::vector<int64_t> res;
    res.reserve(num);
    // decode delta ts
    int32_t delta_ts;
    size_t encode_size = DecodeVarsignedint32(data, kMaxVarint32Length, &delta_ts);
    data += encode_size;
    while (data < end_ptr && num > 0) {
        int64_t cur_feature_data;
        encode_size = DecodeVarsignedint64(data, kMaxVarint64Length, &cur_feature_data);
        assert(encode_size != 0);
        if (index == 0 || !res.empty()) {
            res.push_back(cur_feature_data);
            --num;
        }
        --index;
        data += encode_size;
    }
    return res;
}

struct ShrinkGetSortWeightV1 : public ShrinkGetSortWeight {
    int64_t GetCurrentFeatureSortWeight(const Slice& tree_key, const Slice& tree_val) const {
        uint8_t tree_version = context_->tree_version;
        if (tree_version == 0) {
            const char* val_data = tree_val.data();
            int32_t feature_size = DecodeInt32FromBigEndian(val_data + 9);
            if (UNLIKELY(feature_size == 0)) {
                return 0;
            } else if (UNLIKELY(feature_size != 1 && feature_size != 2)) {
                // Metrics::GetInstance()->Emit<kMetricCounter>("invalid.tree_kv.cnt", 1, 1);
                return -1;
            }

            return DecodeInt64FromBigEndian(val_data + 13);
        } else if (tree_version == 1) {
            std::vector<int64_t> feature;
            feature = GetVersionOneTreeFeatureVal(tree_val, 0, 1);
            if (feature.empty()) {
                return 0;
            } else {
                assert(feature.size() == 1);
                return feature[0];
            }
        }
        // BC_FATAL("invalid tree version: {}", tree_version);
        return -1;
    }
};

struct ShrinkGetSortWeightV2 : public ShrinkGetSortWeight {
    int64_t GetCurrentFeatureSortWeight(const Slice& tree_key, const Slice& tree_val) const {
        uint8_t tree_version = context_->tree_version;
        if (tree_version == 0) {
            const char* val_data = tree_val.data();
            int32_t feature_size = DecodeInt32FromBigEndian(val_data + 9);
            if (feature_size == 0 || feature_size == 1) {
                return 0;
            } else if (UNLIKELY(feature_size != 2)) {
                // Metrics::GetInstance()->Emit<kMetricCounter>("invalid.tree_kv.cnt", 1, 1);
                return -1;
            }

            return DecodeInt64FromBigEndian(val_data + 21);
        } else if (tree_version == 1) {
            std::vector<int64_t> feature;
            feature = GetVersionOneTreeFeatureVal(tree_val, 1, 1);
            if (feature.empty()) {
                return 0;
            } else {
                assert(feature.size() == 1);
                return feature[0];
            }
        }
        // BC_FATAL("invalid tree version: {}", tree_version);
        return -1;
    }
};

struct ShrinkGetSortWeightVX : public ShrinkGetSortWeight {
    int64_t sort_by_vx;
    int64_t GetCurrentFeatureSortWeight(const Slice& tree_key, const Slice& tree_val) const {
        uint8_t tree_version = context_->tree_version;
        if (tree_version == 0) {
            const char* val_data = tree_val.data();
            int32_t feature_size = DecodeInt32FromBigEndian(val_data + 9);
            if (context_->sort_by_vx >= feature_size) {
                return 0;
            }

            return DecodeInt64FromBigEndian(val_data + 13 + context_->sort_by_vx * 8);
        } else if (tree_version == 1) {
            std::vector<int64_t> feature;
            feature = GetVersionOneTreeFeatureVal(tree_val, context_->sort_by_vx, 1);
            if (feature.empty()) {
                return 0;
            } else {
                assert(feature.size() == 1);
                return feature[0];
            }
        }
        // BC_FATAL("invalid tree version: {}", tree_version);
        return -1;
    }
};

struct ShrinkGetSortWeightRatio : public ShrinkGetSortWeight {
    int64_t GetCurrentFeatureSortWeight(const Slice& tree_key, const Slice& tree_val) const {
        uint8_t tree_version = context_->tree_version;
        if (tree_version == 0) {
            const char* val_data = tree_val.data();
            int32_t feature_size = DecodeInt32FromBigEndian(val_data + 9);
            if (UNLIKELY(feature_size > 2)) {
                // Metrics::GetInstance()->Emit<kMetricCounter>("invalid.tree_kv.cnt", 1, 1);
                return -1;
            }

            if (feature_size == 0 || feature_size == 1) {
                // BC_WARN_DEFAULT_RATE_LIMIT(
                //     "shrink sort by ratio, expected feature size equal 2, current feature size:
                //     {}", feature_size);
                return 0;
            }
            int64_t v2 = DecodeInt64FromBigEndian(val_data + 21);
            if (UNLIKELY(v2 == 0)) {
                // BC_WARN_DEFAULT_RATE_LIMIT("shrink sort by ratio, expected v2 not zero, current
                // v2: {}",
                //    v2);
                return 0;
            }
            int64_t v1 = DecodeInt64FromBigEndian(val_data + 13);
            return v1 / v2;
        } else if (tree_version == 1) {
            std::vector<int64_t> feature;
            feature = GetVersionOneTreeFeatureVal(tree_val, 0, 2);
            if (feature.size() < 2) {
                return 0;
            } else {
                assert(feature.size() == 2);
                if (feature[1] == 0) {
                    return 0;
                } else {
                    return feature[0] / feature[1];
                }
            }
        }
        // BC_FATAL("invalid tree version: {}", tree_version);
        return -1;
    }
};

struct ShrinkGetSortWeightID : public ShrinkGetSortWeight {
    int64_t GetCurrentFeatureSortWeight(const Slice& tree_key, const Slice& tree_val) const {
        return DecodeInt64FromBigEndian(tree_key.data() + 7);
    }
};

struct ShrinkGetSortWeightCustomizeWeight : public ShrinkGetSortWeight {
    int64_t GetCurrentFeatureSortWeight(const Slice& tree_key, const Slice& tree_val) const {
        const std::vector<uint64_t>* feature_index = context_->feature_index;
        const std::vector<uint64_t>* feature_weight = context_->feature_weight;
        if (feature_index == nullptr || feature_weight == nullptr || feature_index->empty() ||
            feature_weight->empty()) {
            ShrinkGetSortWeightVX cmp;
            cmp.SetSortContext(*context_);
            return cmp.GetCurrentFeatureSortWeight(tree_key, tree_val);
        }
        uint8_t tree_version = context_->tree_version;
        if (tree_version == 0) {
            const char* val_data = tree_val.data();
            uint64_t feature_size = DecodeInt32FromBigEndian(val_data + 9);
            int64_t res = 0;
            for (size_t i = 0; i < feature_index->size(); ++i) {
                uint64_t index = feature_index->at(i);
                uint64_t weight = feature_weight->at(i);
                if (UNLIKELY(index >= feature_size)) {
                    // BC_WARN_DEFAULT_RATE_LIMIT(
                    //     "shrink by custom, index out of range, index: {}, feature size: {}",
                    //     index, feature_size);
                    continue;
                } else {
                    int64_t cur_feature = DecodeInt64FromBigEndian(val_data + 13 + index * 8);
                    int64_t tmp_res = res;
                    res += (cur_feature * weight);
                    if (UNLIKELY(res < tmp_res)) {
                        return INT64_MAX;
                    }
                }
            }
            return res;
        } else if (tree_version == 1) {
            // 线上还没有使用
            // BC_FATAL("not support");
        }
        // 线上还没有使用
        // BC_FATAL("invalid tree version: {}", tree_version);
        return -1;
    }
};

struct ShrinkGetSortWeightWilsonWeight : public ShrinkGetSortWeight {
    int64_t GetCurrentFeatureSortWeight(const Slice& tree_key, const Slice& tree_val) const {
        if (context_->table_type == TableType::PAIR) {
            uint8_t tree_version = context_->tree_version;
            if (tree_version == 0) {
                const char* val_data = tree_val.data();
                int32_t feature_size = DecodeInt32FromBigEndian(val_data + 9);
                if (UNLIKELY(feature_size > 2)) {
                    // Metrics::GetInstance()->Emit<kMetricCounter>("invalid.tree_kv.cnt", 1, 1);
                    return -1;
                }
                int64_t v1, v2;
                if (feature_size == 0) {
                    v1 = 0;
                    v2 = 0;
                } else if (feature_size == 1) {
                    v1 = DecodeInt64FromBigEndian(val_data + 13);
                    v2 = 0;
                } else {
                    assert(feature_size == 2);
                    v1 = DecodeInt64FromBigEndian(val_data + 13);
                    v2 = DecodeInt64FromBigEndian(val_data + 21);
                }

                return GetWilsonScore(v1, v2);
            } else if (tree_version == 1) {
                std::vector<int64_t> feature;
                feature = GetVersionOneTreeFeatureVal(tree_val, 0, 2);
                if (feature.empty()) {
                    return 0;
                }
                if (UNLIKELY(feature.size() > 2)) {
                    // Metrics::GetInstance()->Emit<kMetricCounter>("invalid.tree_kv.cnt", 1, 1);
                    return -1;
                }
                int64_t v1 = feature[0];
                int64_t v2 = feature.size() < 2 ? 0 : feature[1];
                return GetWilsonScore(v1, v2);
            }
        }

        // BC_FATAL("invalid args, wilson only support in list table");
        return -1;
    }
};

struct ShrinkComparator {
    inline bool operator()(const std::pair<Slice, int64_t>& v1,
                           const std::pair<Slice, int64_t>& v2) const {
        return v1.second > v2.second;  // reverse order
    }
};

template <typename Iterator>
void FindShrinkTopkMax(Iterator first, size_t topk, Iterator last) {
    std::nth_element(first, first + topk, last, ShrinkComparator());
}

}  // namespace ips
}  // namespace bcache2
