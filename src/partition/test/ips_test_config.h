// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

namespace bcache2 {
namespace partition {

const char default_test_table_conf[] = R"({
    "table_compress": {
      "trigger_compact_snap_count": 1,
      "compact_type": "compress",
      "open_shrink": false,
      "protected_latest_fid_ratio": 0.30,
      "store_timestamp_history": false,
      "access_quota_per_user": 100000000,
      "insert_as_timestamp_point": false,
      "abase_zone": "online",
      "abase_consul": "bytedance.abase.instance_profile_ab",
      "abase_table": "test_a",
      "table_type": "pair",
      "enable_table_ttl": true,
      "table_ttl_conf": "180d",
      "slot_ttl_conf": {
          "1": "30d",
          "2": "30d"
      },
      "time_dimension": {
        "1m": [
          "0s",
          "10m"
        ],
        "10m": [
          "10m",
          "1h"
        ],
        "1h": [
          "1h",
          "24h"
        ],
        "1d": [
          "24h",
          "30d"
        ],
        "30d": [
          "30d",
          "365d"
        ]
      },
      "slot": {
        "0": 100
      }
    },
    "table_truncate_by_count": {
      "trigger_compact_snap_count": 66666,
      "min_snap_count_after_truncate": 66660,
      "compact_type": "truncate",
      "open_shrink": false,
      "protected_latest_fid_ratio": 0.30,
      "store_timestamp_history": false,
      "access_quota_per_user": 100000000,
      "insert_as_timestamp_point": false,
      "reduce_func": "sum",
      "abase_zone": "online",
      "abase_consul": "bytedance.abase.instance_profile_ab",
      "abase_table": "test_a",
      "table_type": "pair",
      "time_dimension": {
        "1000d": [
          "0s",
          "100000000d"
        ]
      },
      "slot": {
        "140": 500,
        "141": 1000,
        "142": 2000,
        "143": 2000,
        "228": 500
      }
    },
    "table_truncate_by_count_list": {
      "trigger_compact_snap_count": 66666,
      "min_snap_count_after_truncate": 66660,
      "compact_type": "truncate",
      "open_shrink": false,
      "protected_latest_fid_ratio": 0.30,
      "store_timestamp_history": false,
      "access_quota_per_user": 100000000,
      "insert_as_timestamp_point": false,
      "reduce_func": "sum",
      "abase_zone": "online",
      "abase_consul": "bytedance.abase.instance_profile_ab",
      "abase_table": "test_a",
      "table_type": "list",
      "time_dimension": {
        "1000d": [
          "0s",
          "100000000d"
        ]
      },
      "slot": {
        "140": 500,
        "141": 1000,
        "142": 2000,
        "143": 2000,
        "228": 500
      }
    },
    "test_table_truncate_time": {
      "trigger_compact_snap_count": 1,
      "compact_type": "truncate",
      "truncate_type": "relative",
      "truncate_range": "2d",
      "open_shrink": false,
      "protected_latest_fid_ratio": 0.30,
      "store_timestamp_history": false,
      "access_quota_per_user": 100000000,
      "insert_as_timestamp_point": true,
      "reduce_func": "sum",
      "abase_zone": "online",
      "abase_consul": "bytedance.abase.instance_profile_ab",
      "abase_table": "test_a",
      "table_type": "pair",
      "time_dimension": {
        "1000d": [
          "0s",
          "100000000d"
        ]
      },
      "slot": {
        "140": 500,
        "141": 1000,
        "142": 2000,
        "143": 2000,
        "228": 500
      }
    },
    "table_ttl": {
      "trigger_compact_snap_count": 1000000,
      "compact_type": "truncate",
      "truncate_type": "relative",
      "truncate_range": "2000d",
      "reduce_func": "none",
      "open_shrink": false,
      "protected_latest_fid_ratio": 0.30,
      "store_timestamp_history": false,
      "access_quota_per_user": 100000000,
      "insert_as_timestamp_point": false,
      "abase_zone": "online",
      "abase_consul": "bytedance.abase.instance_profile_ab",
      "abase_table": "test_a",
      "table_type": "pair",
      "enable_table_ttl": true,
      "table_ttl_conf": "1h",
      "slot_ttl_conf": {
        "0": "5h"
      },
      "time_dimension": {
        "1m": [
          "0s",
          "10m"
        ],
        "10m": [
          "10m",
          "1h"
        ],
        "1h": [
          "1h",
          "24h"
        ],
        "1d": [
          "24h",
          "30d"
        ],
        "30d": [
          "30d",
          "365d"
        ]
      },
      "slot": {
        "0": 500
      }
    }
  })";
const char default_test_table_conf2[] = R"({"day_level_action": {
        "trigger_compact_snap_count": 35,
        "compact_type": "compress",
        "compress_compact_type": "one_time",
        "open_shrink": true,
        "protected_latest_fid_ratio": 0.30,
        "store_timestamp_history": false,
        "access_quota_per_user": 40000000,
        "insert_as_timestamp_point": false,
        "abase_zone": "online",
        "abase_cluster": "toutiao.abase.bcache_data_ad_mva",
        "abase_consul": "toutiao.abase.bcache_data_ad_mva",
        "abase_table": "day_level_action_ips2",
        "abase_allow_write_data": true,
        "abase_query_time_out_mills": 50,
        "abase_retry_max_count": 0,
        "abase_retry_wait_ms": 1,
        "delete_sequence": "vx",
        "enable_table_ttl": true,
        "table_ttl_conf": "180d",
        "slot_ttl_conf": {
          "23" : "90d",
          "24" : "90d",
          "25" : "90d",
          "26" : "90d",
          "27" : "90d",
          "28" : "90d",
          "29" : "90d",
          "293" : "31d",
          "274" : "8d",
          "275" : "8d",
          "276" : "8d",
          "277" : "8d",
          "278" : "8d",
          "279" : "8d",
          "280" : "8d",
          "281" : "8d",
          "282" : "8d",
          "283" : "8d",
          "284" : "8d",
          "285" : "8d",
          "286" : "8d",
          "287" : "8d",
          "288" : "8d",
          "295" : "8d",
          "296" : "8d",
          "302" : "91d",
          "303" : "8d",
          "101" : "1s",
          "112" : "30d",
          "113" : "30d",
          "114" : "30d",
          "124" : "30d",
          "125" : "30d",
          "126" : "30d",
          "127" : "30d",
          "128" : "30d",
          "129" : "30d",
          "259" : "31d",
          "323" : "31d",
          "324" : "31d",
          "325" : "31d",
          "326" : "31d",
          "327" : "31d",
          "328" : "31d",
          "294" : "30d",
          "270" : "31d",
          "321" : "30d",
          "378" : "35d"
        },
        "time_dimension": {
            "10m": [
                "0s",
                "1h"
            ],
            "1h": [
                "1h",
                "6h"
            ],
            "3h": [
                "6h",
                "24h"
            ],
            "6h": [
                "1d",
                "3d"
            ],
            "3d": [
                "3d",
                "60d"
            ],
            "30d": [
                "60d",
                "365d"
            ]
        },
        "slot": {
            "0": 500,
            "23": 500,
            "24": 500,
            "25": 500,
            "26": 500,
            "27": 500,
            "28": 1000,
            "29": 500,
            "43": 500,
            "51": 500,
            "56": 500,
            "57": 500,
            "58": 500,
            "59": 500,
            "60": 500,
            "61": 500,
            "62": 500,
            "101": 500,
            "110": 100,
            "112": 500,
            "113": 500,
            "114": 500,
            "124": 500,
            "125": 500,
            "126": 500,
            "127": 500,
            "128": 500,
            "129": 500,
            "140": 500,
            "141": 1000,
            "142": 500,
            "143": 1000,
            "144": 500,
            "145": 500,
            "228": 500,
            "694": 500,
            "695": 500,
            "696": 500,
            "697": 500,
            "698": 500,
            "699": 500,
            "303": 300,
            "323": 800,
            "259": 500,
            "324": 500,
            "325": 500,
            "326": 500,
            "327": 500,
            "328": 500
        }
    },
    "min_level_action": {
        "trigger_compact_snap_count": 35,
        "compact_type": "compress",
        "compress_compact_type": "one_time",
        "open_shrink": true,
        "protected_latest_fid_ratio": 0.30,
        "store_timestamp_history": false,
        "access_quota_per_user": 40000000,
        "insert_as_timestamp_point": false,
        "abase_zone": "online",
        "abase_cluster": "toutiao.abase.bcache_data_ad_mva",
        "abase_consul": "toutiao.abase.bcache_data_ad_mva",
        "abase_table": "min_level_action_ips2",
        "abase_allow_write_data": true,
        "abase_query_time_out_mills": 50,
        "abase_retry_max_count": 0,
        "abase_retry_wait_ms": 1,
        "delete_sequence": "vx",
        "enable_table_ttl": true,
        "table_ttl_conf": "180d",
        "slot_ttl_conf": {
          "142": "30d",
          "143": "30d",
          "144": "30d",
          "145": "30d",
          "172": "30d",
          "173": "30d",
          "174": "30d",
          "175": "30d",
          "177": "30d",
          "178": "30d",
          "179": "30d",
          "180": "30d",
          "262" : "30d",
          "263" : "30d",
          "370" : "31d",
          "670" : "15d",
          "671" : "15d",
          "672" : "15d",
          "216" : "8d",
          "373" : "15d",
          "374" : "15d",
          "375" : "15d",
          "376" : "15d",
          "678" : "15d",
          "679" : "15d",
          "680" : "15d",
          "681" : "15d",
          "682" : "15d",
          "683" : "15d",
          "684" : "15d",
          "51" : "15d",
          "56" : "15d",
          "57" : "15d",
          "58" : "15d",
          "59" : "15d",
          "289" : "30d",
          "360" : "30d",
          "211" : "30d",
          "212" : "30d",
          "213" : "30d",
          "214" : "30d",
          "215" : "30d",
          "331" : "90d",
          "332" : "90d",
          "362" : "90d",
          "363" : "90d",
          "364" : "90d",
          "365" : "90d",
          "366" : "90d",
          "367" : "90d",
          "368" : "90d",
          "369" : "90d",
          "329" : "3d",
          "330" : "3d",
          "294" : "30d",
          "267" : "90d",
          "268" : "90d",
          "269" : "90d",
          "319" : "30d",
          "320" : "30d",
          "668": "30d",
          "669": "30d",
          "110" : "1s",
          "694": "1s"
        },
        "time_dimension": {
            "1s": [
                "0s",
                "10s"
            ],
            "10s": [
                "10s",
                "1m"
            ],
            "1m": [
                "1m",
                "10m"
            ],
            "10m": [
                "10m",
                "1h"
            ],
            "1h": [
                "1h",
                "24h"
            ],
            "1d": [
                "24h",
                "6d"
            ],
            "3d": [
                "6d",
                "30d"
            ],
            "30d": [
                "30d",
                "365d"
            ]
        },
        "slot": {
            "0": 500,
            "23": 1000,
            "24": 1000,
            "25": 1000,
            "26": 1000,
            "27": 1000,
            "28": 1000,
            "29": 1000,
            "43": 500,
            "51": 500,
            "56": 500,
            "57": 500,
            "58": 500,
            "59": 500,
            "60": 500,
            "61": 500,
            "62": 500,
            "101": 500,
            "110": 100,
            "112": 500,
            "113": 500,
            "114": 500,
            "124": 500,
            "125": 500,
            "126": 500,
            "127": 500,
            "128": 500,
            "129": 500,
            "140": 500,
            "141": 1000,
            "142": 500,
            "143": 1000,
            "144": 500,
            "145": 500,
            "228": 500,
            "668": 500,
            "669": 500,
            "177": 500,
            "178": 500,
            "179": 500,
            "180": 500,
            "679": 500,
            "362": 500,
            "363": 500,
            "364": 500,
            "365": 500,
            "366": 500,
            "367": 500,
            "368": 500,
            "369": 500,
            "351": 500,
            "352": 500,
            "353": 500,
            "354": 500,
            "355": 500,
            "356": 500,
            "357": 500,
            "358": 500,
            "359": 500,
            "678": 500,
            "672": 500,
            "670": 500,
            "331": 500,
            "332": 500,
            "329": 800,
            "330": 800,
            "694": 500,
            "695": 500,
            "696": 500,
            "697": 500,
            "698": 500,
            "699": 500,
            "682": 500,
            "684": 500,
            "680": 500,
            "681": 500,
            "683": 500,
            "371": 500,
            "372": 500,
            "211": 500
        }
    },
    "sequence_action": {
        "trigger_compact_snap_count": 35,
        "min_snap_count_after_truncate": 30,
        "compact_type": "truncate",
        "open_shrink": false,
        "protected_latest_fid_ratio": 0.30,
        "store_timestamp_history": false,
        "access_quota_per_user": 40000000,
        "insert_as_timestamp_point": true,
        "abase_zone": "online",
        "abase_cluster": "toutiao.abase.bcache_data_ad_mva",
        "abase_consul": "toutiao.abase.bcache_data_ad_mva",
        "abase_table": "sequence_action_ips2",
        "abase_allow_write_data": true,
        "abase_query_time_out_mills": 50,
        "abase_retry_max_count": 0,
        "abase_retry_wait_ms": 1,
        "delete_sequence": "vx",
        "table_type": "list",
        "enable_table_ttl": true,
        "table_ttl_conf": "180d",
        "time_dimension": {
            "1000d": [
                "0s",
                "365d"
            ]
        },
        "slot": {
            "0": 1500,
            "23": 2000,
            "24": 2000,
            "25": 2000,
            "26": 2000,
            "27": 2000,
            "28": 2000,
            "29": 2000,
            "51": 2000,
            "56": 500,
            "57": 500,
            "58": 2000,
            "59": 2000,
            "60": 500,
            "61": 500,
            "62": 500,
            "101": 1500,
            "140": 500,
            "141": 2000,
            "143": 1500,
            "144": 500,
            "228": 500
        }
    },
    "game_sequence_action": {
        "trigger_compact_snap_count": 35,
        "min_snap_count_after_truncate": 30,
        "compact_type": "truncate",
        "reduce_func": "none",
        "open_shrink": false,
        "truncate_type": "absolute",
        "truncate_range": "100d",
        "truncate_by_time_max_snap_count": 1500,
        "protected_latest_fid_ratio": 0.30,
        "store_timestamp_history": false,
        "access_quota_per_user": 40000000,
        "insert_as_timestamp_point": true,
        "abase_zone": "online",
        "abase_cluster": "toutiao.abase.bcache_data_ad_mva",
        "abase_consul": "toutiao.abase.bcache_data_ad_mva",
        "abase_table": "game_sequence_action",
        "abase_allow_write_data": true,
        "abase_query_time_out_mills": 50,
        "abase_retry_max_count": 0,
        "abase_retry_wait_ms": 1,
        "delete_sequence": "vx",
        "table_type": "list",
        "enable_table_ttl": true,
        "table_ttl_conf": "180d",
        "time_dimension": {
            "1000d": [
                "0s",
                "365d"
            ]
        },
        "slot": {
            "0": 1500,
            "23": 2000,
            "24": 2000,
            "25": 2000,
            "26": 2000,
            "27": 2000,
            "28": 2000,
            "29": 2000,
            "51": 2000,
            "56": 500,
            "57": 500,
            "58": 2000,
            "59": 2000,
            "60": 500,
            "61": 500,
            "62": 500,
            "101": 1500,
            "140": 500,
            "141": 2000,
            "143": 1500,
            "144": 500,
            "228": 500
        }
    }
})";

}  // namespace partition
}  // namespace bcache2
