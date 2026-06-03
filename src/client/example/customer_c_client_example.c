#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "client/temporalstore_c_client.h"

static int check(int code, char** error_message, const char* op) {
    if (code == 0) {
        return 1;
    }
    fprintf(stderr, "FAIL %s: %s\n", op, *error_message ? *error_message : "unknown error");
    temporalstore_free_string(*error_message);
    *error_message = NULL;
    return 0;
}

int main(int argc, char** argv) {
    if (argc != 5) {
        printf("usage: %s <metaserver_host:port> <idc> <namespace> <table>\n", argv[0]);
        return 2;
    }

    temporalstore_options_t options;
    temporalstore_options_init(&options);
    options.metaserver_addr = argv[1];
    options.idc = argv[2];
    options.namespace_name = argv[3];
    options.table_name = argv[4];
    options.psm = "customer.c.client.example";

    char* error_message = NULL;
    temporalstore_client_t* client = NULL;
    if (!check(temporalstore_connect(&options, &client, &error_message), &error_message,
               "connect")) {
        return 1;
    }

    char prefix[128];
    snprintf(prefix, sizeof(prefix), "customer_c_sdk_%lld", (long long)time(NULL));

    char key[256];
    snprintf(key, sizeof(key), "%s:profile", prefix);
    if (!check(temporalstore_put_string(client, key, "{\"uid\":42,\"tier\":\"gold\"}",
                                        &error_message),
               &error_message, "put string")) {
        temporalstore_close(client, NULL);
        return 1;
    }

    char* profile = NULL;
    if (!check(temporalstore_get_string(client, key, &profile, &error_message), &error_message,
               "get string")) {
        temporalstore_close(client, NULL);
        return 1;
    }

    snprintf(key, sizeof(key), "%s:features", prefix);
    if (!check(temporalstore_hset(client, key, "ctr_7d", "0.042", &error_message),
               &error_message, "hset")) {
        temporalstore_free_string(profile);
        temporalstore_close(client, NULL);
        return 1;
    }

    char* ctr = NULL;
    if (!check(temporalstore_hget(client, key, "ctr_7d", &ctr, &error_message),
               &error_message, "hget")) {
        temporalstore_free_string(profile);
        temporalstore_close(client, NULL);
        return 1;
    }

    snprintf(key, sizeof(key), "%s:campaigns", prefix);
    if (!check(temporalstore_sadd(client, key, "campaign_100", &error_message), &error_message,
               "sadd")) {
        temporalstore_free_string(profile);
        temporalstore_free_string(ctr);
        temporalstore_close(client, NULL);
        return 1;
    }
    temporalstore_string_array_t campaigns = {0, NULL};
    if (!check(temporalstore_smembers(client, key, &campaigns, &error_message), &error_message,
               "smembers")) {
        temporalstore_free_string(profile);
        temporalstore_free_string(ctr);
        temporalstore_close(client, NULL);
        return 1;
    }

    snprintf(key, sizeof(key), "%s:sequence", prefix);
    temporalstore_sequence_feature_row_t rows[2] = {
        {1700000000000ULL, 900, 1, 31, 7000},
        {1700000001000ULL, 901, 3, 120, 7001},
    };
    if (!check(temporalstore_add_sequence_feature_rows(client, key, rows, 2, &error_message),
               &error_message, "sequence add")) {
        temporalstore_string_array_free(&campaigns);
        temporalstore_free_string(profile);
        temporalstore_free_string(ctr);
        temporalstore_close(client, NULL);
        return 1;
    }
    temporalstore_feature_filter_t sequence_filters[1] = {
        {"action_type", TEMPORALSTORE_FEATURE_FILTER_EQUAL, 3},
    };
    temporalstore_sequence_feature_row_array_t queried_rows = {0, NULL};
    if (!check(temporalstore_query_sequence_feature_rows(client, key, 1700000000000ULL,
                                                         1700000002000ULL, 10, sequence_filters, 1,
                                                         &queried_rows, &error_message),
               &error_message, "sequence query")) {
        temporalstore_string_array_free(&campaigns);
        temporalstore_free_string(profile);
        temporalstore_free_string(ctr);
        temporalstore_close(client, NULL);
        return 1;
    }

    temporalstore_ips_feature_stat_t ips_feature = {456, 23, 1, 0, 12, 34};
    int64_t uid = 10000000 + (int64_t)(time(NULL) % 1000000);
    int64_t ts_us = (int64_t)time(NULL) * 1000LL * 1000LL;
    if (!check(temporalstore_add_ips_instance(client, "table_compress", uid, ts_us, 0, 0,
                                              &ips_feature, 1, &error_message),
               &error_message, "ips add")) {
        temporalstore_sequence_feature_row_array_free(&queried_rows);
        temporalstore_string_array_free(&campaigns);
        temporalstore_free_string(profile);
        temporalstore_free_string(ctr);
        temporalstore_close(client, NULL);
        return 1;
    }
    temporalstore_ips_feature_array_t ips_features = {0, NULL};
    if (!check(temporalstore_query_ips_last_instances(client, "table_compress", uid, 0, 0, 23,
                                                      20, 10, &ips_features, &error_message),
               &error_message, "ips query")) {
        temporalstore_sequence_feature_row_array_free(&queried_rows);
        temporalstore_string_array_free(&campaigns);
        temporalstore_free_string(profile);
        temporalstore_free_string(ctr);
        temporalstore_close(client, NULL);
        return 1;
    }

    snprintf(key, sizeof(key), "%s:risk", prefix);
    for (int i = 0; i < 3; ++i) {
        char uuid[256];
        snprintf(uuid, sizeof(uuid), "%s:risk_uuid:%d", prefix, i);
        if (!check(temporalstore_risk_increment(client, key, 1, 24 * 3600,
                                                TEMPORALSTORE_RISK_ONE_MINUTE, uuid, 0,
                                                &error_message),
                   &error_message, "risk increment")) {
            temporalstore_ips_feature_array_free(&ips_features);
            temporalstore_sequence_feature_row_array_free(&queried_rows);
            temporalstore_string_array_free(&campaigns);
            temporalstore_free_string(profile);
            temporalstore_free_string(ctr);
            temporalstore_close(client, NULL);
            return 1;
        }
    }

    int64_t risk_count = 0;
    if (!check(temporalstore_risk_count(client, key, TEMPORALSTORE_RISK_ONE_MINUTE, -1, 0,
                                        TEMPORALSTORE_WINDOW_HOUR, &risk_count, &error_message),
               &error_message, "risk count")) {
        temporalstore_ips_feature_array_free(&ips_features);
        temporalstore_sequence_feature_row_array_free(&queried_rows);
        temporalstore_string_array_free(&campaigns);
        temporalstore_free_string(profile);
        temporalstore_free_string(ctr);
        temporalstore_close(client, NULL);
        return 1;
    }

    printf("profile=%s\n", profile);
    printf("ctr_7d=%s\n", ctr);
    printf("campaigns=%zu\n", campaigns.count);
    printf("sequence_rows=%zu\n", queried_rows.count);
    printf("ips_features=%zu\n", ips_features.count);
    printf("risk_count=%lld\n", (long long)risk_count);
    printf("PASS customer C client example\n");

    temporalstore_ips_feature_array_free(&ips_features);
    temporalstore_sequence_feature_row_array_free(&queried_rows);
    temporalstore_string_array_free(&campaigns);
    temporalstore_free_string(profile);
    temporalstore_free_string(ctr);
    return check(temporalstore_close(client, &error_message), &error_message, "close") ? 0 : 1;
}
