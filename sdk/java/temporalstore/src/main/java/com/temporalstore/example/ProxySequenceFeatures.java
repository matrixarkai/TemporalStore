package com.temporalstore.example;

import com.temporalstore.TemporalStoreClient;
import com.temporalstore.TemporalStoreProxyClient;
import java.util.Arrays;
import java.util.List;

public final class ProxySequenceFeatures {
    private ProxySequenceFeatures() {}

    public static void main(String[] args) {
        TemporalStoreProxyClient.Options options = new TemporalStoreProxyClient.Options();
        options.endpoint = args.length > 0 ? args[0] : "http://127.0.0.1:8080";
        options.namespaceName = args.length > 1 ? args[1] : "sdk_ns";
        options.tableName = args.length > 2 ? args[2] : "sdk_table";

        try (TemporalStoreProxyClient client = new TemporalStoreProxyClient(options)) {
            client.putString("java:proxy:user:42", "{\"tier\":\"gold\"}");
            System.out.println("profile=" + client.getString("java:proxy:user:42"));

            String key = "java:proxy:user:42:sequence";
            client.addSequenceFeatureRows(
                    key,
                    Arrays.asList(
                            new TemporalStoreClient.SequenceFeatureRow(
                                    1700000000000L, 900L, 1, 31, 7000L),
                            new TemporalStoreClient.SequenceFeatureRow(
                                    1700000001000L, 901L, 3, 120, 7001L)));

            List<TemporalStoreClient.SequenceFeatureRow> rows =
                    client.querySequenceFeatureRows(
                            key,
                            1700000000000L,
                            1700000002000L,
                            10,
                            Arrays.asList(
                                    new TemporalStoreClient.FeatureFilter(
                                            "action_type",
                                            TemporalStoreClient.FeatureFilterOp.EQUAL,
                                            3)));
            System.out.println("rows=" + rows.size());
        }
    }
}
