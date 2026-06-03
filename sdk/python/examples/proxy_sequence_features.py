from temporalstore import (
    FeatureFilter,
    FeatureFilterOp,
    ProxyClient,
    ProxyOptions,
    SequenceFeatureRow,
)


options = ProxyOptions(
    endpoint="http://127.0.0.1:8080",
    namespace_name="sdk_ns",
    table_name="sdk_table",
)

client = ProxyClient(options)
client.put_string("python:proxy:user:42", '{"tier":"gold"}')
print("profile=", client.get_string("python:proxy:user:42"))

key = "python:proxy:user:42:sequence"
client.add_sequence_feature_rows(
    key,
    [
        SequenceFeatureRow(1700000000000, 900, 1, 31, 7000),
        SequenceFeatureRow(1700000001000, 901, 3, 120, 7001),
    ],
)
rows = client.query_sequence_feature_rows(
    key,
    1700000000000,
    1700000002000,
    10,
    [FeatureFilter("action_type", FeatureFilterOp.EQUAL, 3)],
)
print(rows)
