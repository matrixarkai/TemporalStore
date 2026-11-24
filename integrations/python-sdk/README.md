# TemporalStore Python SDK

Build and install the SDK from the repository with uv pip install
./integrations/python-sdk. The build generates the Python gRPC bindings from
the same .proto file used by the Rust SDK.

CommandClient connects to a configured gRPC endpoint and reuses a channel
across calls. Use temporalstore_sdk.commands to create typed commands and read
the status on each result. Use the client as a context manager or call close
to release its channel. The default deadline is ten seconds per RPC.

MetadataClient reads namespace and table topology from the metaserver. Pass
its credential provider a callable that returns the configured metaserver
token when authentication is enabled. TableClient uses the topology to
resolve a command's shard and the shard's advertised gRPC primary. It
preserves response order for batches that span several shards.

Set TS_SERVER_GRPC_BIND_ADDR on the datanodes. Set
TS_SERVER_GRPC_ADVERTISE_ADDR when binding a wildcard address or when clients
must use a different hostname. TableClient requires the current gRPC command
service and a table without a nonzero load version. Checked gRPC execution is
required before serving fenced tables.
