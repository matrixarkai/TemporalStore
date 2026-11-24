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

Set `parallel_reads=True` on `TableClient` to overlap read-only requests
across up to eight shards. The default is serial dispatch. Writes and
mixed batches keep their dispatch order when read overlap is enabled.

A batch can override its RPC deadline with `timeout_seconds` while reusing
the table's channels and cached topology.


Set TS_SERVER_GRPC_BIND_ADDR on the datanodes. Set
TS_SERVER_GRPC_ADVERTISE_ADDR when binding a wildcard address or when clients
must use a different hostname. TableClient sends nonzero load versions to
the checked gRPC methods, which require a datanode that supports fencing.
