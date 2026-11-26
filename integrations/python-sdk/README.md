# TemporalStore Python SDK

Build and install the SDK from the repository with uv pip install
./integrations/python-sdk. The build generates the Python gRPC bindings from
the same .proto file used by the Rust SDK.

CommandClient connects to a configured gRPC endpoint and reuses a channel
across calls. Use temporalstore_sdk.commands to create typed commands and read
the status on each result. Use the client as a context manager or call close
to release its channel. The default deadline is ten seconds per RPC.

This release supports explicit endpoints and shard IDs. Cluster routing and
the Feast and LMCache configuration paths are implemented separately.
