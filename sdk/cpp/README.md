# TemporalStore C++ SDK

The C++ SDK is the native implementation. The public convenience include is:

```cpp
#include "temporalstore/client.h"
```

It aliases the production client types from `src/client/temporalstore_client.h` into the
`temporalstore` namespace while keeping the implementation in the existing native client.

## Build In This Repository

```bash
cmake --build build-ubuntu22 --target customer_client_example -j4
```

## Example

```cpp
temporalstore::ClientOptions options;
options.metaserver_addr = "127.0.0.1:18200";
options.namespace_name = "sdk_ns";
options.table_name = "sdk_table";

std::unique_ptr<temporalstore::Client> client;
temporalstore::Status status = temporalstore::Client::Connect(options, &client);
if (!status.ok()) {
    return 1;
}

client->PutString("user:42", "{\"tier\":\"gold\"}");
std::string profile;
client->GetString("user:42", &profile);
```
