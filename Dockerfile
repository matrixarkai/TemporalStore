FROM rust:1.87-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --workspace --release

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --create-home temporalstore
COPY --from=builder /src/target/release/metaserver /usr/local/bin/metaserver
COPY --from=builder /src/target/release/server /usr/local/bin/server
COPY --from=builder /src/target/release/proxy /usr/local/bin/proxy
COPY --from=builder /src/target/release/redis_proxy /usr/local/bin/redis_proxy
COPY --from=builder /src/target/release/distributed_raft_harness /usr/local/bin/distributed_raft_harness
COPY --from=builder /src/target/release/scale_harness /usr/local/bin/scale_harness
COPY --from=builder /src/target/release/client_scale_harness /usr/local/bin/client_scale_harness
COPY --from=builder /src/target/release/raft_secondary_replication_harness /usr/local/bin/raft_secondary_replication_harness
COPY --from=builder /src/target/release/storage_modes_harness /usr/local/bin/storage_modes_harness
USER temporalstore
WORKDIR /var/lib/temporalstore
