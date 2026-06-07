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
USER temporalstore
WORKDIR /var/lib/temporalstore
