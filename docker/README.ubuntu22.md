# Ubuntu 22.04 Build

This is the repeatable Linux build path for the local source tree.

## WSL2 Setup

This machine must have WSL and Ubuntu 22.04 installed before the build can run:

```powershell
# Run from an elevated PowerShell, then reboot if Windows asks.
wsl --install
wsl --install -d Ubuntu-22.04
wsl --set-default-version 2
```

After Ubuntu starts, copy the source into the Linux filesystem for faster builds:

```bash
mkdir -p ~/src
rsync -aL --delete /mnt/c/b2src/ ~/src/temporalstore/
cd ~/src/temporalstore
```

## Packages

```bash
sudo apt-get update
sudo apt-get install -y \
  autoconf automake bison build-essential bzip2 ca-certificates curl flex gawk git \
  libapr1-dev libaprutil1-dev libboost-all-dev libbz2-dev libevent-dev libev-dev \
  libgflags-dev libicu-dev libkrb5-dev libleveldb-dev liblz4-dev libnuma-dev \
  libsasl2-dev libsnappy-dev libssl-dev libtool libunwind-dev make nasm pkg-config \
  python3 python3-pip python3-venv rsync swig unzip yasm zlib1g-dev
```

## Build

The build script compiles bundled Protobuf 3.2.0 into `.local/ubuntu22/protobuf-3.2.0`
and forces CMake/bRPC to use that pinned `protoc` and library.

```bash
BUILD_TYPE=Debug ENABLE_MTCACHE=OFF JOBS=$(nproc) bash tools/build_ubuntu22.sh
```

For a release build:

```bash
BUILD_TYPE=Release ENABLE_MTCACHE=OFF JOBS=$(nproc) bash tools/build_ubuntu22.sh
```

`ENABLE_MTCACHE=OFF` is the first-pass build. Turn it on only after the core
server/metaserver build is clean and MtCache's own dependencies are validated.

## Docker

After Docker is installed and the source is in a Linux filesystem:

```bash
docker build -t temporalstore-build:ubuntu22 -f docker/Dockerfile.ubuntu22 .
```

The Docker image uses Ubuntu 22.04 and the same pinned Protobuf 3.2.0 path as
the WSL build script.

## Compose Plan

Use Docker Compose after the binaries build:

1. Start MinIO as the S3-compatible object-store target.
2. Start metaserver with a local/MinIO-backed object-store config.
3. Start one or more server instances.
4. Add a smoke-test client that creates a table, writes sample keys, restarts a
   server, and verifies recovery from the durable stream.
