# TemporalStore Docker Images

This directory holds the Docker build paths. Pick the one that matches what you
need — most developers want the first.

| You want… | Use | Base image | Who it's for |
| --- | --- | --- | --- |
| A running TemporalStore, fast | [`../docker-compose.single-node.yml`](../docker-compose.single-node.yml) → [`Dockerfile.single-node`](Dockerfile.single-node) | public `rust:1.87` | anyone |
| The full Rust multi-binary image | [`../Dockerfile`](../Dockerfile) | public `rust:1.87` | Rust service/dev work |
| A C++ build on a public base | [`README.ubuntu22.md`](README.ubuntu22.md) → [`Dockerfile.ubuntu22`](Dockerfile.ubuntu22) | public `ubuntu:22.04` | building the C++ service |
| The internal C++ compile image | [`Makefile`](Makefile) → [`Dockerfile`](Dockerfile) | `hub.byted.org` (internal) | maintainers on the internal network |

## Fastest: run a single node (Rust)

No toolchain on your host — just Docker. From the repo root:

```bash
docker compose -f docker-compose.single-node.yml up --build
```

Then `curl http://127.0.0.1:17102/health`. Full walkthrough, smoke test, and how
to install Docker itself: [`../docs/INSTALL.md`](../docs/INSTALL.md).

## Build the C++ service (public Ubuntu 22.04)

External and open-source contributors build the C++ service on a stock Ubuntu
22.04 base. See [`README.ubuntu22.md`](README.ubuntu22.md) for prerequisites and
the exact commands:

```bash
docker build -t temporalstore-build:ubuntu22 -f docker/Dockerfile.ubuntu22 .
```

## Internal C++ compile image (maintainers only)

`Makefile` + `Dockerfile` build the ByteDance internal compile-base image. They
pull from `hub.byted.org` and `mirrors.byted.org`, so they **only work inside
that network** — external users should use the Ubuntu 22.04 path above instead.

```bash
sudo make build   # builds bcache2.compile:v1.0 from docker/Dockerfile
sudo make run     # opens a shell in the compile container with the repo mounted
```

### Switch gcc version inside the internal image

```bash
# export module functions, then switch compilers
source $MODULESHOME/init/bash
module switch gcc/6.3.0   # or: module switch gcc/8.3.0
```

Other module commands: `module avail`, `module list`, `module load gcc/6.3.0`,
`module unload gcc`. See <https://modules.readthedocs.io/en/latest/module.html>.
