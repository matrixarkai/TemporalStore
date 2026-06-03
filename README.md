# BCache evolution

## Setup Development Environment

1. Install cmake
2. Clone code: git clone -b evolution git@code.byted.org:storage/BCache.git
3. cd BCache
4. git submodule update --init --recursive
5. mkdir build && cd build && cmake .. && make -j8

## Merge Request

1. Commit message follow: https://www.conventionalcommits.org/zh-hans/v1.0.0-beta.4/
2. Use `merge-request` description template
3. Rebase your commits before merge to evolution
