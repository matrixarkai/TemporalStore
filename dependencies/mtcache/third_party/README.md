### Prerequisites:
GCC >= 10.2


#### debian 9.0 above
Install tools depended:

apt-get -q update \
    && apt-get -q install -y --no-install-recommends \
      bzip2 wget git byacc flex automake libtool binutils-source binutils-dev \
      bison libncurses5-dev make mlocate unzip patch vim-common zip libidn2-0-dev \
      libcurl4-openssl-dev build-essential libiberty-dev python3 gperf uuid-dev libnuma-dev ccache

#### Docker way
Both clang and tools have been installed inside docker image hub.byted.org/compile/mtcache_compile_clang:3cd9fbe9012d9e59bf73645bd9d79125
Please use it directly to build mtcache project.

Run the following command to start docker:

```
docker run -it -v /{host_mtcache_project_dir}:/{docker_mtcache_project_dir} hub.byted.org/compile/mtcache_compile_clang:3cd9fbe9012d9e59bf73645bd9d79125
```

#### Build thirdparty libraries
To build thirdpary libraries, do the following steps:

```
cd ${mtcache_project_dir}/third_party
mkdir build && cd build
cmake ..
make
```

#### Build 3rd with clang
Now, the docker image with clang has been ready (Only for China Area)
So if you want to build mtcache 3rd with clang, please follow steps:
```
docker run -it -v /{host_mtcache_project_dir}:/{docker_mtcache_project_dir} hub.byted.org/compile/mtcache_compile_clang:3cd9fbe9012d9e59bf73645bd9d79125
cd {docker_mtcache_project_dir}/third_party
mkdir build && cd build
cmake .. -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_C_COMPILER=clang
```

After that, you could find the outputs in ${mtcache_project_dir}/third_party/install
Enjoy it.
