# The basic image used to compile MTCache with Clang 12.0.1.
# Including MTCache thirty_party building with Clang and ASAN.
# Without GCC compiler collection.

FROM hub.byted.org/compile/debian.stretch.compile_base:5871d5c19485e642e5841ba1a97e16ae

ENV HTTPS_PROXY http://bj-rd-proxy.byted.org:3128

# install base
RUN apt-get -q update \
  && apt-get -q install -y --no-install-recommends \
  bzip2 wget git byacc flex automake libtool binutils-source binutils-dev \
  bison libncurses5-dev make mlocate unzip patch vim-common zip libidn2-0-dev \
  libcurl4-openssl-dev build-essential libiberty-dev python3 gperf uuid-dev libnuma-dev openjdk-8-jdk ccache \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

# install cmake
RUN export http_proxy=sys-proxy-rd-relay.byted.org:8118 https_proxy=sys-proxy-rd-relay.byted.org:8118 \
  && wget -q https://cmake.org/files/v3.20/cmake-3.20.4-linux-x86_64.tar.gz -O /tmp/cmake-3.20.4-linux-x86_64.tar.gz \
  && tar -zxf /tmp/cmake-3.20.4-linux-x86_64.tar.gz -C /usr/local \
  && ln -s /usr/local/cmake-3.20.4-linux-x86_64/bin/cmake /usr/local/bin/cmake \
  && ln -s /usr/local/cmake-3.20.4-linux-x86_64/bin/ctest /usr/local/bin/ctest \
  && rm -rf /tmp/*


# install clang
RUN export http_proxy=sys-proxy-rd-relay.byted.org:8118 https_proxy=sys-proxy-rd-relay.byted.org:8118 \
  && wget -q https://github.com/llvm/llvm-project/releases/download/llvmorg-12.0.1/clang+llvm-12.0.1-x86_64-linux-gnu-ubuntu-16.04.tar.xz -O /tmp/clang.tar.xz \
  && mkdir -p /opt/clang_12_0_1 \
  && tar -xf /tmp/clang.tar.xz -C /opt/clang_12_0_1 --strip-components=1 \
  && cd /opt/clang_12_0_1/lib \
  && rm -rf libunwind* \
  && rm -rf /tmp/*

ENV CLANG_HOME /opt/clang_12_0_1
ENV CC ${CLANG_HOME}/bin/clang
ENV CXX ${CLANG_HOME}/bin/clang++
ENV PATH ${CLANG_HOME}/bin:${PATH}
ENV PATH /usr/local/jdk/bin:$PATH

# build MTCache third party with ASAN
COPY . /tmp/mtcache
RUN export http_proxy=sys-proxy-rd-relay.byted.org:8118 https_proxy=sys-proxy-rd-relay.byted.org:8118 \
  && cd /tmp/mtcache/third_party \
  && rm -rf downloads \
  && mkdir build \
  && cd build \
  && cmake -DCMAKE_INSTALL_PREFIX=/opt/mtcache_thirdparty_asan -DENABLE_ASAN=ON ../ \
  && make \
  && rm -rf /tmp/mtcache


# set ASAN_OPTIONS for ASAN because of some unsolved issues with Clang edition
ENV ASAN_OPTIONS detect_container_overflow=true

ENV HTTPS_PROXY=

# add dirs according to the rules of compile image
RUN mkdir -p /opt/tiger && chown tiger:tiger /opt/tiger
RUN mkdir -p /var/log/tiger && chown tiger:tiger /var/log/tiger
RUN mkdir -p /opt/log/tiger && chown tiger:tiger /opt/log/tiger
