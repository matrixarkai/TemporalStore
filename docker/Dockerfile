FROM hub.byted.org/toutiao.compile_base:v2.0

# suppress debconf warnings
ARG DEBIAN_FRONTEND=noninteractive

# common libs from stretch source
# add stretch to the source list
RUN echo "deb http://mirrors.byted.org/debian stretch main contrib non-free\\ndeb http://mirrors.byted.org/debian stretch-backports main contrib non-free\\ndeb http://mirrors.byted.org/debian-security stretch/updates main contrib non-free" > /etc/apt/sources.list
RUN apt update

# install common libs
RUN apt install -y \
    # common libs from toutiao.compile_cpp.gcc_6
    scons libkrb5-dev flex bison libsasl2-dev libnuma-dev libaprutil1-dev gawk \
    libsnappy-dev automake maven libtool swig pkg-config ctags ccache zlib1g-dev \
    libbz2-dev libssl-dev libevent-dev libicu-dev libmemcached-dev \
    libmsgpack-dev python-dev libp11-kit-dev libidn11-dev libunwind-dev libapr1-dev \
    # common libs from toutiao.rhino.compile
    make rsync pkg-config build-essential bzip2 nasm zlib1g-dev \
    autoconf automake libtool libmagickwand-dev python-dev libcrypto++-dev

# update apt twice due to the "Hash Sum mismatch" issue encountered by toutiao.compile_cpp.gcc_6
RUN apt install -y  \
    # common libs from toutiao.compile_cpp.gcc_6
    g++-6 gcc-6 libreadline-dev libmysqlclient-dev unixodbc-dev \
    # common libs required by bcache2
    # python3 from jessie does not meet bcache2's requirement
    # install pkg of Code coverage, debugging tools, etc.
    bvc yasm nasm python3 python3-pip python3-venv git gdb \
    lcov valgrind tcl-dev

# gcc
RUN rm -f /usr/bin/gcc && ln -s /usr/bin/gcc-6 /usr/bin/gcc \
    && rm -f /usr/bin/g++ && ln -s /usr/bin/g++-6 /usr/bin/g++
# gcc830
COPY docker/pkgs/gcc-8.3.0.tar.gz /tmp/gcc-8.3.0.tar.gz
RUN mkdir -p /tmp/gcc \
    && tar -zxf /tmp/gcc-8.3.0.tar.gz -C /tmp/gcc --strip-components=1 \
    && cd /tmp/gcc \
    && ./contrib/download_prerequisites --no-force \
    && mkdir -p /usr/local/cpp_tools/gcc-8.3.0/ \
    && ./configure --build=x86_64-linux-gnu -enable-languages=c,c++ --disable-multilib --prefix=/usr/local/cpp_tools/gcc-8.3.0/ \
    && make -j6 \
    && make install \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /var/tmp/* /tmp/*

# env modules
COPY docker/pkgs/modules-5.1.1.tar.gz /tmp/modules-5.1.1.tar.gz
RUN mkdir -p /tmp/modules \
    && tar -zxf /tmp/modules-5.1.1.tar.gz -C /tmp/modules --strip-components=1 \
    && cd /tmp/modules \
    && ./configure --modulefilesdir=/opt/tiger/modulefiles \
    && make -j6 \
    && make install \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /var/tmp/* /tmp/*

# build cmake
COPY docker/pkgs/cmake-3.22.0.tar.gz /tmp/cmake-3.22.0.tar.gz
RUN mkdir -p /tmp/cmake \
    && tar -zxf /tmp/cmake-3.22.0.tar.gz -C /tmp/cmake --strip-components=1 \
    && cd /tmp/cmake \
    && ./configure \
    && make -j6 \
    && make install \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /var/tmp/* /tmp/*

# reset proxy
ENV HTTP_PROXY=
ENV HTTPS_PROXY=
ENV NO_PROXY=

# set PATH
ENV PATH=/opt/tiger/typhoon-blade:/home/tiger/system_op/bin:/app/bvc/bin:${PATH}

# # golang
# ENV GOROOT=/usr/local/go
# ENV GOPATH=/opt/tiger/compile_path
# ENV PATH=$PATH:/opt/common_tools:/usr/local/go/bin:/opt/tiger/compile_go_path/bin
# # may need to change according to REGION
# ENV TZ=Asia/Shanghai
# ENV GOPRIVATE=code.byted.org,gitlab.everphoto.cn,git.byted.org,sysrepo.byted.org,git.smartisan.com
# ENV GOPROXY=https://go-mod-proxy.byted.org,https://goproxy.cn,https://proxy.golang.org,direct
# ENV GOSUMDB=sum.golang.google.cn

# # golang 1.16
# RUN cd /usr/local \
#     && bvc clone -f bytedance/lang/tango1_16 go \
#     && mkdir -p /bd_build &>/dev/null \
#     && cd ${GOROOT} \
#     && cp -R bd_build/* /bd_build/ \
#     && rm -rf "pkg/obj" "pkg/$(go env GOHOSTOS)_$(go env GOARCH)/cmd" \
#     && chmod +x /bd_build/*.sh && /bd_build/deploy.sh \
#     && rm -rf /var/lib/apt/lists/* /var/cache/debconf /var/tmp/* /tmp/* \
#     && apt clean

# env
# onebox related envs
ENV BYTED_HOST_IP=""
ENV BYTED_HOST_IPV6=""

# modules
ENV MODULESHOME=/usr/local/Modules

# init modules
RUN rm -rf /opt/tiger/modulefiles/
# copy modulefiles to the destination dir
COPY docker/modulefiles /opt/tiger/modulefiles/

ENV MODULES_CMD=${MODULESHOME}/libexec/modulecmd.tcl
ENV MODULEPATH=/opt/tiger/modulefiles
ENV PATH=$PATH:${MODULESHOME}/bin

# use gcc830
ENV LD_LIBRARY_PATH=/usr/local/cpp_tools/gcc-8.3.0/lib64:/usr/local/cpp_tools/gcc-8.3.0/lib:$LD_LIBRARY_PATH
ENV PATH=/usr/local/cpp_tools/gcc-8.3.0/bin:$PATH
ENV CC=/usr/local/cpp_tools/gcc-8.3.0/bin/gcc
ENV CXX=/usr/local/cpp_tools/gcc-8.3.0/bin/g++
ENV LOADEDMODULES=gcc/8.3.0
ENV _LMFILES_=/opt/tiger/modulefiles/gcc/8.3.0

# python
RUN pip3 install --no-cache-dir -i https://bytedpypi.byted.org/simple setuptools wheel pytest

# change working dir
WORKDIR /home/tiger/

CMD ["/sbin/my_init"]
