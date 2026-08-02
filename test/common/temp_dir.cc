// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "test/common/temp_dir.h"

#include <byte/include/assert.h>
#include <byte/io/file_util.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <random>

namespace bcache2 {

TempDir::TempDir() : TempDir("TmpDir_") {}

TempDir::TempDir(const std::string& prefix) : dir_(prefix) {
    std::random_device rd;
    for (int i = 0; i < 8; ++i) {
        dir_.push_back(rd() % 26 + 'A');
    }

    int ret = mkdir(dir_.c_str(), 0755);
    BYTE_ASSERT(ret == 0);
}

TempDir::~TempDir() {
    byte::Status status = byte::DeleteDirectory(dir_, true);
    BYTE_ASSERT_DEBUG(status.ok());
}

std::string TempDir::GetDir() const { return dir_; }

uint32_t GetIdlePort() {
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = 0;  // bind random port
    addr.sin_addr.s_addr = INADDR_ANY;

    socklen_t socklen = sizeof(addr);
    if (bind(sock_fd, (struct sockaddr*)&addr, socklen) != 0) {
        return 0;
    }

    if (getsockname(sock_fd, (sockaddr*)&addr, &socklen) != 0) {
        return 0;
    }
    return ntohs(addr.sin_port);
}

}  // namespace bcache2
