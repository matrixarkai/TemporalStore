// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "common/pidfile.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#if defined(__CYGWIN__)
extern "C" int ftruncate(int, off_t);
#endif

namespace bcache2 {

PidFile::~PidFile() {
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }

    unlink(filepath_.c_str());
}

bool PidFile::TryLock() {
    if (f_) {
        return false;
    }
    f_ = true;

    fd_ = open(filepath_.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd_ == -1) {
        return false;
    }

    if (flock(fd_, LOCK_EX | LOCK_NB) != 0) {
        return false;
    }

    if (ftruncate(fd_, 0) != 0) {
        return false;
    }
    std::string pid = std::to_string(getpid());
    ssize_t size = write(fd_, pid.c_str(), pid.size());
    if (size == -1 || static_cast<size_t>(size) != pid.size()) {
        return false;
    }
    return true;
}

}  // namespace bcache2
