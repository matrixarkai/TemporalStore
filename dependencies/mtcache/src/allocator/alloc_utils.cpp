#include "alloc_utils.h"

#include "common/logging.h"
#include "util/align_util.h"

#include <sys/mman.h>

#include <cerrno>
#include <fcntl.h>
#include <filesystem>
#include <mutex>
#include <unistd.h>
#include <xmmintrin.h>

// TODO(lyj): move to a better place
#define PERM_rw_r__r__ 0644

namespace mtcache {

#ifndef MAP_HUGE_SHIFT
#define MAP_HUGE_SHIFT 26
#endif

#ifndef MAP_HUGE_2MB
#define MAP_HUGE_2MB (21 << MAP_HUGE_SHIFT)
#endif

static constexpr size_t kHugePageSz = 2 * 1024 * 1024;

static void* FindHintAddrForMmap(size_t object_len, size_t alignment) {
  size_t hint_request_size = object_len + alignment;
  void* hint_addr = mmap(nullptr, hint_request_size, PROT_READ,
                         MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (hint_addr == MAP_FAILED) {
    return nullptr;
  }
  munmap(hint_addr, hint_request_size);
  hint_addr = reinterpret_cast<void*>(
      ROUND_UP(reinterpret_cast<uintptr_t>(hint_addr), alignment));
  return hint_addr;
}

static int VMemFree(void* addr, size_t len) {
  int res = munmap(addr, len);
  if (res != 0) {
    LOG(ERROR) << "munmap failed, addr=" << addr << ", len=" << len
               << ", error msg is: " << google::StrError(errno);
  }
  return res;
}

AllocatorType ParseAllocatorType(const std::string& allocator_type) {
  if (allocator_type == "Log") {
    return AllocatorType::kLogBasedAllocator;
  } else if (allocator_type == "Pool") {
    return AllocatorType::kPoolBasedAllocator;
  } else if (allocator_type == "Jemalloc") {
    return AllocatorType::kJeAllocator;
  } else {
    LOG(FATAL) << "Invalid allocator type: " << allocator_type;
  }
}

noodle::Result<void*, CacheError> DramAllocateObject(size_t object_len,
                                                     size_t alignment) {
  // Find a proper address to do final mmap
  void* hint_addr = FindHintAddrForMmap(object_len, alignment);
  if (hint_addr == nullptr) {
    return &Errors::kDramAllocationFailed;
  }

  // Final mmap
  void* addr = mmap(hint_addr, object_len, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (addr == MAP_FAILED) {
    return &Errors::kDramAllocationFailed;
  }
  if (reinterpret_cast<uintptr_t>(addr) % alignment != 0) {
    munmap(addr, object_len);
    return &Errors::kAllocationRetry;
  }
  return addr;
}

noodle::Result<void*, CacheError> DramAllocateObject_v2(void* address,
                                                        size_t object_len) {
  DCHECK((object_len > kHugePageSz) && (object_len % kHugePageSz == 0));
  void* addr = mmap(address, object_len, PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_FIXED | MAP_ANONYMOUS, -1, 0);
  if (addr == MAP_FAILED) {
    LOG(ERROR) << "Failed to mmap in DRAM, " << google::StrError(errno);
    return &Errors::kDramAllocationFailed;
  }
  return addr;
}

noodle::Result<void, CacheError> DramFreeObject(void* addr, size_t len) {
  if (VMemFree(addr, len) != 0) {
    return &Errors::kDramFreeFailed;
  }
  return {};
}

// TODO(lyj): return more specific error
noodle::Result<void*, CacheError> PMemAllocateObject(
    const std::string& filename, size_t object_len, size_t alignment) {
  int fd = open(filename.c_str(), O_CREAT | O_RDWR, PERM_rw_r__r__);
  if (fd < 0) {
    LOG(ERROR) << "Fail to open file on PMEM, path=" << filename;
    return &Errors::kPMemAllocationFailed;
  }
  // Port from PMDK, I(lyj) don't know why `ftruncate` before `fallocate`
  if (ftruncate(fd, static_cast<off_t>(object_len)) != 0) {
    close(fd);
    LOG(ERROR) << "Fail to truncate file on PMEM, path=" << filename;
    return &Errors::kPMemAllocationFailed;
  }
  int r = fallocate(fd, 0, 0, static_cast<off_t>(object_len));
  if (r != 0) {
    close(fd);
    LOG(ERROR) << "Fail to fallocate file on PMEM, path=" << filename;
    return &Errors::kPMemAllocationFailed;
  }

  // Find a proper address to do final mmap
  void* hint_addr = FindHintAddrForMmap(object_len, alignment);
  if (hint_addr == nullptr) {
    close(fd);
    LOG(ERROR) << "Fail to find mmap hint for file on PMEM, path=" << filename;
    return &Errors::kPMemAllocationFailed;
  }

  // Final mmap
  void* addr =
      mmap(hint_addr, object_len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  close(fd);
  if (addr == MAP_FAILED) {
    LOG(ERROR) << "Fail to mmap file on PMEM, path=" << filename;
    return &Errors::kPMemAllocationFailed;
  }
  if (reinterpret_cast<uintptr_t>(addr) % alignment != 0) {
    munmap(addr, object_len);
    return &Errors::kAllocationRetry;
  }
  return addr;
}

noodle::Result<void*, CacheError> PMemAllocateObject_v2(
    void* address, const std::string& filename, size_t object_len) {
  DCHECK((object_len > kHugePageSz) && (object_len % kHugePageSz == 0));
  int fd = open(filename.c_str(), O_CREAT | O_RDWR, PERM_rw_r__r__);
  if (fd < 0) {
    LOG(ERROR) << "Fail to open file on PMEM, path=" << filename
               << ", msg: " << google::StrError(errno);
    close(fd);
    return &Errors::kPMemAllocationFailed;
  }
  // Port from PMDK, I(lyj) don't know why `ftruncate` before `fallocate`
  if (ftruncate(fd, static_cast<off_t>(object_len)) != 0) {
    LOG(ERROR) << "Fail to truncate file on PMEM, path=" << filename
               << ", msg: " << google::StrError(errno);
    close(fd);
    return &Errors::kPMemAllocationFailed;
  }
  int r = fallocate(fd, 0, 0, static_cast<off_t>(object_len));
  if (r != 0) {
    LOG(ERROR) << "Fail to fallocate file on PMEM, path=" << filename
               << ", msg: " << google::StrError(errno);
    close(fd);
    return &Errors::kPMemAllocationFailed;
  }

  // Final mmap
  void* addr = mmap(address, object_len, PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_FIXED, fd, 0);
  close(fd);
  if (addr == MAP_FAILED) {
    LOG(ERROR) << "Fail to mmap file on PMEM, path=" << filename << ", "
               << google::StrError(errno);
    return &Errors::kPMemAllocationFailed;
  }
  return addr;
}

noodle::Result<void, CacheError> PMemFreeObject(void* addr, size_t len) {
  if (VMemFree(addr, len) != 0) {
    return &Errors::kPMemFreeFailed;
  }
  return {};
}

noodle::Result<void*, CacheError> PreAllocate(size_t len, size_t align) {
  CHECK((align >= kHugePageSz) && (align % kHugePageSz == 0))
      << "Alignment must be multiple times of huge page size: " << kHugePageSz;
  CHECK_GT(len, 0) << "PreAllocate length must be larger than 0";
  size_t valid_len = ROUND_UP(len, kHugePageSz);
  // 1. We use PROT_NONE flag to prevent users from reading/writing to this
  //    reserved VM directly since this function only intends to "reserve" a
  //    large continuous range of VM.
  // 2. We use MAP_HUGETLB flag here to prevent this "reserved" virtual memory
  //    being core-dumpped if the process encounters failure. According to
  //    https://man7.org/linux/man-pages/man5/core.5.html, huge pages will not
  //    be written to core dump file. Since the reserved virtual memory is
  //    usually very large and there is no valid data in it, it should not
  //    be written to core dump file. Because the returned address can not be
  //    read/write directly and `DramAllocateObject_v2`/
  //    `PMemAllocateObject_v2` will fixed-mmap again without "MAP_HUGETLB"
  //    flag, at that time the mappings will become small pages again.
  void* addr = mmap(
      nullptr, valid_len + align, PROT_NONE,
      MAP_SHARED | MAP_ANONYMOUS | MAP_NORESERVE | MAP_HUGETLB | MAP_HUGE_2MB,
      -1, 0);
  if (addr == MAP_FAILED) {
    LOG(ERROR) << "Mmap failed when pre-allocate, size = " << valid_len
               << ", align=" << align
               << ", error msg is: " << google::StrError(errno);
    return &Errors::kPreAllocateVMFailed;
  }
  uintptr_t aligned_addr = (reinterpret_cast<uintptr_t>(addr) &
                            ~(static_cast<uintptr_t>(align) - 1ULL));
  // If addr is already aligned, then munmap the alignment space at tail.
  // If addr is not aligned, then munmap the extra space at head && tail.
  if (aligned_addr != reinterpret_cast<uintptr_t>(addr)) {
    aligned_addr += static_cast<uintptr_t>(align);
    size_t sz = aligned_addr - reinterpret_cast<uintptr_t>(addr);
    int mres = munmap(addr, sz);
    if (mres != 0) {
      LOG(ERROR) << "munmap failed in PreAllocate, len = " << sz
                 << ", error msg is: " << google::StrError(errno);
      // We do not return error here because the extra mmaped VM will not
      // harm the process.
    }
  }
  size_t sz = reinterpret_cast<uintptr_t>(addr) + align - aligned_addr;
  int mres = munmap(reinterpret_cast<void*>(aligned_addr + valid_len), sz);
  if (mres != 0) {
    LOG(ERROR) << "munmap failed in PreAllocate, len = " << sz
               << ", error msg is: " << google::StrError(errno);
  }
  return reinterpret_cast<void*>(aligned_addr);
}

noodle::Result<void, CacheError> PostFree(void* addr, size_t len) {
  size_t valid_len = ROUND_UP(len, kHugePageSz);
  if (VMemFree(addr, valid_len) != 0) {
    return &Errors::kPostFreeVMFailed;
  }
  return {};
}

void PMemFlush(void* addr, size_t len) {
#ifndef NDEBUG
  // In certain CI / test environments, CLWB is not supported.
  return;
#endif
  // In ByteDance, servers in which PMem is installed must have CPUs equal or
  // newer than Intel Xeon 8260. CLWB is always supported.
  uintptr_t uptr;

  /*
   * Loop through cache-line-size (typically 64B) aligned chunks
   * covering the given range.
   */
  for (uptr = (uintptr_t)addr & ~(64 - 1); uptr < (uintptr_t)addr + len;
       uptr += 64) {
    // XSAVEOPT Opcode:   0FAE/6
    // CLWB     Opcode: 660FAE/6 -> 66XSAVEOPT
    asm volatile(".byte 0x66; xsaveopt %0" : "+m"(*(volatile char*)(uptr)));
  }
}

void PMemDrain() { _mm_sfence(); }

void PMemPersist(void* addr, size_t len) {
  if (len > 0) {
    PMemFlush(addr, len);
    PMemDrain();
  }
}

int GetThreadLocalResourceID() {
  static std::mutex pool_mtx;
  static std::vector<bool> id_pool;  // protected by pool_mtx
  static __thread int tls_id = -1;

  struct IDIniter {
    IDIniter() {
      tls_id = 0;
      std::lock_guard<std::mutex> guard(pool_mtx);
      for (bool empty : id_pool) {
        if (empty) {
          id_pool[tls_id] = false;
          return;
        }
        ++tls_id;
      }
      id_pool.emplace_back();
    }

    ~IDIniter() {
      std::lock_guard<std::mutex> guard(pool_mtx);
      id_pool[tls_id] = true;
    }
  };

  static thread_local IDIniter tls_initer;
  return tls_id;
}

std::vector<std::string> GetPmemFileName(
    const std::string& data_path, int64_t expected_len,
    std::vector<std::string>* invalid_fname) {
  const std::filesystem::path dir{data_path};
  std::vector<std::string> valid_fname;
  if (expected_len < 0) {
    // All files are valid.
    for (const auto& file : std::filesystem::directory_iterator(dir)) {
      if (!file.is_regular_file()) {
        continue;
      }
      valid_fname.emplace_back(file.path().filename());
    }
  } else {
    // Check valid files.
    for (const auto& file : std::filesystem::directory_iterator(dir)) {
      if (!file.is_regular_file()) {
        continue;
      }
      if (file.file_size() == expected_len) {
        valid_fname.emplace_back(file.path().filename());
      } else if (invalid_fname != nullptr) {
        invalid_fname->emplace_back(file.path().filename());
      }
    }
  }
  return valid_fname;
}

noodle::Result<void*, CacheError> PMemMapFile(void* addr,
                                              const std::string& filename,
                                              size_t object_len) {
  int fd = open(filename.c_str(), O_RDWR, PERM_rw_r__r__);
  if (fd < 0) {
    LOG(ERROR) << "Fail to open file on PMEM, path=" << filename;
    return &Errors::kPmemOpenFailed;
  }
  // Final mmap
  void* address = mmap(addr, object_len, PROT_READ | PROT_WRITE,
                       MAP_SHARED | MAP_FIXED, fd, 0);
  close(fd);
  if (address == MAP_FAILED) {
    LOG(ERROR) << "Fail to mmap file on PMEM, path=" << filename << ", "
               << google::StrError(errno);
    return &Errors::kPmemOpenFailed;
  }
  return address;
}

bool DeletePmemFile(const std::string& path) {
  return std::filesystem::remove(path);
}

}  // namespace mtcache
