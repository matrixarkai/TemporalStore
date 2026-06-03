#ifdef __CYGWIN__
#include <errno.h>

#include <atomic>
#include <new>

#include "absl/base/internal/thread_identity.h"
#include "absl/synchronization/internal/per_thread_sem.h"
#include "absl/synchronization/internal/waiter.h"

extern "C" bool AbslContainerInternalSampleEverything() { return false; }

extern "C" int* __errno_location() { return &errno; }

extern "C" int RunningOnValgrind() { return 0; }

extern "C" void AbslInternalPerThreadSemInit(
        absl::base_internal::ThreadIdentity* identity) {
    new (absl::synchronization_internal::Waiter::GetWaiter(identity))
            absl::synchronization_internal::Waiter();
}

extern "C" void AbslInternalPerThreadSemPost(
        absl::base_internal::ThreadIdentity* identity) {
    absl::synchronization_internal::Waiter::GetWaiter(identity)->Post();
}

extern "C" void AbslInternalPerThreadSemPoke(
        absl::base_internal::ThreadIdentity* identity) {
    absl::synchronization_internal::Waiter::GetWaiter(identity)->Poke();
}

extern "C" bool AbslInternalPerThreadSemWait(
        absl::synchronization_internal::KernelTimeout timeout) {
    absl::base_internal::ThreadIdentity* identity =
            absl::synchronization_internal::GetOrCreateCurrentThreadIdentity();

    int ticker = identity->ticker.load(std::memory_order_relaxed);
    identity->wait_start.store(ticker ? ticker : 1, std::memory_order_relaxed);
    identity->is_idle.store(false, std::memory_order_relaxed);

    if (identity->blocked_count_ptr != nullptr) {
        identity->blocked_count_ptr->fetch_add(1, std::memory_order_relaxed);
    }

    bool timeout_reached =
            !absl::synchronization_internal::Waiter::GetWaiter(identity)->Wait(timeout);

    if (identity->blocked_count_ptr != nullptr) {
        identity->blocked_count_ptr->fetch_sub(1, std::memory_order_relaxed);
    }

    identity->is_idle.store(false, std::memory_order_relaxed);
    identity->wait_start.store(0, std::memory_order_relaxed);
    return !timeout_reached;
}
#endif
