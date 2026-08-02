/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "numa_thread_pool_executor.h"

#include <folly/executors/GlobalThreadPoolList.h>
#include <folly/synchronization/AsymmetricMemoryBarrier.h>
#include <folly/tracing/StaticTracepoint.h>

namespace mtcache {

using SyncVecThreadPoolExecutors =
    folly::Synchronized<std::vector<NumaThreadPoolExecutor*>>;

SyncVecThreadPoolExecutors& getSyncVecThreadPoolExecutors() {
  static folly::Indestructible<SyncVecThreadPoolExecutors> storage;
  return *storage;
}

void NumaThreadPoolExecutor::registerThreadPoolExecutor(
    NumaThreadPoolExecutor* tpe) {
  getSyncVecThreadPoolExecutors().wlock()->push_back(tpe);
}

void NumaThreadPoolExecutor::deregisterThreadPoolExecutor(
    NumaThreadPoolExecutor* tpe) {
  getSyncVecThreadPoolExecutors().withWLock([tpe](auto& tpes) {
    tpes.erase(std::remove(tpes.begin(), tpes.end(), tpe), tpes.end());
  });
}

DEFINE_int64(numa_threadtimeout_ms, 60000,
             "Idle time before NumaThreadPoolExecutor threads are joined");

NumaThreadPoolExecutor::NumaThreadPoolExecutor(
    size_t /* maxThreads */, size_t minThreads,
    std::shared_ptr<ThreadFactory> threadFactory, bool isWaitForAll)
    : threadFactory_(std::move(threadFactory)),
      isWaitForAll_(isWaitForAll),
      taskStatsCallbacks_(std::make_shared<TaskStatsCallbackRegistry>()),
      threadPoolHook_("folly::NumaThreadPoolExecutor"),
      minThreads_(minThreads),
      threadTimeout_(FLAGS_numa_threadtimeout_ms) {
  namePrefix_ = getNameHelper();
}

NumaThreadPoolExecutor::~NumaThreadPoolExecutor() {
  joinKeepAliveOnce();
  CHECK_EQ(0, threadList_.get().size());
}

NumaThreadPoolExecutor::Task::Task(Func&& func,
                                   std::chrono::milliseconds expiration,
                                   Func&& expireCallback)
    : func_(std::move(func)),
      expiration_(expiration),
      expireCallback_(std::move(expireCallback)),
      context_(folly::RequestContext::saveContext()) {
  // Assume that the task in enqueued on creation
  enqueueTime_ = std::chrono::steady_clock::now();
}

namespace {

template <class F>
void nothrow(const char* name, F&& f) {
  try {
    f();
  } catch (const std::exception& e) {
    LOG(ERROR) << "NumaThreadPoolExecutor: " << name << " threw unhandled "
               << typeid(e).name() << " exception: " << e.what();
  } catch (...) {
    LOG(ERROR) << "NumaThreadPoolExecutor: " << name
               << " threw unhandled non-exception object";
  }
}

}  // namespace

void NumaThreadPoolExecutor::runTask(const ThreadPtr& thread, Task&& task) {
  thread->idle.store(false, std::memory_order_relaxed);
  auto startTime = std::chrono::steady_clock::now();
  TaskStats stats;
  stats.enqueueTime = task.enqueueTime_;
  if (task.context_) {
    stats.requestId = task.context_->getRootId();
  }
  stats.waitTime = startTime - task.enqueueTime_;

  {
    folly::RequestContextScopeGuard rctx(task.context_);
    if (task.expiration_ > std::chrono::milliseconds(0) &&
        stats.waitTime >= task.expiration_) {
      task.func_ = nullptr;
      stats.expired = true;
      if (task.expireCallback_ != nullptr) {
        invokeCatchingExns("NumaThreadPoolExecutor: expireCallback",
                           std::exchange(task.expireCallback_, {}));
      }
    } else {
      invokeCatchingExns("NumaThreadPoolExecutor: func",
                         std::exchange(task.func_, {}));
      task.expireCallback_ = nullptr;
    }
  }
  if (!stats.expired) {
    stats.runTime = std::chrono::steady_clock::now() - startTime;
  }

  // Times in this USDT use granularity of std::chrono::steady_clock::duration,
  // which is platform dependent. On Facebook servers, the granularity is
  // nanoseconds. We explicitly do not perform any unit conversions to avoid
  // unnecessary costs and leave it to consumers of this data to know what
  // effective clock resolution is.
  FOLLY_SDT(folly, thread_pool_executor_task_stats, namePrefix_.c_str(),
            stats.requestId, stats.enqueueTime.time_since_epoch().count(),
            stats.waitTime.count(), stats.runTime.count());

  thread->idle.store(true, std::memory_order_relaxed);
  thread->lastActiveTime.store(std::chrono::steady_clock::now(),
                               std::memory_order_relaxed);
  thread->taskStatsCallbacks->callbackList.withRLock([&](auto& callbacks) {
    *thread->taskStatsCallbacks->inCallback = true;
    SCOPE_EXIT { *thread->taskStatsCallbacks->inCallback = false; };
    try {
      for (auto& callback : callbacks) {
        callback(stats);
      }
    } catch (const std::exception& e) {
      LOG(ERROR) << "NumaThreadPoolExecutor: task stats callback threw "
                    "unhandled "
                 << typeid(e).name() << " exception: " << e.what();
    } catch (...) {
      LOG(ERROR) << "NumaThreadPoolExecutor: task stats callback threw "
                    "unhandled non-exception object";
    }
  });
}

void NumaThreadPoolExecutor::add(Func, std::chrono::milliseconds, Func) {
  throw std::runtime_error(
      "add() with expiration is not implemented for this Executor");
}

size_t NumaThreadPoolExecutor::numThreads() const {
  return maxThreads_.load(std::memory_order_relaxed);
}

size_t NumaThreadPoolExecutor::numActiveThreads() const {
  return activeThreads_.load(std::memory_order_relaxed);
}

// Set the maximum number of running threads.
void NumaThreadPoolExecutor::setNumThreads(size_t numThreads) {
  /* Since NumaThreadPoolExecutor may be dynamically adjusting the number of
     threads, we adjust the relevant variables instead of changing
     the number of threads directly.  Roughly:

     If numThreads < minthreads reset minThreads to numThreads.

     If numThreads < active threads, reduce number of running threads.

     If the number of pending tasks is > 0, then increase the currently
     active number of threads such that we can run all the tasks, or reach
     numThreads.

     Note that if there are observers, we actually have to create all
     the threads, because some observer implementations need to 'observe'
     all thread creation (see tests for an example of this)
  */

  size_t numThreadsToJoin = 0;
  {
    SharedMutex::WriteHolder w{&threadListLock_};
    auto pending = getPendingTaskCountImpl();
    maxThreads_.store(numThreads, std::memory_order_relaxed);
    auto active = activeThreads_.load(std::memory_order_relaxed);
    auto minthreads = minThreads_.load(std::memory_order_relaxed);
    if (numThreads < minthreads) {
      minthreads = numThreads;
      minThreads_.store(numThreads, std::memory_order_relaxed);
    }
    if (active > numThreads) {
      numThreadsToJoin = active - numThreads;
      if (numThreadsToJoin > active - minthreads) {
        numThreadsToJoin = active - minthreads;
      }
      NumaThreadPoolExecutor::removeThreads(numThreadsToJoin, false);
      activeThreads_.store(active - numThreadsToJoin,
                           std::memory_order_relaxed);
    } else if (pending > 0 || !observers_.empty() || active < minthreads) {
      size_t numToAdd = std::min(pending, numThreads - active);
      if (!observers_.empty()) {
        numToAdd = numThreads - active;
      }
      if (active + numToAdd < minthreads) {
        numToAdd = minthreads - active;
      }
      NumaThreadPoolExecutor::addThreads(numToAdd);
      activeThreads_.store(active + numToAdd, std::memory_order_relaxed);
    }
  }

  /* We may have removed some threads, attempt to join them */
  joinStoppedThreads(numThreadsToJoin);
}

// threadListLock_ is writelocked
void NumaThreadPoolExecutor::addThreads(size_t n) {
  std::vector<ThreadPtr> newThreads;
  for (size_t i = 0; i < n; i++) {
    newThreads.push_back(makeThread());
  }
  for (auto& thread : newThreads) {
    // TODO need a notion of failing to create the thread
    // and then handling for that case
    thread->handle = threadFactory_->newThread(
        std::bind(&NumaThreadPoolExecutor::threadRun, this, thread));
    threadList_.add(thread);
  }
  for (auto& thread : newThreads) {
    thread->startupBaton.wait(
        folly::Baton<>::wait_options().logging_enabled(false));
  }
  if (cpuAffinityMask_ != nullptr) {
    for (auto& thread : newThreads) {
      int32_t res = setThreadCpuAffinity(thread, cpuAffinityMask_.get());
      if (res != 0) {
        LOG(ERROR) << "Fail to set cpu affinity for new added threads, errno="
                   << res;
      }
    }
  }
  for (auto& o : observers_) {
    for (auto& thread : newThreads) {
      o->threadStarted(thread.get());
    }
  }
}

// threadListLock_ is writelocked
void NumaThreadPoolExecutor::removeThreads(size_t n, bool isJoin) {
  isJoin_ = isJoin;
  stopThreads(n);
}

int32_t NumaThreadPoolExecutor::setCpuAffinity(
    const std::vector<int32_t> cpus) {
  if (cpus.size() == 0) {
    // no cpu can the threads run on.
    LOG(ERROR) << "Can not set an empty cpu set!";
    return EINVAL;
  }
  cpu_set_t mask;
  CPU_ZERO(&mask);
  for (int32_t cpu_id : cpus) {
    CPU_SET(cpu_id, &mask);
  }
  SharedMutex::WriteHolder w{&threadListLock_};
  if (cpuAffinityMask_ == nullptr) {
    cpuAffinityMask_ = std::make_unique<cpu_set_t>();
  }
  memcpy(cpuAffinityMask_.get(), &mask, sizeof(cpu_set_t));
  for (const auto& thread_ptr : threadList_.get()) {
    int32_t res = setThreadCpuAffinity(thread_ptr, cpuAffinityMask_.get());
    if (res != 0) {
      // Fail to set affinity
      LOG(ERROR) << "Fail to set cpu affinity for existing threads, errno="
                 << res;
      return res;
    }
  }
  return 0;
}

int32_t NumaThreadPoolExecutor::setThreadCpuAffinity(const ThreadPtr& t,
                                                     const cpu_set_t* mask) {
  pthread_t handle = t->handle.native_handle();
  return pthread_setaffinity_np(handle, sizeof(cpu_set_t), mask);
}

void NumaThreadPoolExecutor::joinStoppedThreads(size_t n) {
  for (size_t i = 0; i < n; i++) {
    auto thread = stoppedThreads_.take();
    thread->handle.join();
  }
}

void NumaThreadPoolExecutor::stop() {
  joinKeepAliveOnce();
  size_t n = 0;
  {
    SharedMutex::WriteHolder w{&threadListLock_};
    maxThreads_.store(0, std::memory_order_release);
    activeThreads_.store(0, std::memory_order_release);
    n = threadList_.get().size();
    removeThreads(n, false);
    n += threadsToJoin_.load(std::memory_order_relaxed);
    threadsToJoin_.store(0, std::memory_order_relaxed);
  }
  joinStoppedThreads(n);
  CHECK_EQ(0, threadList_.get().size());
  CHECK_EQ(0, stoppedThreads_.size());
}

void NumaThreadPoolExecutor::join() {
  joinKeepAliveOnce();
  size_t n = 0;
  {
    SharedMutex::WriteHolder w{&threadListLock_};
    maxThreads_.store(0, std::memory_order_release);
    activeThreads_.store(0, std::memory_order_release);
    n = threadList_.get().size();
    removeThreads(n, true);
    n += threadsToJoin_.load(std::memory_order_relaxed);
    threadsToJoin_.store(0, std::memory_order_relaxed);
  }
  joinStoppedThreads(n);
  CHECK_EQ(0, threadList_.get().size());
  CHECK_EQ(0, stoppedThreads_.size());
}

void NumaThreadPoolExecutor::withAll(
    folly::FunctionRef<void(NumaThreadPoolExecutor&)> f) {
  getSyncVecThreadPoolExecutors().withRLock([f](auto& tpes) {
    for (auto tpe : tpes) {
      f(*tpe);
    }
  });
}

NumaThreadPoolExecutor::PoolStats NumaThreadPoolExecutor::getPoolStats() const {
  const auto now = std::chrono::steady_clock::now();
  SharedMutex::ReadHolder r{&threadListLock_};
  NumaThreadPoolExecutor::PoolStats stats;
  size_t activeTasks = 0;
  size_t idleAlive = 0;
  for (const auto& thread : threadList_.get()) {
    if (thread->idle.load(std::memory_order_relaxed)) {
      const std::chrono::nanoseconds idleTime =
          now - thread->lastActiveTime.load(std::memory_order_relaxed);
      stats.maxIdleTime = std::max(stats.maxIdleTime, idleTime);
      idleAlive++;
    } else {
      activeTasks++;
    }
  }
  stats.pendingTaskCount = getPendingTaskCountImpl();
  stats.totalTaskCount = stats.pendingTaskCount + activeTasks;

  stats.threadCount = maxThreads_.load(std::memory_order_relaxed);
  stats.activeThreadCount =
      activeThreads_.load(std::memory_order_relaxed) - idleAlive;
  stats.idleThreadCount = stats.threadCount - stats.activeThreadCount;
  return stats;
}

size_t NumaThreadPoolExecutor::getPendingTaskCount() const {
  SharedMutex::ReadHolder r{&threadListLock_};
  return getPendingTaskCountImpl();
}

const std::string& NumaThreadPoolExecutor::getName() const {
  return namePrefix_;
}

std::string NumaThreadPoolExecutor::getNameHelper() const {
  auto ntf = dynamic_cast<folly::NamedThreadFactory*>(threadFactory_.get());
  if (ntf == nullptr) {
    return folly::demangle(typeid(*this).name()).toStdString();
  }
  return ntf->getNamePrefix();
}

std::atomic<uint64_t> NumaThreadPoolExecutor::Thread::nextId(0);

void NumaThreadPoolExecutor::subscribeToTaskStats(TaskStatsCallback cb) {
  if (*taskStatsCallbacks_->inCallback) {
    throw std::runtime_error("cannot subscribe in task stats callback");
  }
  taskStatsCallbacks_->callbackList.wlock()->push_back(std::move(cb));
}

folly::BlockingQueueAddResult NumaThreadPoolExecutor::StoppedThreadQueue::add(
    NumaThreadPoolExecutor::ThreadPtr item) {
  std::lock_guard<std::mutex> guard(mutex_);
  queue_.push(std::move(item));
  return sem_.post();
}

NumaThreadPoolExecutor::ThreadPtr
NumaThreadPoolExecutor::StoppedThreadQueue::take() {
  while (true) {
    {
      std::lock_guard<std::mutex> guard(mutex_);
      if (!queue_.empty()) {
        auto item = std::move(queue_.front());
        queue_.pop();
        return item;
      }
    }
    sem_.wait();
  }
}

folly::Optional<NumaThreadPoolExecutor::ThreadPtr>
NumaThreadPoolExecutor::StoppedThreadQueue::try_take_for(
    std::chrono::milliseconds time) {
  while (true) {
    {
      std::lock_guard<std::mutex> guard(mutex_);
      if (!queue_.empty()) {
        auto item = std::move(queue_.front());
        queue_.pop();
        return item;
      }
    }
    if (!sem_.try_wait_for(time)) {
      return folly::none;
    }
  }
}

size_t NumaThreadPoolExecutor::StoppedThreadQueue::size() {
  std::lock_guard<std::mutex> guard(mutex_);
  return queue_.size();
}

void NumaThreadPoolExecutor::addObserver(std::shared_ptr<Observer> o) {
  {
    SharedMutex::WriteHolder r{&threadListLock_};
    observers_.push_back(o);
    for (auto& thread : threadList_.get()) {
      o->threadPreviouslyStarted(thread.get());
    }
  }
  while (activeThreads_.load(std::memory_order_relaxed) <
         maxThreads_.load(std::memory_order_relaxed)) {
    ensureActiveThreads();
  }
}

void NumaThreadPoolExecutor::removeObserver(std::shared_ptr<Observer> o) {
  SharedMutex::WriteHolder r{&threadListLock_};
  for (auto& thread : threadList_.get()) {
    o->threadNotYetStopped(thread.get());
  }

  for (auto it = observers_.begin(); it != observers_.end(); it++) {
    if (*it == o) {
      observers_.erase(it);
      return;
    }
  }
  DCHECK(false);
}

// Idle threads may have destroyed themselves, attempt to join
// them here
void NumaThreadPoolExecutor::ensureJoined() {
  auto tojoin = threadsToJoin_.load(std::memory_order_relaxed);
  if (tojoin) {
    {
      SharedMutex::WriteHolder w{&threadListLock_};
      tojoin = threadsToJoin_.load(std::memory_order_relaxed);
      threadsToJoin_.store(0, std::memory_order_relaxed);
    }
    joinStoppedThreads(tojoin);
  }
}

// threadListLock_ must be write locked.
bool NumaThreadPoolExecutor::tryTimeoutThread() {
  // Try to stop based on idle thread timeout (try_take_for),
  // if there are at least minThreads running.
  if (!minActive()) {
    return false;
  }

  // Remove thread from active count
  activeThreads_.store(activeThreads_.load(std::memory_order_relaxed) - 1,
                       std::memory_order_relaxed);

  // There is a memory ordering constraint w.r.t the queue
  // implementation's add() and getPendingTaskCountImpl() - while many
  // queues have seq_cst ordering, some do not, so add an explicit
  // barrier.  tryTimeoutThread is the slow path and only happens once
  // every thread timeout; use asymmetric barrier to keep add() fast.
  folly::asymmetricHeavyBarrier();

  // If this is based on idle thread timeout, then
  // adjust vars appropriately (otherwise stop() or join()
  // does this).
  if (getPendingTaskCountImpl() > 0) {
    // There are still pending tasks, we can't stop yet.
    // re-up active threads and return.
    activeThreads_.store(activeThreads_.load(std::memory_order_relaxed) + 1,
                         std::memory_order_relaxed);
    return false;
  }

  threadsToJoin_.store(threadsToJoin_.load(std::memory_order_relaxed) + 1,
                       std::memory_order_relaxed);

  return true;
}

// If we can't ensure that we were able to hand off a task to a thread,
// attempt to start a thread that handled the task, if we aren't already
// running the maximum number of threads.
void NumaThreadPoolExecutor::ensureActiveThreads() {
  ensureJoined();

  // Matches barrier in tryTimeoutThread().  Ensure task added
  // is seen before loading activeThreads_ below.
  folly::asymmetricLightBarrier();

  // Fast path assuming we are already at max threads.
  auto active = activeThreads_.load(std::memory_order_relaxed);
  auto total = maxThreads_.load(std::memory_order_relaxed);

  if (active >= total) {
    return;
  }

  SharedMutex::WriteHolder w{&threadListLock_};
  // Double check behind lock.
  active = activeThreads_.load(std::memory_order_relaxed);
  total = maxThreads_.load(std::memory_order_relaxed);
  if (active >= total) {
    return;
  }
  NumaThreadPoolExecutor::addThreads(1);
  activeThreads_.store(active + 1, std::memory_order_relaxed);
}

// If an idle thread times out, only join it if there are at least
// minThreads threads.
bool NumaThreadPoolExecutor::minActive() {
  return activeThreads_.load(std::memory_order_relaxed) >
         minThreads_.load(std::memory_order_relaxed);
}

}  // namespace mtcache
