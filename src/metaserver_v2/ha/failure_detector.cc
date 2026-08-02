// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/ha/failure_detector.h"

#include <mutex>
#include <ostream>
#include <utility>

#include "common/logging.h"
#include "metaserver_v2/flags.h"

namespace bcache2 {
namespace metaserver {

constexpr size_t FailureDetector::kSampleCapacity;
constexpr int64_t FailureDetector::kMaxInterpretPauseTimeUs;

void FailureDetector::Report(const Endpoint& ep, int64_t arrival_timepoint) {
    std::lock_guard<bthread::Mutex> _(mu_);
    auto iter = arrival_samples_.find(ep);
    if (iter == arrival_samples_.end()) {
        iter = arrival_samples_.emplace(std::make_pair(ep, FailureDetector::kSampleCapacity)).first;
    }
    ArrivalWindow& window = iter->second;
    window.Add(arrival_timepoint);
    last_report_timepoint_ = arrival_timepoint;
}

FailureDetector::Diagnose FailureDetector::Interpret(const Endpoint& ep, int64_t now) {
    std::lock_guard<bthread::Mutex> _(mu_);

    auto iter = arrival_samples_.find(ep);
    if (iter == arrival_samples_.end()) {
        // TODO(wuzhenyu) THINK AGAIN
        return Diagnose::kNotExists;
    }
    int64_t diff = now - last_interpret_timepoint_;
    last_interpret_timepoint_ = now;
    if (diff > kMaxInterpretPauseTimeUs) {
        // pause for a while to avoid false negitave
        last_pause_timepoint_ = now;
        return Diagnose::kUnknown;
    }
    if (now < last_pause_timepoint_ + kMaxInterpretPauseTimeUs) {
        // conitnue to pause for a while
        return Diagnose::kUnknown;
    }
    ArrivalWindow& window = iter->second;
    double phi = window.CalculatePhi(now);
    LOG_CALL_DEBUG().put("endpoint", ep).put("phi", phi).put("v", phi * kPhiFactor);
    if (kPhiFactor * phi > GetPhiFailureThreshold()) {
        LOG_INFO("reach failure threshold").put("now", now).put("endpoint", ep).put("phi", phi);
        return Diagnose::kFailure;
    }
    return Diagnose::kNormal;
}

void FailureDetector::Remove(const Endpoint& ep) {
    std::lock_guard<bthread::Mutex> _(mu_);
    arrival_samples_.erase(ep);
}

int64_t FailureDetector::GetLastReportTimepoint() { return last_report_timepoint_; }

double FailureDetector::GetPhiFailureThreshold() {
    // default: 8
    return FLAGS_metaserver_phi_failure_threshold;
}

double FailureDetector::GetInterpretPauseTime() {
    return FLAGS_metaserver_phi_interpret_pause_time_us;
}

FailureDetector::ArrivalWindow::ArrivalWindow(size_t cap) : stats_(cap) { CHECK_GT(cap, 0ULL); }

void FailureDetector::ArrivalWindow::Add(int64_t arrival_timepoint) {
    if (last_timepoint_ == 0) {
        stats_.Add(kInitialValueUs);
    } else {
        int64_t interval = arrival_timepoint - last_timepoint_;
        if (interval > 0 && interval <= kMaxIntervalUs) {
            stats_.Add(interval);
        } else {
            LOG_WARNING("illegal interval")
                .put("last", last_timepoint_)
                .put("arrival", arrival_timepoint);
        }
    }
    last_timepoint_ = arrival_timepoint;
}

double FailureDetector::ArrivalWindow::Mean() const { return stats_.Mean(); }

double FailureDetector::ArrivalWindow::CalculatePhi(int64_t now) {
    double mean = Mean();
    if (last_timepoint_ == 0) {
        return .0;
    }
    CHECK(mean > .0) << this;
    if (now <= last_timepoint_) {
        LOG_WARNING("time skew due to concurrency, return last phi")
            .put("now", now)
            .put("last_report", last_timepoint_);
        return last_reported_phi_;
    }
    int64_t delta = now - last_timepoint_;
    last_reported_phi_ = delta / mean;
    return last_reported_phi_;
}

double FailureDetector::ArrivalWindow::LastReportedPhi() const { return last_reported_phi_; }

FailureDetector::ArrivalWindow::Stats::Stats(size_t cap) : intervals_(cap) { CHECK_GT(cap, 0ULL); }

void FailureDetector::ArrivalWindow::Stats::Add(int64_t interval) {
    CHECK_GT(interval, 0);
    if (cursor_ == intervals_.size()) {
        cursor_ = 0;
        filled_ = true;
    }
    if (filled_) {
        sum_ -= intervals_[cursor_];
    }
    intervals_[cursor_++] = interval;
    sum_ += interval;
    mean_ = static_cast<double>(sum_) / (filled_ ? intervals_.size() : cursor_);
}

std::ostream& FailureDetector::ArrivalWindow::Stats::descr(std::ostream& os) const {
    os << "sum:" << sum_ << " mean:" << mean_ << " v:[ ";
    int size = filled_ ? intervals_.size() : cursor_;
    for (int i = 0; i < size; i++) {
        os << intervals_[(i + cursor_) % intervals_.size()] << " ";
    }
    return os << "]";
}

std::ostream& FailureDetector::ArrivalWindow::descr(std::ostream& os) const {
    return stats_.descr(os);
}

}  // namespace metaserver
}  // namespace bcache2

