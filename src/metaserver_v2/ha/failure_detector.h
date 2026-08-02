// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <math.h>
#include <stdint.h>
#include <ostream>
#include <unordered_map>
#include <vector>

#include "bthread/mutex.h"
#include "butil/time.h"

#include "common/proto_enhance.h"
#include "common/status.h"
#include "protocol/base.pb.h"

namespace bcache2 {
namespace metaserver {

/// Phi failture detector, references:
///   * "The Phi Accrual Failure Detector" by Hayashibara
///   * Cassandra org.apache.cassandra.gms.FailureDetector
///   * Alchemy ConfigServer FailureDetector
class FailureDetector {
 public:
    enum class Diagnose { kUnknown, kNotExists, kNormal, kFailure };

 public:
    FailureDetector() = default;
    ~FailureDetector() = default;

    void Report(const Endpoint& ep, int64_t arrival_timepoint);
    Diagnose Interpret(const Endpoint& ep, int64_t timepoint);
    int64_t GetLastReportTimepoint();

    void Remove(const Endpoint& ep);

    double GetPhiFailureThreshold();
    double GetInterpretPauseTime();

 private:
    static constexpr size_t kSampleCapacity = 1000;
    /// defaults
    static constexpr int64_t kMaxInterpretPauseTimeUs = 10 * 1'000 * 1'000;  // 10s
    static constexpr double kPhiFactor = 0.43429448190325182765;

    class ArrivalWindow {
     public:
        explicit ArrivalWindow(size_t cap);
        ~ArrivalWindow() = default;

        void Add(int64_t arrival_timepoint);
        double Mean() const;
        double CalculatePhi(int64_t now);
        double LastReportedPhi() const;

        std::ostream& descr(std::ostream&) const;

     private:
        class Stats {
         public:
            explicit Stats(size_t cap);
            ~Stats() = default;

            void Add(int64_t interval);
            double Mean() const { return mean_; }

            std::ostream& descr(std::ostream&) const;

         private:
            uint64_t cursor_{0};
            double mean_{.0};
            int64_t sum_{0};
            bool filled_{false};
            std::vector<int64_t> intervals_;
        };

        static constexpr size_t kInitialValueUs = 30 * 1'000 * 1'000;  // 30s
        static constexpr int64_t kMaxIntervalUs = 60 * 1'000 * 1'000;  // 60s

     private:
        double last_reported_phi_{.0};
        int64_t last_timepoint_{0};
        Stats stats_;
    };

 private:
    bthread::Mutex mu_;
    int64_t last_report_timepoint_{0};
    int64_t last_interpret_timepoint_{0};
    int64_t last_pause_timepoint_{0};
    std::unordered_map<Endpoint, ArrivalWindow, EndpointHash> arrival_samples_;
};

}  // namespace metaserver
}  // namespace bcache2
