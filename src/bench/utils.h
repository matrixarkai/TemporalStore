// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <assert.h>
#include <math.h>

#include <random>

namespace bcache2 {
namespace bench {

// Returns a Zipf random variable in [1, n]
inline int Zipf(int n, double alpha = 1.0) {
    static std::random_device dev;
    static std::mt19937 rng(dev());

    static int first = 1;                // Static first time flag
    static double c = 0;                 // Normalization constant
    static double* sum_probs = nullptr;  // Pre-calculated sum of probabilities
    double z = 0.0;                      // Uniform random number (0 < z < 1)
    int zipf_value = 0;                  // Computed exponential value to be returned
    int i = 0;                           // Loop counter
    int low = 0, high = 0, mid = 0;      // Binary-search bounds

    // Compute normalization constant on first call only
    if (first == 1) {
        for (i = 1; i <= n; i++) c = c + (1.0 / pow(static_cast<double>(i), alpha));
        c = 1.0 / c;

        sum_probs = static_cast<double*>(malloc((n + 1) * sizeof(*sum_probs)));
        sum_probs[0] = 0;
        for (i = 1; i <= n; i++) {
            sum_probs[i] = sum_probs[i - 1] + c / pow(static_cast<double>(i), alpha);
        }
        first = 0;
    }

    // Pull a uniform random number (0 < z < 1)
    do {
        z = std::uniform_real_distribution<>(0.0, 1.0)(rng);
    } while ((z == 0) || (z == 1));

    // Map z to the value
    low = 1, high = n;
    do {
        mid = floor((low + high) / 2);
        if (sum_probs[mid] >= z && sum_probs[mid - 1] < z) {
            zipf_value = mid;
            break;
        } else if (sum_probs[mid] >= z) {
            high = mid - 1;
        } else {
            low = mid + 1;
        }
    } while (low <= high);

    // Assert that zipf_value is between 1 and N
    assert((zipf_value >= 1) && (zipf_value <= n));

    return (zipf_value);
}

}  // namespace bench
}  // namespace bcache2
