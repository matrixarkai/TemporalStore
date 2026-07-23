// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <byte/include/macros.h>
#include <gtest/gtest.h>
#include <stdio.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "model/cpc/cpc_union.h"

namespace bcache2 {
namespace swig {

class RiskCPCModuleTest : public ::testing::Test {};

TEST_F(RiskCPCModuleTest, RiskCPCTest) {
    // 该值越大，耗时越大，越精确,占用的内存也越大，2的1g_k次方字节
    const int lg_k = 12;
    datasketches::cpc_sketch sketch1(lg_k);
    for (int key = 0; key < 10000; key++) sketch1.update(key);
    datasketches::cpc_sketch sketch2(lg_k);
    for (int key = 5000; key < 15000; key++) sketch2.update(key);

    datasketches::cpc_sketch sketch3(lg_k);
    for (int key = 10000; key < 40000; key++) sketch3.update(key);
    auto start = std::chrono::high_resolution_clock::now();
    datasketches::cpc_union u(lg_k);
    for (int i = 0; i < 60; i++) {
        u.update(sketch1);
        u.update(sketch2);
        u.update(sketch3);
    }

    datasketches::cpc_sketch sketch = u.get_result();
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    printf("Distinct count time cost is %lu microseconds\n", duration.count());
    std::cout << "Distinct count estimate: " << sketch.get_estimate() << std::endl;
    std::cout << "Distinct count lower bound 95% confidence: " << sketch.get_lower_bound(2)
              << std::endl;
    std::cout << "Distinct count upper bound 95% confidence: " << sketch.get_upper_bound(2)
              << std::endl;

    std::string page;
    google::protobuf::io::StringOutputStream output(&page);
    google::protobuf::io::CodedOutputStream stream(&output);
    double d1 = 2.356;
    const char* kxp_tmp = reinterpret_cast<const char*>(&d1);
    stream.WriteRaw(kxp_tmp, sizeof(d1));

    // google::protobuf::io::ArrayInputStream input(page.second.data(), page.second.size());
    // google::protobuf::io::CodedInputStream stream(&input);
}

}  // namespace swig
}  // namespace bcache2
