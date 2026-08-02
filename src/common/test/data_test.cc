// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/data.h"

#include <byte/include/assert.h>
#include <byte/include/macros.h>
#include <gtest/gtest.h>

#include <thread>  // NOLINT(build/c++11)

#include "common/sync_closure.h"

namespace bcache2 {

void Func(Data data) { ASSERT_EQ(10, data.size()); }

void Func2(Data* data) {
    char* d = new char[5]{'A', 'B', 'C', 'D', '\0'};
    Data data1(d, 5);
    ASSERT_STREQ("ABCD", data1.data());
    ASSERT_EQ(5, data1.size());
    *data = data1;
    ASSERT_EQ(nullptr, data1.data());
    ASSERT_EQ(0, data1.size());
}

TEST(DataTest, Simple) {
    char* d = new char[10];
    Data data1(d, 10);
    ASSERT_EQ(d, data1.data());
    ASSERT_EQ(10, data1.size());

    Data data2 = std::move(data1);
    ASSERT_EQ(nullptr, data1.data());
    ASSERT_EQ(0, data1.size());
    ASSERT_EQ(d, data2.data());
    ASSERT_EQ(10, data2.size());

    Data data3 = data2.Copy();
    ASSERT_NE(nullptr, data2.data());
    ASSERT_EQ(10, data2.size());
    ASSERT_NE(nullptr, data3.data());
    ASSERT_EQ(10, data3.size());

    Closure<void>* closure = NewClosure(&Func, data2);
    closure->Run();
    ASSERT_EQ(nullptr, data2.data());
    ASSERT_EQ(0, data2.size());
}

TEST(DataTest, Empty) {
    Data data1;
    ASSERT_EQ(nullptr, data1.data());
    ASSERT_EQ(0, data1.size());

    Data data2 = std::move(data1);
    ASSERT_EQ(nullptr, data2.data());
    ASSERT_EQ(0, data2.size());

    Data data3 = data2.Copy();
    ASSERT_EQ(nullptr, data3.data());
    ASSERT_EQ(0, data3.size());
}

TEST(DatTest, Vecotr) {
    std::vector<Data> v(2);
    ASSERT_EQ(nullptr, v[1].data());

    v[1] = Data(new char[10], 10);
    ASSERT_NE(nullptr, v[1].data());

    std::vector<Data> v2 = std::move(v);
    ASSERT_TRUE(v.empty());
    ASSERT_EQ(10, v2[1].size());
}

TEST(DataTest, ReturnFromFunc) {
    Data data;
    Func2(&data);
    ASSERT_STREQ("ABCD", data.data());
    ASSERT_EQ(5, data.size());
}

}  // namespace bcache2
