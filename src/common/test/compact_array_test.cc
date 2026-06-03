// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/compact_array.h"

#include <gtest/gtest.h>

namespace bcache2 {
namespace test {

TEST(CompactArray, Trivially) {
    struct Foo {
        int reference = 0;
        int integer = 0;
        bool boolean = false;
    };
    static_assert(std::is_trivially_copyable<Foo>::value);

    CompactArray<Foo> array;
    ASSERT_EQ(array.Size(), 0);
    ASSERT_TRUE(array.Empty());

    for (size_t i = 1; i <= 100; ++i) {
        Foo foo;
        foo.integer = i;
        foo.boolean = i % 2 == 0;
        array.PushBack(std::move(foo));
        ASSERT_EQ(array.Size(), i);
    }

    for (size_t i = 0; i < array.Size(); ++i) {
        printf("Foo[%zu] = {%d, %s}\n", i, array[i].integer, array[i].boolean ? "true" : "false");
    }

    std::swap(array[0], array.Back());
    ASSERT_EQ(array[0].integer, 100);
    ASSERT_EQ(array.Back().integer, 1);
    array.PopBack();
    ASSERT_EQ(array.Size(), 99);
    for (size_t i = 0; i < array.Size(); ++i) {
        ASSERT_NE(array[i].integer, 1);
    }
    while (!array.Empty()) {
        array.PopBack();
    }
    ASSERT_EQ(array.Size(), 0);
    ASSERT_TRUE(array.Empty());
}

TEST(CompactArray, NonTrivially) {
    static_assert(!std::is_trivially_copyable<std::vector<std::string>>::value);
    CompactArray<std::vector<std::string>> array;
    ASSERT_EQ(array.Size(), 0);
    ASSERT_TRUE(array.Empty());

    for (size_t i = 1; i <= 100; ++i) {
        std::vector<std::string> vec;
        vec.emplace_back(std::to_string(i));
        array.PushBack(std::move(vec));
        ASSERT_EQ(array.Size(), i);
    }

    for (size_t i = 0; i < array.Size(); ++i) {
        printf("Array[%zu] = {%s}\n", i, array[i].front().c_str());
    }

    std::swap(array[0], array.Back());
    ASSERT_EQ(array[0], std::vector<std::string>{"100"});
    ASSERT_EQ(array.Back(), std::vector<std::string>{"1"});
    array.PopBack();
    ASSERT_EQ(array.Size(), 99);
    for (size_t i = 0; i < array.Size(); ++i) {
        ASSERT_NE(array[i], std::vector<std::string>{"1"});
    }
    while (!array.Empty()) {
        array.PopBack();
    }
    ASSERT_EQ(array.Size(), 0);
    ASSERT_TRUE(array.Empty());
}

TEST(CompactArray, Clear) {
    CompactArray<int> array;
    ASSERT_EQ(array.Size(), 0);
    ASSERT_TRUE(array.Empty());

    for (size_t i = 1; i <= 100; ++i) {
        array.PushBack(i);
        ASSERT_EQ(array.Size(), i);
    }

    array.Clear();
    ASSERT_EQ(array.Size(), 0);
    ASSERT_TRUE(array.Empty());
}

struct Foo {
    Foo() { ++counter; }
    ~Foo() { --counter; }

    static size_t counter;
};
size_t Foo::counter = 0;

TEST(CompactArray, ConstructAndDestroy) {
    {
        CompactArray<Foo> array;
        array.Resize(10);
        ASSERT_EQ(Foo::counter, 10);
        array.Clear();
        ASSERT_EQ(Foo::counter, 0);
    }

    {
        CompactArray<Foo> array;
        array.Resize(10);
    }
    ASSERT_EQ(Foo::counter, 0);
}

}  // namespace test
}  // namespace bcache2
