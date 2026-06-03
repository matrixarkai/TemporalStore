// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/allocator.h"

#include <gtest/gtest.h>

#include <list>

namespace bcache2 {
namespace test {

class AllocatorTest : public testing::Test {
 public:
    void SetUp() override {}

    void TearDown() override {}

 protected:
    Allocator allocator;
};

TEST_F(AllocatorTest, Container) {
    struct Foo {
        int value = 0;
        bool boolean = false;

        Foo(int a, bool b) : value(a), boolean(b) {}
    };

    {
        // empty wrapper, so we expect death
        std::list<Foo, Allocator::StlWrapper<Foo>> list;
        ASSERT_DEATH(list.push_back(Foo(1, false)), "");
    }

    // test alloc, rebind and stats
    Allocator::StlWrapper<Foo> wrapper(&allocator);
    std::list<Foo, Allocator::StlWrapper<Foo>> list(wrapper);
    list.push_back(Foo(10, false));
    list.push_back(Foo(10, false));
    list.pop_back();
    list.push_back(Foo(10, false));
    list.push_back(Foo(10, false));
    ASSERT_GE(list.get_allocator().Impl()->GetStats().alloc_cnt(), 4);
    ASSERT_GE(list.get_allocator().Impl()->GetStats().dealloc_cnt(), 1);
    ASSERT_EQ(list.get_allocator().Impl(), wrapper.Impl());

    // test copy elements and copy allocator
    std::list<Foo, Allocator::StlWrapper<Foo>> list2 = list;
    list2.push_back(Foo(10, false));
    ASSERT_GE(list2.get_allocator().Impl()->GetStats().alloc_cnt(), 5);
    ASSERT_EQ(list2.get_allocator().Impl(), wrapper.Impl());

    // test move elements and move allocator
    std::list<Foo, Allocator::StlWrapper<Foo>> list3 = std::move(list);
    list3.push_back(Foo(10, false));
    ASSERT_GE(list3.get_allocator().Impl()->GetStats().alloc_cnt(), 6);
    ASSERT_EQ(list3.get_allocator().Impl(), wrapper.Impl());

    // test swap elements and swap allocator
    std::list<Foo, Allocator::StlWrapper<Foo>> list4;
    list4.swap(list3);
    list4.push_back(Foo(10, false));
    ASSERT_GE(list4.get_allocator().Impl()->GetStats().alloc_cnt(), 7);
    ASSERT_EQ(list4.get_allocator().Impl(), wrapper.Impl());
}

}  // namespace test
}  // namespace bcache2
