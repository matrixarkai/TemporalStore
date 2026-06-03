// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <unordered_map>

#include "gtest/gtest.h"

#include "common/proto_enhance.h"

namespace bcache2::common::test {

TEST(ProtoEnhanceTest, EndpointHash) {
    {
        std::unordered_map<Endpoint, int, EndpointHash> m;
        Endpoint ep1;
        ep1.set_addr_family(Endpoint::ADDR_V6);
        ep1.set_port(1);
        ep1.set_ip6("fdbd:dc02:24:140::21");
        m[ep1] = 1;

        Endpoint ep2;
        ep2.CopyFrom(ep1);
        ep2.set_ip6("fdbd:dc02:24:140::23");
        auto iter = m.find(ep2);
        ASSERT_TRUE(iter == m.end());
        ASSERT_NE(ep1, ep2);
    }
    {
        Endpoint ep1;
        ep1.set_port(1);
        Endpoint ep2;
        ep2.CopyFrom(ep1);
        ASSERT_EQ(ep1, ep2);

        ep1.set_ip4("123");
        ep2.set_ip4("123");
        ep2.set_ip6("123");
        ASSERT_NE(ep1, ep2);
        ep1.clear_ip4();
        ep2.clear_ip4();
        ASSERT_NE(ep1, ep2);
        ep1.set_ip6("432");
        ASSERT_NE(ep1, ep2);
        ep1.set_ip6("123");
        ASSERT_EQ(ep1, ep2);
    }
}

TEST(ProtoEnhanceTest, EndpointConvert) {
    Endpoint ep;
    ep.set_addr_family(Endpoint::ADDR_DUAL_STACK);
    ep.set_port(65535);
    ep.set_ip4("255.255.255.255");
    ep.set_ip6("ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255");
    {
        Endpoint2 ep2;
        Status status = ToEndpoint2(ep, &ep2);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_EQ(ep.addr_family(), ep2.addr_family());
        ASSERT_EQ(ep.port(), ep2.port());
        ASSERT_EQ(ep2.be32_addr_size(), 5);
        Endpoint ep_bk;
        status = ToEndpoint(ep2, &ep_bk);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_EQ(ep.addr_family(), ep_bk.addr_family());
        ASSERT_EQ(ep.port(), ep_bk.port());
        ASSERT_EQ(ep.ip4(), ep_bk.ip4());
        if (ep.ip6() != ep_bk.ip6() && ep_bk.ip6() != "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff") {
            ASSERT_TRUE(false);
        }
    }
    ep.set_ip4("355.255.255.255");
    {
        Endpoint2 ep2;
        Status status = ToEndpoint2(ep, &ep2);
        ASSERT_TRUE(!status.ok());
    }

    ep.set_addr_family(Endpoint::ADDR_V6);
    ep.clear_ip4();
    ep.set_ip6("fdbd:dc03:14:316::135");
    {
        Endpoint2 ep2;
        Status status = ToEndpoint2(ep, &ep2);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_EQ(ep.addr_family(), ep2.addr_family());
        ASSERT_EQ(ep.port(), ep2.port());
        ASSERT_EQ(ep2.be32_addr_size(), 4);
        Endpoint ep_bk;
        status = ToEndpoint(ep2, &ep_bk);
        ASSERT_TRUE(status.ok()) << status.ToString();
        ASSERT_EQ(ep.addr_family(), ep_bk.addr_family());
        ASSERT_EQ(ep.port(), ep_bk.port());
        ASSERT_EQ(ep.ip4(), ep_bk.ip4());
        ASSERT_EQ(ep.ip6(), ep_bk.ip6());
    }
    ep.set_ip6("ffff:ffff:ffff:ffff:ffff:ffff::x");
    {
        Endpoint2 ep2;
        Status status = ToEndpoint2(ep, &ep2);
        ASSERT_TRUE(!status.ok());
    }
}

TEST(LocationTest, BelongsToTest) {
    Location loc1;
    loc1.set_vregion("cn");
    loc1.set_vdc("vdc1");
    loc1.set_vau("vau1");
    {
        Location loc2 = loc1;
        ASSERT_TRUE(BelongsTo(loc2, loc1));
        loc2.clear_vau();
        ASSERT_FALSE(BelongsTo(loc2, loc1));
        ASSERT_TRUE(BelongsTo(loc1, loc2));
        loc2.clear_vdc();
        ASSERT_FALSE(BelongsTo(loc2, loc1));
        ASSERT_TRUE(BelongsTo(loc1, loc2));
        loc2.set_tag("fasd");
        ASSERT_FALSE(BelongsTo(loc1, loc2));
        loc1.set_tag("fasd");
        loc2.clear_tag();
        ASSERT_FALSE(BelongsTo(loc1, loc2));
        loc1.clear_tag();
    }
}

}  // namespace bcache2::common::test
