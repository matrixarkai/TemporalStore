// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/operator_tool.h"

#include "gtest/gtest.h"

namespace bcache2::operator_tool::test {

TEST(CMDBClientTest, DISABLED_Simple) {
    std::string cmdb_host = getenv("CMDB_HOST");
    std::string cmdb_jwt_uri = getenv("CMDB_JWT_URI");
    std::string cmdb_key = getenv("CMDB_KEY");
    CMDBClient client(cmdb_host, cmdb_jwt_uri, cmdb_key);
    Location loc;
    Status status = client.QueryHostLocation("10.159.81.94", &loc);
    ASSERT_TRUE(status.ok()) << status;
    std::cout << loc.ShortDebugString() << std::endl;
}

}  // namespace bcache2::operator_tool::test
