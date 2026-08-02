// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <iostream>
#include <memory>
#include <string>

#include "client/client.h"

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cout << "usage ./client_example bcache2.metaserver.smoke.service lq ns table"
                    << std::endl;
        return 0;
    }

    std::unique_ptr<bcache2::client::Client> client;
    bcache2::client::ClientOptions options;
    bcache2::Status status;
    options.log_level = bcache2::client::LogLevel::kAll;
    options.af = bcache2::client::AddressFamily::kIp4;
    std::string master = argv[1];
    if (master.find(':') != std::string::npos) {
        options.master_addr = master;
    } else {
        options.master_consul = master;
    }
    options.idc = argv[2];
    options.host = "127.0.0.1";
    options.psm = "example.test";
    bcache2::client::Client* temp_client;
    status = bcache2::client::Client::Create(options, &temp_client);
    if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
        return 1;
    }
    client.reset(temp_client);

    std::unique_ptr<bcache2::client::Table> table;
    bcache2::client::TableOptions table_options;
    bcache2::client::Table* temp_table = nullptr;
    status = client->OpenTable(argv[3], argv[4], table_options, &temp_table);
    if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
        return 1;
    }
    table.reset(temp_table);

    status = table->HSet("hinata_key", "hinata_field", "hinata_value");
    if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
        return 1;
    }

    std::string value;
    status = table->HGet("hinata_key", "hinata_field", &value);
    if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
        return 1;
    }
    std::cout << value << std::endl;
    bcache2::client::Pipeline* temp_pipeline;
    status = table->OpenPipeline(&temp_pipeline);
    if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
        return 1;
    }
    std::unique_ptr<bcache2::client::Pipeline> pipeline;
    pipeline.reset(temp_pipeline);
    std::vector<bcache2::Status> statusArray;
    pipeline->HSet("zhangfucheng_key", "zhangfucheng_field", "zhangfucheng_value");

    pipeline->HGet("zhangfucheng_key", "zhangfucheng_field", &value);
    statusArray = pipeline->Sync();
    if (!statusArray[0].ok() || !statusArray[1].ok()) {
        std::cout << status.ToString() << std::endl;
        return 1;
    }
    std::cout << value << std::endl;
    status = client->CloseTable(table.get());
    if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
        return 1;
    }
    return 0;
}
