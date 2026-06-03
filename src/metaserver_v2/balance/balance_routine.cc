// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/balance/balance_routine.h"

#include <map>
#include <string>
#include <vector>

#include "butil/time.h"
#include "common/logging.h"
#include "common/status.h"
#include "metaserver_v2/flags.h"
#include "metaserver_v2/meta/namespace.h"
#include "metaserver_v2/meta/proxy.h"
#include "metaserver_v2/meta/table.h"

namespace bcache2 {
namespace metaserver {

BalanceRoutine::BalanceRoutine() {}
BalanceRoutine::~BalanceRoutine() { Stop(); }

Status BalanceRoutine::Start(const Options& opts) {
    if (running_) {
        return Status::Internal("already running");
    }
    running_ = true;

    metabase_ = opts.metabase;
    scheduler_mgr_ = opts.scheduler_manager;

    int rc = bthread_start_background(&routine_thd_, nullptr, RunRoutine, this);
    if (rc != 0) {
        running_ = false;
        return Status::Internal("failed to start background bthread");
    }
    return Status::OK();
}

void BalanceRoutine::Stop() {
    if (!running_) {
        return;
    }
    running_ = false;
    bthread_stop(routine_thd_);
    bthread_join(routine_thd_, nullptr);
}

void* BalanceRoutine::RunRoutine(void* arg) {
    auto ins = static_cast<BalanceRoutine*>(arg);
    ins->Routine();
    return nullptr;
}

void BalanceRoutine::Routine() {
    LOG_INFO("enter routine");
    while (running_) {
        bthread_usleep(FLAGS_metaserver_balance_routine_interval_ms * 1000);
        if (!FLAGS_metaserver_balance_enabled) {
            continue;
        }

        BalanceByTable();
    }
    LOG_INFO("exiting routine");
}

/// the most simple strategy
void BalanceRoutine::BalanceByTable() {
    NamespaceManager* ns_mgr = metabase_->GetNamespaceManager();
    std::vector<NamespacePtr> ns_list = ns_mgr->List();
    for (auto ns : ns_list) {
        std::vector<TablePtr> tables = ns->List();
        for (auto table : tables) {
            if (table->GetState() != TableState::TABLE_NORMAL) {
                continue;
            }

            const TableInfo& info = table->GetInfo();
            for (auto& unit : info.partition_units()) {
                if (!FLAGS_metaserver_balance_enabled) {
                    continue;
                }
                scheduler_mgr_->BalanceTable(table, unit);
            }
        }  // for tables
    }
}

}  // namespace metaserver
}  // namespace bcache2
