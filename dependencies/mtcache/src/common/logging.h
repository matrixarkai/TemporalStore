#pragma once

#include "util/glog_advanced.h"

// Define VLOG levels.  We want display per-row info less than per-file which
// is less than per-query.  For now per-connection is the same as per-query.
#define VLOG_CONNECTION VLOG(1)
#define VLOG_RPC VLOG(8)
#define VLOG_QUERY VLOG(1)
#define VLOG_FILE VLOG(2)
#define VLOG_ROW VLOG(10)
#define VLOG_PROGRESS VLOG(2)
#define VLOG_BGTASK VLOG(1)

#define VLOG_CONNECTION_IS_ON VLOG_IS_ON(1)
#define VLOG_RPC_IS_ON VLOG_IS_ON(2)
#define VLOG_QUERY_IS_ON VLOG_IS_ON(1)
#define VLOG_FILE_IS_ON VLOG_IS_ON(2)
#define VLOG_ROW_IS_ON VLOG_IS_ON(3)
#define VLOG_PROGRESS_IS_ON VLOG_IS_ON(2)
