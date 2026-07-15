#pragma once

#include "client/temporalstore_client.h"

namespace temporalstore {

using Client = bcache2::client::TemporalStoreClient;
using ClientOptions = bcache2::client::TemporalStoreClientOptions;
using FeatureFilter = bcache2::client::TemporalFeatureFilter;
using FeatureFilterOp = bcache2::client::TemporalFeatureFilterOp;
using FeaturePoint = bcache2::client::TemporalFeaturePoint;
using FeatureQuery = bcache2::client::TemporalFeatureQuery;
using FeatureWritePolicy = bcache2::client::TemporalFeatureWritePolicy;
using IpsFeatureStat = bcache2::client::IpsFeatureStat;
using IpsInstance = bcache2::client::IpsInstance;
using IpsLastQuery = bcache2::client::IpsLastQuery;
using ControlStatePrecision = bcache2::client::ControlStatePrecision;
using ControlStateWindow = bcache2::client::ControlStateWindow;
using ControlStateWindowUnit = bcache2::client::ControlStateWindowUnit;
using SequenceFeatureRow = bcache2::client::SequenceFeatureRow;
using Status = bcache2::Status;

}  // namespace temporalstore
