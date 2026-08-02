// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "model/model_manager.h"

#include "model/dummy_model.h"
#include "model/context_model.h"
#include "model/feature_model.h"
#include "model/hash_model.h"
#include "model/ips_model.h"
#include "model/raw_model.h"
#include "model/risk_cpc_model.h"
#include "model/risk_hash_model.h"
#include "model/string_model.h"
#include "model/timeseries_model.h"

namespace bcache2 {
namespace model {

ModelApi ModelManager::model_api_table_[UINT8_MAX];

// NOTE: DO NOT change value of model id
REGISTER_MODEL(StringModel, 1);
REGISTER_MODEL(FeatureModel, 2);
REGISTER_MODEL(HashModel, 3);
REGISTER_MODEL(TimeSeriesModel, 4);
REGISTER_MODEL(IpsModel, 5);
REGISTER_MODEL(RawModel, 6);
REGISTER_MODEL(RiskHashModel, 7);
REGISTER_MODEL(CPCModel, 8);
REGISTER_MODEL(ContextNodeModel, 9);
REGISTER_MODEL(ContextEventModel, 10);
REGISTER_MODEL(ContextIndexModel, 11);
REGISTER_MODEL(ContextAuditModel, 12);
REGISTER_MODEL(ContextDirtyModel, 13);
REGISTER_MODEL(ContextChildModel, 14);
REGISTER_MODEL(ContextEmbeddingModel, 15);
REGISTER_MODEL(ContextSummaryModel, 16);
REGISTER_MODEL(ContextCompressionModel, 17);
REGISTER_MODEL(ContextEntityModel, 18);
REGISTER_MODEL(DummyModel, 127);

}  // namespace model
}  // namespace bcache2
