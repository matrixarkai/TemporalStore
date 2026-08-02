#pragma once

#include "storage_engine.h"

namespace mtcache {

// StorageEngineSSD defines the storage engine that stores data in SSD.
// Use TerarkDB as storage engine.

class StorageEngineSSD : public StorageEngine {
 public:
  // Constructor instantiate a PMEM storage engine
  StorageEngineSSD() {}

  // Default destructor
  ~StorageEngineSSD() override {}

  virtual std::string path() = 0;
};

}  // namespace mtcache
