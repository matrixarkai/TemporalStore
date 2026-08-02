#pragma once

#include <string>

namespace mtcache {

// AccessRecordType is an enum type definition for the AccessRecordCallback
// interface to track the access type of a cache operation.
enum class AccessRecordType : uint8_t {
  kPut = 1,     // Put access insert an buffer into the cache
  kGet = 2,     // Get access reads an buffer from the cache
  kDelete = 3,  // Delete access removes an buffer from the cache
  kMaxCode = 4
};

// AccessRecordCallback defines the interface to receive notification of a
// cache operation event: the access type (Put/Get/Delete) and the key of the
// related cache buffer.
// A class (e.g. L2ARC cache instance) could use cache access records from
// this interface to implement its cache replacement policy.
class AccessRecordCallback {
 public:
  virtual ~AccessRecordCallback() {}
  // OnAccess receive a notification for an access record with specified type
  // and key.
  virtual void OnAccess(AccessRecordType type, const std::string& key) = 0;
};

}  // namespace mtcache
