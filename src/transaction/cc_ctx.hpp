#pragma once

#include "type.hpp"
#include "node.hpp"
#include "edge.hpp"
#include <folly/futures/Future.h>
#include <unordered_map>

class CCContex {
 public:
  // true for exclusive, false for share
  std::unordered_map<uint64_t, bool> _lock_history;
  // for 2pl: read history is not placed here. only the original version of those record wrotten 
  std::unordered_map<internal_id_t, Node*> _org_nodes;
  std::unordered_map<std::tuple<label_t, internal_id_t, internal_id_t>, Edge*> _org_edges;
  uint64_t txn_id, timestamp;
  int Locked(const uint64_t hashed) {
    auto iter = _lock_history.find(hashed);
    if (iter == _lock_history.end()) {
      return 0;
    }
    // 2 for exclusive, 1 for share
    return (iter->second) ? 2 : 1;
  }
};