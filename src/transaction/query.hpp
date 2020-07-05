#pragma once
#include "query_step.hpp"
#include <vector>

class CCContex; 

class Query {
 private:
  std::vector<QueryStep*> _step;
  uint64_t _ts;
  uint64_t _txnid;
  CCContex* _cc_ctx;
 public:
  QueryStep* get_first_step() {
    return _step[0];
  }
};