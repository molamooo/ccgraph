#pragma once
#include "query_step.hpp"
#include "query.hpp"
#include <memory>

class Worker {
 private:
  template<typename T>
  using sptr = std::shared_ptr<T>;
  void ProcNoOp(QueryStep* step) { }
  void ProcIf(QueryStep* step) {
    auto if_step = dynamic_cast<IfStep*>(step);
    auto end_if_step = dynamic_cast<EndIfStep*>(if_step->get_end_if());
    if (if_step->get_prev(0)->get_int_rst()) {
      end_if_step->set_prev(if_step->_true_branch);
    } else {
      end_if_step->set_prev(if_step->_false_branch);
    }
  }
  void ProcIntOp(QueryStep* _step_) {
    auto step = dynamic_cast<IntOpStep*>(_step_);
    int prev1 = step->get_prev(0)->get_int_rst(),
        prev2 = step->get_prev(1)->get_int_rst();
    switch(step->_optype) {
      case IntOpStep::kAdd: step->_rst = prev1 + prev2; break;
      case IntOpStep::kSub: step->_rst = prev1 - prev2; break;
      case IntOpStep::kMul: step->_rst = prev1 * prev2; break;
      case IntOpStep::kDiv: step->_rst = prev1 / prev2; break;
      case IntOpStep::kMod: step->_rst = prev1 % prev2; break;
      case IntOpStep::kEq:  step->_rst = (prev1 == prev2) ? 1 : 0; break;
      case IntOpStep::KNe:  step->_rst = (prev1 != prev2) ? 1 : 0; break;
      // case IntOpStep::kNeg: step->_rst = (prev1 != prev2) ? 1 : 0; break;
      case IntOpStep::kL:   step->_rst = (prev1  < prev2) ? 1 : 0; break;
      case IntOpStep::kLe:  step->_rst = (prev1 <= prev2) ? 1 : 0; break;
      case IntOpStep::kG:   step->_rst = (prev1  > prev2) ? 1 : 0; break;
      case IntOpStep::kGe:  step->_rst = (prev1 >= prev2) ? 1 : 0; break;
    }
  }
 public:
  void ProcessQuery(std::shared_ptr<Query> q) {
    // the initial point of processing a query, no reentrant
    QueryStep* start = q->get_first_step();
  }
  void ProcessStep(QueryStep* step) {
    step->set_started();
    step->get_prev_count();
    switch (step->get_type()) {
      case kNoOp: ProcNoOp(step); break;
      case kIntConst: break;
      case kNodeId: break;
      case kGetNode: 
      case kGetAllNeighbor:
      case kGetAllEdge:
      case kGetEdgeVia:
      case kCreateNode:
      case kUpdateNode:
      case kDeleteNode:
      case kCreateEdge:
      case kUpdateEdge:
      case kDeleteEdge:
      case kFilterVia:
      case kIntOp: ProcIntOp(step); break;
      case kIf: ProcIf(step); break;
      case kEndIf:
      case kMerge: ;
      // default: break;
    }
    step->set_done();
  }
};