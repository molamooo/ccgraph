#pragma once
#include "step.hpp"
#include "exceptions.hpp"

#include <atomic>
#include <vector>

class Query;
/**
 * Each query would store a vector pointing to each step. deleted after query
 * is done.
 * 
 */

class QueryStep {
 private:
  Query* _query;
  std::atomic_bool _started;
  std::atomic_bool _done;
  std::atomic_flag _latch;
 public:
  bool is_started()  { return _started.load(); }
  bool is_done()     { return    _done.load(); }
  void set_started() { _started.store(true); }
  void set_done()    {    _done.store(true); }

  void acquire_latch() { while(_latch.test_and_set()); }
  void release_latch() { _latch.clear(); }

  virtual OperatorType get_type() = 0;
  virtual int get_prev_count() = 0;
  virtual int get_next_count() = 0;
  virtual QueryStep* get_prev(const int idx) = 0;
  virtual QueryStep* get_next(const int idx) = 0;

  int get_int_rst() { throw QueryException("Not a int rst step."); }
};

class LeafStep {
 public:
  int get_prev_count() { return 0; }
  QueryStep* get_prev(const int) {throw QueryException("No prev for leaf step."); };
};

class NoOpStep : public QueryStep {
 private:
  std::vector<QueryStep*> _prevs, _nexts;
 public:
  OperatorType get_type() { return OperatorType::kNoOp; };
  int get_prev_count() { return _prevs.size(); }
  int get_next_count() { return _nexts.size(); }
  QueryStep* get_prev(const int idx) { return _prevs.at(idx); }
  QueryStep* get_next(const int idx) { return _nexts.at(idx); }
};

class IntOpStep : public QueryStep {
 public:
  enum IntOpType {
    // binary
    kAdd = 0,
    kSub,
    kMul,
    kDiv,
    kMod,
    kEq,
    KNe,
    kGe,
    kG,
    kLe,
    kL,
    // unary
    // kNeg,
  };
 private:
  QueryStep* _prevs[2];
  std::vector<QueryStep*> _nexts;
 public:
  int _rst;
  IntOpType _optype;
  bool is_unary() { return _optype >= kNeg; }
  OperatorType get_type() { return OperatorType::kIntOp; };
  int get_prev_count() { return (_optype >= kNeg) ? 1 : 2; }
  int get_next_count() { return _nexts.size(); }
  QueryStep* get_prev(const int idx) { return _prevs[idx]; }
  QueryStep* get_next(const int idx) { return _nexts.at(idx); }
  int get_int_rst() { return _rst; }
};

// class IntStep : public QueryStep {
//  private:
//   int _val;
//  public:
//   OperatorType get_type() { return OperatorType::kIntRst; };
//   int get_val() { return _val; }
//   void set_val(const int v) { _val = v; }
// };

class IfStep : public QueryStep {
 private:
  QueryStep* _cond_step;
 public:
  QueryStep *_end_if_step, *_true_branch, *_false_branch;
  OperatorType get_type() { return OperatorType::kIf; };
  int get_prev_count() { return 1; }
  int get_next_count() { return 1; }
  QueryStep* get_end_if() { return _end_if_step; }
  QueryStep* get_prev(const int idx) { return _cond_step; }
  QueryStep* get_next(const int idx) { return (_cond_step->get_int_rst()) ? _true_branch : _false_branch; }
};

class EndIfStep : public QueryStep {
 private:
  QueryStep *_if_step, *_prev; // _prev is set in runtime.
  std::vector<QueryStep*> _nexts;
 public:
  OperatorType get_type() { return OperatorType::kEndIf; };
  int get_prev_count() { return 1; }
  int get_next_count() { return _nexts.size(); }
  void set_prev(QueryStep* p) { _prev = p; }
  QueryStep* get_prev(const int idx) { return _prev; }
  QueryStep* get_next(const int idx) { return _nexts.at(idx); }

};