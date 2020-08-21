#pragma once
#include "step.hpp"
#include "exceptions.hpp"
#include "edge.hpp"
#include "node.hpp"
#include "result.hpp"
#include "consts.hpp"

#include <atomic>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <functional>


enum MathOp {
  kAdd = 0,
  kSub,
  kMul,
  kDiv,
  kMod,
  kMax,
  kMin,
  kTimeSubMin,
};

struct FilterCtx {
  ResultItem val;
  Result::ColumnType ty;
  CompareOp op;
  std::string col_alias_to_filter = "";
};


class Query;
class CCContex;
/**
 * Each query would store a vector pointing to each step. deleted after query
 * is done.
 * 
 */

class QueryStep {
 protected:
  // Query* _query;
  CCContex* _cc_ctx = nullptr;
  
  std::atomic_bool _started;
  std::atomic_bool _done;
  // std::atomic_flag _latch;
  // size_t _prev_done = 0;

  Result* _rst = nullptr;
  bool _move_prev_rst = false;


  std::vector<QueryStep*> _nexts;
  std::vector<QueryStep*> _prevs;
  std::vector<size_t> _src_cols, _cols_to_put;
 public:
  QueryStep() : _started(false), _done(false) {}
  void set_rst(Result* rst) { _rst = rst; }
  Result & get_rst() { return *_rst; }
  bool get_reuse_prev_rst() { return _move_prev_rst; }
  bool is_started()  { return _started.load(); }
  bool is_done()     { return    _done.load(); }
  void set_started() { _started.store(true); }
  void set_done()    {    _done.store(true); }

  // void acquire_latch() { while(_latch.test_and_set()); }
  // void release_latch() { _latch.clear(); }

  // size_t inc_prev_done() { return ++_prev_done; }
  // size_t get_prev_done() { return _prev_done; }

  virtual OperatorType get_type() = 0;

  virtual size_t get_prev_count() = 0;
  QueryStep* get_prev(const size_t idx) { return _prevs[idx]; }
  virtual void set_prev(std::vector<QueryStep*> prevs) {
    if (prevs.size() != get_prev_count()) throw QueryException("prev size mismatch");
    _prevs = prevs;
  }

  CCContex* get_cc_ctx() const { return _cc_ctx; }
  void set_cc_ctx(CCContex* ctx) { _cc_ctx = ctx; }

  label_t get_label() const { throw QueryException("Not inherited get_label"); }
  void    set_label(label_t) { throw QueryException("Not inherited set_label"); }

  dir_t get_dir() const { throw QueryException("Not inherited get_dir"); }
  void  set_dir(dir_t) { throw QueryException("Not inherited set_dir"); }

  size_t get_src_col(const size_t idx) { return _src_cols[idx]; }
  size_t  get_src_col() { return get_src_col(0); }
  void set_src_col(std::vector<size_t> src_cols) {
    if (src_cols.size() != get_prev_count()) throw QueryException("prev size mismatch with src col size");
    _src_cols = src_cols;
  }

  virtual size_t get_dst_col_count() = 0;
  virtual size_t get_col_to_put(const size_t idx) { 
    if (get_dst_col_count() <= idx)  throw QueryException("get col_to_put out of boundry"); 
    return _cols_to_put[idx];
  }
  size_t  get_col_to_put() { return get_col_to_put(0); }
  void set_col_to_put(std::vector<size_t> cols_to_put) {
    if (cols_to_put.size() != get_dst_col_count()) throw QueryException("cols_to_put size mismatch");
    _cols_to_put = cols_to_put;
  }

  virtual inline size_t get_next_count() { return _nexts.size(); }
  virtual QueryStep* get_next(const size_t idx) { 
    if (_nexts.size() <= idx) throw FatalException("get next out of boundry");
    return _nexts.at(idx);
  }
  void append_next(QueryStep* next) { 
    if (_nexts.size() >= 1) throw Unimplemented("Currently we only support sequencial execution");
    _nexts.push_back(next);
  }


  // size_t get_int_rst() { throw QueryException("Not a size_t rst step."); }
};

class LeafStep {
 public:
  size_t get_prev_count() { return 0; }
  // QueryStep* get_prev(const size_t) {throw QueryException("No prev for leaf step."); };
};

class NoOpStep : public QueryStep {
 private:
  // std::vector<QueryStep*> _prevs;
 public:
  OperatorType get_type() { return OperatorType::kNoOp; };
  size_t get_prev_count() { return _prevs.size(); }
  size_t get_dst_col_count() { return 0; }
  // size_t get_next_count() { return _nexts.size(); }
  // QueryStep* get_prev(const size_t idx) { return _prevs.at(idx); }
  void set_prev(std::vector<QueryStep*> prevs) { _prevs = prevs; }
  // QueryStep* get_next(const size_t idx) { return _nexts.at(idx); }
};

class ConstStep : public QueryStep {
 private:
  // std::vector<QueryStep*> _prevs;
 public:
  OperatorType get_type() { return OperatorType::kConst; };
  size_t get_prev_count() { return 0; }
  size_t get_dst_col_count() { return 0; }
  // QueryStep* get_prev(const size_t idx) { return _prevs.at(idx); }
  // void set_prev(std::vector<QueryStep*> prevs) { _prevs = prevs; }
  // QueryStep* get_next(const size_t idx) { return _nexts.at(idx); }
};

class PlacePropCol : public QueryStep {
 private:
  // QueryStep* _prev;
  // std::vector<QueryStep*> _nexts;
 public:
  size_t _prop_idx = std::numeric_limits<size_t>::max();
  OperatorType get_type() { return OperatorType::kPlaceProp; };
  // fixed: the prev is not useful?: must be 1, since we check the size of src_col
  size_t get_prev_count() { return 1; }
  size_t get_dst_col_count() { return 1; }
  // size_t get_next_count() { return _nexts.size(); }
  // QueryStep* get_prev(const size_t idx) { return _prev; }
  // void set_prev(std::vector<QueryStep*> prevs) { _prev = prevs[0]; }
  // QueryStep* get_next(const size_t idx) { return _nexts.at(idx); }
};

class PlacePropColBack : public QueryStep {
 private:
  // QueryStep* _prev;
  // std::vector<QueryStep*> _nexts;
 public:
  size_t _prop_idx = std::numeric_limits<size_t>::max();
  OperatorType get_type() { return OperatorType::kPlacePropBack; };
  size_t get_prev_count() { return 1; }
  size_t get_dst_col_count() { return 1; }
  // size_t get_next_count() { return _nexts.size(); }
  // QueryStep* get_prev(const size_t idx) { return _prev; }
  // void set_prev(std::vector<QueryStep*> prevs) { _prev = prevs[0]; }
  // QueryStep* get_next(const size_t idx) { return _nexts.at(idx); }
};

class AlgeoStep : public QueryStep {
 private:
  // QueryStep* _prevs[2];
  // size_t _src_cols[2];
  // size_t _col_to_put;
 public:
  MathOp _op;
  OperatorType get_type() { return OperatorType::kAlgeo; }
  size_t get_prev_count() { return 2; }
  size_t get_dst_col_count() { return 1; }
};

// class IntOpStep : public QueryStep {
//  public:
//   enum IntOpType {
//     // binary
//     kAdd = 0,
//     kSub,
//     kMul,
//     kDiv,
//     kMod,
//     kEq,
//     KNe,
//     kGe,
//     kG,
//     kLe,
//     kL,
//     // unary
//     // kNeg,
//   };
//  private:
//   QueryStep* _prevs[2];
//   size_t _src_col1, _src_col2;
//   bool _match_row;
//   // std::vector<QueryStep*> _nexts;
//  public:
//   // size_t _rst;
//   IntOpType _optype;
//   // bool is_unary() { return _optype >= kNeg; }
//   OperatorType get_type() { return OperatorType::kIntOp; }
//   // size_t get_prev_count() { return (_optype >= kNeg) ? 1 : 2; }
//   size_t get_prev_count() { return 2; }
//   // size_t get_next_count() { return _nexts.size(); }
//   QueryStep* get_prev(const size_t idx) { return _prevs[idx]; }
//   // QueryStep* get_next(const size_t idx) { return _nexts.at(idx); }
//   void set_prev(std::vector<QueryStep*> prevs) {
//     _prevs[0] = prevs[0]; 
//     _prevs[1] = prevs[1];
//   }
  
//   bool get_match_row() { return _match_row; }
//   size_t get_src_col1() { return _src_col1; }
//   size_t get_src_col2() { return _src_col2; }
//   // size_t get_int_rst() { return _rst; }
// };

class IfStep : public QueryStep {
 private:
  size_t _src_col, _src_row;
 public:
  QueryStep *_end_if_step = nullptr, *_true_branch=nullptr, *_false_branch=nullptr;
  OperatorType get_type() { return OperatorType::kIf; };
  size_t get_prev_count() { return 1; }
  size_t get_dst_col_count() { return 1; }

  size_t get_next_count() { return 1; }
  QueryStep* get_end_if() { return _end_if_step; }
  // QueryStep* get_prev(const size_t idx) { return _cond_step; }
  ResultItem get_cond() { return get_prev(0)->get_rst().get(_src_row, _src_col); }
  QueryStep* get_next(const size_t idx) {
    return (get_cond()) ? _true_branch : _false_branch; 
    // return (_cond_step->get_int_rst()) ? _true_branch : _false_branch; 
  }
};

class EndIfStep : public QueryStep {
 private:
  QueryStep *_if_step = nullptr; // _prev is set in runtime.
  // std::vector<QueryStep*> _nexts;
 public:
  OperatorType get_type() { return OperatorType::kEndIf; };
  size_t get_prev_count() { return 1; }
  size_t get_dst_col_count() { return 0; }
  // size_t get_next_count() { return _nexts.size(); }
  // QueryStep* get_prev(const size_t idx) { return _prev; }
  // QueryStep* get_next(const size_t idx) { return _nexts.at(idx); }
};

class NodeIdStep : public QueryStep {
 private:
  // std::vector<QueryStep*> _nexts;
  // internal_id_t _id;
  label_t _label = 0; // if is 0, label is unknown. we need to traversal each label.

 public:
  OperatorType get_type() { return OperatorType::kNodeId; };
  size_t get_prev_count() { return 0; }
  size_t get_dst_col_count() { return 1; }
  // size_t get_next_count() { return _nexts.size(); }
  // QueryStep* get_prev(const size_t idx) { throw QueryException("No prev."); }
  // QueryStep* get_next(const size_t idx) { return _nexts.at(idx); }

  // internal_id_t get_id() const { return _id; }
  label_t get_label() const { return _label; }
  void set_label(label_t l) { _label = l; }

};

class GetNodeStep : public QueryStep {
 private:
  // std::vector<QueryStep*> _nexts;
  label_t _label = 0;
 public:
  // if the src_col points to an edge, we need to know which node side to query.
  // size_t _src_node_col;
  std::string _src_node_col_alias;
  label_t get_label() { return _label; }
  void set_label(label_t l) { _label = l; }

  OperatorType get_type() { return OperatorType::kGetNode; };
  size_t get_prev_count() { return 1; }
  size_t get_dst_col_count() { return 1; }
  // size_t get_next_count() { return _nexts.size(); }
  // QueryStep* get_prev(const size_t idx) { return _prev; }
  // QueryStep* get_next(const size_t idx) { return _nexts.at(idx); }
};

class GetAllNeighbourStep : public QueryStep {
 private:
  // if do not access the prop of dest, then no need to lock
  // only allow true for now.
  bool _access_prop = true;
  label_t _label = 0;
  dir_t _dir;
 public:
  bool _filter_node_label = false;
  label_t _node_label;
  bool _optional = false; // if true, place a 0 when there is no edge;
  OperatorType get_type() { return OperatorType::kGetAllNeighbor; };
  size_t get_prev_count() { return 1; }
  size_t get_dst_col_count() { return 1; }
  // size_t get_next_count() { return _nexts.size(); }
  // QueryStep* get_prev(const size_t idx) { return _prev; }
  // QueryStep* get_next(const size_t idx) { return _nexts.at(idx); }
  label_t get_label() const { return _label; }
  void set_label(label_t l) { _label = l; }
  dir_t get_dir() const { return _dir; }
  void  set_dir(dir_t d) {_dir = d;} 
};
class GetAllNeighbourVarLenStep : public QueryStep {
 private:
  // if do not access the prop of dest, then no need to lock
  // only allow true for now.
  // bool _access_prop = true;

  // history nodes used for query
  std::unordered_set<std::tuple<internal_id_t, size_t>> _history;
  // history result nodes

  label_t _label = 0;
  dir_t _dir;
  bool touched(const internal_id_t & id, const size_t & src_row) { return _history.find(std::make_tuple(id, src_row)) != _history.end(); }
 public:
  std::queue<std::tuple<internal_id_t, label_t, size_t, size_t>> _rqst;
  std::unordered_set<std::tuple<internal_id_t, size_t>> _hist_rsts;
  std::function<bool(Node*, size_t)> _terminate;
  std::function<bool(Node*, size_t)> _place_to_rst;
  OperatorType get_type() { return OperatorType::kGetAllNeighborVarLen; };
  size_t get_prev_count() { return 1; }
  size_t get_dst_col_count() { return 2; }
  label_t get_label() const { return _label; }
  void set_label(label_t l) { _label = l; }
  dir_t get_dir() const { return _dir; }
  void  set_dir(dir_t d) {_dir = d;}
  // fixed: use touched to remove redundancy
  bool should_place_to_rst(Node* n, size_t depth, size_t src_row) {
    if (_place_to_rst(n, depth) == false) return false;
    if (_hist_rsts.find(std::make_tuple(n->_internal_id, src_row)) != _hist_rsts.end()) return false;
    _hist_rsts.insert(std::make_tuple(n->_internal_id, src_row));
    return true;
  }
  void append_rqst(internal_id_t id, label_t label, size_t depth, size_t src_row) {
    if (touched(id, src_row)) return;
    _rqst.push(std::make_tuple(id, label, depth, src_row)); 
    _history.insert(std::make_tuple(id, src_row));
  }
  bool has_rqst() {
    return _rqst.size() > 0;
  }
  std::tuple<internal_id_t, label_t, size_t, size_t> pop_rqst() {
    std::tuple<internal_id_t, label_t, size_t, size_t> ret = _rqst.front();
    _rqst.pop();
    return ret;
  }
  bool terminate(Node* n, size_t d) { return _terminate(n, d); }
};

/**
 * @brief if the dir is dir_bidir, then return any one of them.
 */
class GetSingleEdgeStep : public QueryStep {
 private:
  // std::vector<QueryStep*> _nexts;
  label_t _label = 0;
  dir_t _dir;
 public:
  inline OperatorType get_type() { return OperatorType::kGetSingleEdge; };
  inline size_t get_prev_count() { return 2; }
  size_t get_dst_col_count() { return 1; }
  label_t get_label() const { return _label; }
  void set_label(label_t l) { _label = l; }
  dir_t get_dir() const { return _dir; }
  void  set_dir(dir_t d) {_dir = d;}
  // inline size_t get_next_count() { return _nexts.size(); }
  // inline QueryStep* get_next(const size_t idx) { return _nexts.at(idx); }
};

class GetAllEdgeStep : public QueryStep {
 private:
  // std::vector<QueryStep*> _nexts;
  label_t _label = 0;
  dir_t _dir;
 public:
  bool _optional = false; // if true, place a 0 when there is no edge;
  inline OperatorType get_type() { return OperatorType::kGetAllEdge; };
  inline size_t get_prev_count() { return 1; }
  size_t get_dst_col_count() { return 1; }
  label_t get_label() const { return _label; }
  void set_label(label_t l) { _label = l; }
  dir_t get_dir() const { return _dir; }
  void  set_dir(dir_t d) {_dir = d;}
};

class GroupByStep : public QueryStep {
 public:
  enum Aggregator {
    kCount=0,
    kMax,
    kMin,
    kAvg,
    kSum,
    kFirst,
    kMakeSet,
  };
  // context for one result col
  struct GroupCtx {
    Aggregator _op;
    size_t _src_col, _dst_col;
    // bool _do_filter;
    std::vector<FilterCtx> _filters;
  };
 private:
  std::vector<size_t> _key_cols;
 public:
  std::vector<GroupCtx> _rst_cols;
  std::vector<size_t> get_key_cols() const { return _key_cols; }
  void append_key_col( const size_t col) { _key_cols.push_back(col); }

  inline OperatorType get_type() { return OperatorType::kGroupBy; };
  inline size_t get_prev_count() { return 1; }
  size_t get_dst_col_count() { return _rst_cols.size(); }
  size_t get_col_to_put(const size_t ) {throw QueryException("Group by step does not use col_to_put"); }
  size_t get_src_col(const size_t ) {throw QueryException("Group by step does not use col_to_put"); }
};

class SortStep : public QueryStep {
 private:
 public:
  std::vector<size_t> _key_cols;
  std::vector<bool> _order; // true for increase, false for decrease
  size_t _skip = 0, _limit;
  inline OperatorType get_type() { return OperatorType::kSort; };
  inline size_t get_prev_count() { return 1; }
  size_t get_dst_col_count() { return 0; }
};

class FilterStep : public QueryStep {
 private:
  
 public:
  CompareOp _filter_op;
  inline OperatorType get_type() { return OperatorType::kFilter; };
  inline size_t get_prev_count() { return 2; }
  size_t get_dst_col_count() { return 0; }
};

class FilterSimpleStep : public QueryStep {
 private:
  // std::vector<QueryStep*> _nexts;
  
 public:
  std::function<bool(ResultItem val)> if_match;

  // CompareOp _filter_op;
  inline OperatorType get_type() { return OperatorType::kFilterSimple; };
  inline size_t get_prev_count() { return 1; }
  size_t get_dst_col_count() { return 0; }
};

class FilterByLabelStep : public QueryStep {
 private:
  label_t _label;
 public:
  CompareOp _filter_op;
  inline OperatorType get_type() { return OperatorType::kFilterByLabel; };
  inline size_t get_prev_count() { return 1; }
  size_t get_dst_col_count() { return 0; }
  label_t get_label() const { return _label; }
  void set_label(label_t l) { _label = l; }
};

class CreateNodeStep : public QueryStep {
 private:
  label_t _label;
 public:
  inline OperatorType get_type() { return OperatorType::kCreateNode; };
  inline size_t get_prev_count() { return 1; }
  size_t get_dst_col_count() { return 1; }
  label_t get_label() const { return _label; }
  void set_label(label_t l) { _label = l; }
};
class UpdateNodeStep : public QueryStep {
 private:
  label_t _label;
 public:
  inline OperatorType get_type() { return OperatorType::kUpdateNode; };
  inline size_t get_prev_count() { return 1; }
  size_t get_dst_col_count() { return 1; }
  label_t get_label() const { return _label; }
  void set_label(label_t l) { _label = l; }
};

class DeleteNodeStep : public QueryStep {
 private:
  label_t _label;
 public:
  inline OperatorType get_type() { return OperatorType::kDeleteNode; };
  inline size_t get_prev_count() { return 1; }
  size_t get_dst_col_count() { return 0; }
  label_t get_label() const { return _label; }
  void set_label(label_t l) { _label = l; }
};


class CreateEdgeStep : public QueryStep {
 private:
  label_t _label;
  dir_t _dir;
 public:
  inline OperatorType get_type() { return OperatorType::kCreateEdge; };
  inline size_t get_prev_count() { return 2; }
  size_t get_dst_col_count() { return 1; }
  label_t get_label() const { return _label; }
  void set_label(label_t l) { _label = l; }
  dir_t get_dir() const { return _dir; }
  void set_dir(dir_t d) { _dir = d; }
};

class UpdateEdgeStep : public QueryStep {
 private:
 public:
  label_t _label;
  inline OperatorType get_type() { return OperatorType::kUpdateEdge; };
  inline size_t get_prev_count() { return 2; }
  size_t get_dst_col_count() { return 1; }
  label_t get_label() const { return _label; }
  void set_label(label_t l) { _label = l; }
};

class DeleteEdgeStep : public QueryStep {
 private:
  label_t _label;
 public:
  inline OperatorType get_type() { return OperatorType::kDeleteEdge; };
  inline size_t get_prev_count() { return 2; }
  size_t get_dst_col_count() { return 0; }
  label_t get_label() const { return _label; }
  void set_label(label_t l) { _label = l; }
};