#pragma once
#include "query_step.hpp"
#include "query.hpp"
#include "ccgraph.hpp"
#include "schema.hpp"
#include "time_util.hpp"
#include "string_server.hpp"
#include "logger.hpp"
#include <memory>
#include <folly/futures/Future.h>
#include <limits>
#include <unordered_set>



template<typename T>
T algeo(const T l, const T r, const MathOp op) {
  switch (op) {
    case kAdd: return l + r;
    case kSub: return l - r;
    case kMul: return l * r;
    case kDiv: return l / r;
    case kMod: return l % r;
    case kMax: return std::max(l, r);
    case kMin: return std::min(l, r);
    case kTimeSubMin: throw QueryException("time operation must be on uint64");
    default: throw FatalException("Unreachable");
  }
}
template <>
uint64_t algeo<uint64_t>(const uint64_t l, const uint64_t r, const MathOp op) {
  switch (op) {
    case kAdd: return l + r;
    case kSub: return l - r;
    case kMul: return l * r;
    case kDiv: return l / r;
    case kMod: return l % r;
    case kMax: return std::max(l, r);
    case kMin: return std::min(l, r);
    case kTimeSubMin: return date_time::minus_ldbc(l, r) / 1000 / 60;
    default: throw FatalException("Unreachable");
  }
}

template <>
double algeo<double>(const double l, const double r, const MathOp op) {
  switch (op) {
    case kAdd: return l + r;
    case kSub: return l - r;
    case kMul: return l * r;
    case kDiv: return l / r;
    case kMod: throw QueryException("No mod on double");
    case kMax: return std::max(l, r);
    case kMin: return std::min(l, r);
    case kTimeSubMin: throw QueryException("time operation must be on uint64");
    default: throw FatalException("Unreachable");
  }
}
class Worker {
 private:
  template<typename T>
  using sptr = std::shared_ptr<T>;
  using VFuture=folly::Future<folly::Unit>;
  using VPromise=folly::Promise<folly::Unit>;
  template<typename T>
  using Future=folly::Future<T>;

  CCGraph* _ccgraph = nullptr;
  SchemaManager* _schema = nullptr;

  template<typename T>
  static bool compare(const T l, const T r, const CompareOp op) {
    switch (op) {
      case kEq: return l == r;
      case kNe: return l != r;
      case kG:  return l > r;
      case kGe: return l >= r;
      case kL:  return l < r;
      case kLE: return l <= r;
      default: throw FatalException("Unreachable");
    }
  }
  static bool compare_item(const ResultItem l, const ResultItem r, 
                    const Result::ColumnType ty1, const Result::ColumnType ty2,
                    const CompareOp op) {
    switch (ty1) {
      case Result::kEdge: {
        if (l == 0 || r == 0) {
          //  this is the case where node pointer is nullptr.
          if (ty2 != Result::kEdge || (op != kEq && op != kNe) || r != 0) throw Unimplemented("should filter out nullptr to compare order of ptr");
          return compare(l, r, op);
        }
        throw QueryException("No direct compare between edge");
        // if (ty2 != Result::kEdge)
        //   throw QueryException("Can not compare edge with non-edge");
        // if (l == 0) {
        //   //  this is the case where edge pointer is nullptr.
        //   if (op != kEq || op != kNe || r != 0) throw Unimplemented("should filter");
        //   return compare(l, r, op);
        // }
        // Edge *e1 = (Edge*)l, *e2 = (Edge*)r;
        // if (op == CompareOp::kEq) return compare(e1->_id1, e2->_id1, op) &&
        //                                  compare(e1->_id2, e2->_id2, op) &&
        //                                  compare(e1->_label, e2->_label, op);

        // if (op == CompareOp::kNe) return compare(e1->_id1, e2->_id1, op) ||
        //                                  compare(e1->_id2, e2->_id2, op) ||
        //                                  compare(e1->_label, e2->_label, op);

        // return (compare(e1->_id1, e2->_id1, op)) ||
        //        (compare(e1->_id1, e2->_id1, CompareOp::kEq) && compare(e1->_id2, e2->_id2, op)) ||
        //        (compare(e1->_id1, e2->_id1, CompareOp::kEq) && compare(e1->_id2, e2->_id2, CompareOp::kEq) && compare(e1->_label, e2->_label, op));
      } 
      case Result::kNode: {
        if (l == 0 || r == 0) {
          if (ty2 != Result::kNode || (op != kEq && op != kNe)) throw Unimplemented("should filter out nullptr");
          //  this is the case where node pointer is nullptr.
          return compare(l, r, op);
        }
        if (ty2 == Result::kNode) {
          labeled_id_t id1 = ((Node*)l)->_external_id, id2 = ((Node*)r)->_external_id;
          return compare(id1, id2, op);
        } else if (ty2 == Result::kLabeledNodeId) {
          labeled_id_t id1 = ((Node*)l)->_external_id, id2(r);
          return compare(id1, id2, op);
        } else if (ty2 == Result::kInnerNodeId) {
          internal_id_t id1 = ((Node*)l)->_internal_id, id2;
          id2.id = r;
          return compare(id1, id2, op);
        } else {
          throw QueryException("Can only compare node id with uint64 or node id");
        }
      }
      case Result::kDOUBLE: {
        if (ty2 != ty1) throw QueryException("Compare must be on same type");
        double d1 = *(double*)&l, d2 = *(double*)&r;
        return compare(d1, d2, op);
      }
      case Result::kUINT32: {
        if (ty2 != ty1) throw QueryException("Compare must be on same type");
        uint32_t d1 = *(uint32_t*)&l, d2 = *(uint32_t*)&r;
        return compare(d1, d2, op);
      }
      // case Result::kSet: throw QueryException("no compare on set");
      case Result::kSet: // fix: use string as set
      case Result::kSTRING: {
        if (ty2 != ty1) throw QueryException("Compare must be on same type");
        return StringServer::get()->compare(l, r, op);
      }
      case Result::kUINT64: {
        if (ty2 != ty1) throw QueryException("Compare must be on same type");
        uint64_t d1 = *(uint64_t*)&l, d2 = *(uint64_t*)&r;
        return compare(d1, d2, op);
      }
      case Result::kUINT8: {
        if (ty2 != ty1) throw QueryException("Compare must be on same type");
        uint8_t d1 = *(uint8_t*)&l, d2 = *(uint8_t*)&r;
        return compare(d1, d2, op);
      }
      case Result::kINT64: {
        if (ty2 != ty1) throw QueryException("Compare must be on same type");
        int64_t d1 = *(int64_t*)&l, d2 = *(int64_t*)&r;
        return compare(d1, d2, op);
      }
      case Result::kInnerNodeId: {
        if (ty2 != ty1) throw QueryException("Compare must be on same type");
        internal_id_t id1(l), id2(r);
        return compare(l, r, op);
      }
      case Result::kLabeledNodeId: {
        if (ty2 != ty1) throw QueryException("Compare must be on same type");
        labeled_id_t id1(l), id2(r);
        return compare(l, r, op);
      }
      default: throw FatalException("Unreachable");
    }
  }

  static ResultItem algeo_item(
      const ResultItem l, const ResultItem r,
      const Result::ColumnType l_ty, const Result::ColumnType r_ty,
      const MathOp op) {
    if (l_ty != r_ty) throw QueryException("Algeo, lr must have the same type");
    switch (l_ty) {
      case Result::kInnerNodeId:
      case Result::kLabeledNodeId:
      case Result::kNode:
      case Result::kEdge: throw QueryException("Algeo, type must be numberic");
      case Result::kDOUBLE: {
        ResultItem ret = 0;
        double d1 = *(double*)&l, d2 = *(double*)&r, &rst = *(double*)&ret;
        rst = algeo(d1, d2, op);
        return ret;
      }
      case Result::kUINT8: {
        ResultItem ret = 0;
        uint8_t d1 = *(uint8_t*)&l, d2 = *(uint8_t*)&r, &rst = *(uint8_t*)&ret;
        rst = algeo(d1, d2, op);
        return ret;
      }
      case Result::kUINT32: {
        ResultItem ret = 0;
        uint32_t d1 = *(uint32_t*)&l, d2 = *(uint32_t*)&r, &rst = *(uint32_t*)&ret;
        rst = algeo(d1, d2, op);
        return ret;
      }
      case Result::kUINT64: {
        ResultItem ret = 0;
        uint64_t d1 = *(uint64_t*)&l, d2 = *(uint64_t*)&r, &rst = *(uint64_t*)&ret;
        rst = algeo(d1, d2, op);
        return ret;
      }
      case Result::kINT64: {
        ResultItem ret = 0;
        int64_t d1 = *(int64_t*)&l, d2 = *(int64_t*)&r, &rst = *(int64_t*)&ret;
        rst = algeo(d1, d2, op);
        return ret;
      }
      case Result::kSTRING: throw QueryException("No algeo on string");
      case Result::kSet: throw QueryException("No algeo on set");
      default: throw FatalException("Unreachable");
    }
  }

  void ProcIf(QueryStep* step) {
    auto if_step = dynamic_cast<IfStep*>(step);
    auto end_if_step = dynamic_cast<EndIfStep*>(if_step->get_end_if());
    if (if_step->get_cond()) {
      end_if_step->set_prev({if_step->_true_branch});
    } else {
      end_if_step->set_prev({if_step->_false_branch});
    }
  }

  struct AggregateResult {
    bool collected_first = false;
    ResultItem main_rst = 0;
    std::string make_set_rst = "";
    uint64_t cnt = 0;
    GroupByStep::Aggregator op;
    Result::ColumnType src_type, dst_type;
    std::vector<FilterCtx> *filters;
    // std::vector<Result::ColumnType> original_col_types;
    Result* orignal_rst_table = nullptr;
    // void init(Result::ColumnType type, GroupByStep::GroupCtx & group_ctx) {
    //   init(type, group_ctx._op, group_ctx._filters);
    // }
    void init(Result::ColumnType type, GroupByStep::Aggregator op, std::vector<FilterCtx> & filters, Result* orig_rst_table) {
      // this->original_col_types = orig_col_types;
      this->orignal_rst_table = orig_rst_table;
      src_type = type;
      this->op = op;
      this->filters = &filters;
      switch (op) {
        case GroupByStep::kAvg: dst_type = Result::kDOUBLE; break;
        case GroupByStep::kCount: dst_type = Result::kUINT64; break;
        case GroupByStep::kMax: {
          dst_type = src_type;
          switch (src_type) {
            case Result::kNode:
            case Result::kSTRING: 
            case Result::kSet:
            case Result::kEdge: throw QueryException("Unimplemented");
            case Result::kDOUBLE: *(double*)&main_rst = std::numeric_limits<double>::max(); break;
            case Result::kINT64:  *(int64_t*)&main_rst = std::numeric_limits<int64_t>::max(); break;
            case Result::kUINT64:  *(uint64_t*)&main_rst = std::numeric_limits<uint64_t>::max(); break;
            case Result::kUINT32:  *(uint32_t*)&main_rst = std::numeric_limits<uint32_t>::max(); break;
            case Result::kUINT8:  *(uint8_t*)&main_rst = std::numeric_limits<uint8_t>::max(); break;
            case Result::kInnerNodeId: *(uint64_t*)&main_rst = std::numeric_limits<uint64_t>::max(); break;
            case Result::kLabeledNodeId: *(uint64_t*)&main_rst = std::numeric_limits<uint64_t>::max(); break;
          }
          break;
        }
        case GroupByStep::kMin: {
          dst_type = src_type;
          switch (src_type) {
            case Result::kSTRING: 
            case Result::kSet:
            case Result::kEdge: throw QueryException("Unimplemented");
            case Result::kNode: main_rst = 0;
            case Result::kDOUBLE: *(double*)&main_rst = std::numeric_limits<double>::min(); break;
            case Result::kINT64:  *(int64_t*)&main_rst = std::numeric_limits<int64_t>::min(); break;
            case Result::kUINT64:  *(uint64_t*)&main_rst = std::numeric_limits<uint64_t>::min(); break;
            case Result::kUINT32:  *(uint32_t*)&main_rst = std::numeric_limits<uint32_t>::min(); break;
            case Result::kUINT8:  *(uint8_t*)&main_rst = std::numeric_limits<uint8_t>::min(); break;
            case Result::kInnerNodeId: *(uint64_t*)&main_rst = std::numeric_limits<uint64_t>::min(); break;
            case Result::kLabeledNodeId: *(uint64_t*)&main_rst = std::numeric_limits<uint64_t>::min(); break;
          }
          break;
        }
        case GroupByStep::kSum: dst_type = src_type; break;
        case GroupByStep::kFirst: dst_type = src_type; break;
        case GroupByStep::kMakeSet: {
          dst_type = Result::kSet;
          // main_rst = (ResultItem)new std::unordered_set<ResultItem>;
          // dst_type = Result::kSTRING;
          break;
        }
      }
    }
    void collect(ResultItem val, const ResultRow& entire_row) {
      for (auto & filter : *filters) {
        if (filter.col_alias_to_filter == "") {
          if (compare_item(val, filter.val, src_type, filter.ty, filter.op) == false) {
            return;
          }
        } else {
          size_t col_to_filter = orignal_rst_table->get_col_idx_by_alias(filter.col_alias_to_filter);
          ResultItem val_to_filter = entire_row.at(col_to_filter);
          Result::ColumnType src_col_type = orignal_rst_table->get_type(col_to_filter);
          if (compare_item(val_to_filter, filter.val, src_col_type, filter.ty, filter.op) == false) {
            return;
          }
        }
      }
      // if (op == GroupByStep::kMakeSet) {
      //   ((std::unordered_set<ResultItem>*)main_rst)->insert(val);
      //   return;
      // }
      // if (op == GroupByStep::kCount) {
      //   main_rst++;
      //   return;
      // }
      if (src_type == Result::kEdge) {
        // todo assert op is count
        if (op != GroupByStep::kCount && op != GroupByStep::kFirst) throw QueryException("group edge ptr, op must be count");
        // return;
      }
      if (src_type == Result::kNode) {
        // todo assert op is count
        if (op != GroupByStep::kCount && op != GroupByStep::kMax && op != GroupByStep::kMin && op != GroupByStep::kFirst) throw QueryException("group node ptr, op must be count");
        // return;
      }
      // if (src_type == Result::kSTRING) {
      //   // todo: assert op is count; compare is not implemented
      //   // if (op != GroupByStep::kCount || op != ) throw QueryException("group ptr, op must be count");
      //   return;
      // }
      switch (op) {
        case GroupByStep::kAvg : {
          cnt++;
          // todo: might overflow
          main_rst = algeo_item(main_rst, val, src_type, src_type, kAdd);
          break;
        }
        case GroupByStep::kMax : { 
          if (src_type == Result::kNode) {
            if (val == 0) {
              // do nothing. this is the one we should ignore
              return;
            }
            if (main_rst == 0) {
              main_rst = val;
              return;
            }
          }
          if (compare_item(main_rst, val, src_type, src_type, CompareOp::kL))
            main_rst = val;
          break;
        }
        case GroupByStep::kMin : {
          if (src_type == Result::kNode) {
            if (val == 0) {
              // do nothing. this is the one we should ignore
              return;
            }
            if (main_rst == 0) {
              main_rst = val;
              return;
            }
          }
          if (compare_item(main_rst, val, src_type, src_type, CompareOp::kG))
            main_rst = val;
          break;
        }
        case GroupByStep::kSum : {
          // todo: might overflow
          main_rst = algeo_item(main_rst, val, src_type, src_type, kAdd);
          break;
        }
        case GroupByStep::kCount: {
          main_rst ++;
          break;
        }
        case GroupByStep::kFirst: {
          if (collected_first) break;
          collected_first = true;
          main_rst = val;
          break;
        }
        case GroupByStep::kMakeSet: {
          if (make_set_rst != "") make_set_rst += ";";
          make_set_rst += Result::get_string(val, src_type);
          // LOG_VERBOSE("making set, rst is [%s]", make_set_rst.c_str());
          break;
        }
        default: throw FatalException("Unreachable");
      }
    }
  };

  struct AggregateCols {
    std::vector<AggregateResult> cols;
    ResultRow key_example;
    std::vector<GroupByStep::GroupCtx> * ctxs;
    std::vector<size_t> key_cols;
    std::vector<Result::ColumnType> orig_col_types;
    Result * original_table = nullptr;
    void init(std::vector<GroupByStep::GroupCtx> & ctxs_in,
              std::vector<Result::ColumnType> types,
              std::vector<size_t> key_cols,
              const ResultRow & key,
              Result* orig_table) {
      this->original_table = orig_table;
      this->orig_col_types = types;
      this->ctxs = &ctxs_in;
      this->key_cols = key_cols;
      key_example = key;

      cols.resize(ctxs->size());
      for (size_t i = 0; i < ctxs->size(); i++) {
        cols.at(i).init(types.at(ctxs->at(i)._src_col), ctxs->at(i)._op, ctxs->at(i)._filters, original_table);
      }
    }
    void collect(const ResultRow & row) {
      for (size_t i = 0; i < ctxs->size(); i++) {
        // LOG_VERBOSE("collecting a row, now the src_col is %d", (*ctxs).at(i)._src_col);
        cols.at(i).collect(row.at((*ctxs).at(i)._src_col), row);
      }
    }
    void write_back(Result & dst) {
      ResultRow & row = dst.append_row();
      for (size_t i = 0; i < key_cols.size(); i++) {
        row.at(i) = key_example.at(key_cols.at(i));
      }
      for (size_t k = 0; k < cols.size(); k++) {
        if (dst.get_type(ctxs->at(k)._dst_col) != cols.at(k).dst_type) {
          throw QueryException("Mismatch aggregate result type");
        }
        if (cols.at(k).op == GroupByStep::kAvg) {
          double &sum = *(double*)&cols.at(k).main_rst;
          row.at(ctxs->at(k)._dst_col) = sum / cols.at(k).cnt;
        } else if (cols.at(k).op == GroupByStep::kMakeSet) {
          row.at(ctxs->at(k)._dst_col) = StringServer::get()->touch("[" + cols.at(k).make_set_rst + "]");
        } else {
          row.at(ctxs->at(k)._dst_col) = cols.at(k).main_rst;
        }
      }
    }
  };

  struct ResultRowHasher {
    std::vector<size_t> _key_cols;
    std::vector<Result::ColumnType> _col_types;
    ResultRowHasher(std::vector<size_t> key_cols, std::vector<Result::ColumnType> col_types) : 
      _key_cols(key_cols), _col_types(col_types) {}
    std::size_t operator()(const ResultRow& k) const {
      size_t hash_rst = 0;
      for (size_t col : _key_cols) {
        ResultItem r = k.at(col);
        switch (_col_types.at(col)) {
          case Result::kNode: {
            Node *n1 = (Node*)r;
            if (n1 == nullptr) {
              hash_rst = hash_rst ^ std::hash<uint64_t>()(0);
            } else {
              hash_rst = hash_rst ^ std::hash<internal_id_t>()(n1->_internal_id);
            }
            break;
          }
          case Result::kEdge : {
            Edge *e1 = (Edge*)r;
            hash_rst = hash_rst ^ (e1->_internal_id1 * 18446744073709551557ull);
            hash_rst = hash_rst ^ (e1->_internal_id2 * 18446744073709551557ull);
            hash_rst = hash_rst ^ (e1->_label * 18446744073709551557ull);
            break;
          }
          case Result::kDOUBLE : {
            double &d1 = *(double*)&r;
            hash_rst = hash_rst ^ std::hash<double>()(d1);
            break;
          }
          // case Result::kSet: throw QueryException("no hash on set col"); 
          case Result::kSet:  // fix: same as string
          case Result::kSTRING : // this is an unique id for each string
          case Result::kUINT8 :
          case Result::kUINT32 :
          case Result::kUINT64 : 
          case Result::kInnerNodeId: 
          case Result::kLabeledNodeId: 
          case Result::kINT64: {
            hash_rst = hash_rst ^ std::hash<uint64_t>()(r);
            break;
          }
        }
      }
      return hash_rst;
    } 
  };

  struct ResultRowCompareEqual {
    std::vector<size_t> _key_cols;
    std::vector<Result::ColumnType> _col_types;
    ResultRowCompareEqual(std::vector<size_t> key_cols, std::vector<Result::ColumnType> col_types) : 
      _key_cols(key_cols), _col_types(col_types) {}
    bool operator()(const ResultRow& lhs, const ResultRow& rhs) const {
      for (size_t col : _key_cols) {
        ResultItem r1 = lhs.at(col), r2 = rhs.at(col);
        if (compare_item(r1, r2, _col_types.at(col), _col_types.at(col), CompareOp::kNe))
          return false;
        // switch (_col_types[idx]) {
        //   case Result::kNode: {
        //     Node *n1 = (Node*)r1, *n2 = (Node*)r2;
        //     if (n1->_id != n2->_id) return false;
        //     break;
        //   }
        //   case Result::kEdge : {
        //     Edge *e1 = (Edge*)r1, *e2 = (Edge*)r2;
        //     if (e1->_id1 != e2->_id1 || 
        //         e1->_id2 != e2->_id2 || 
        //         e1->_label != e2->_label) return false;
        //     break;
        //   }
        //   case Result::kDOUBLE : {
        //     double &d1 = *(double*)&r1, &d2 = *(double*)&r2;
        //     if (d1 != d2) return false;
        //     break;
        //   }
        //   case Result::kSTRING :
        //   case Result::kUINT8 :
        //   case Result::kUINT32 :
        //   case Result::kUINT64 : {
        //     if (r1 != r2) return false;
        //     break;
        //   }
        // }
      }
      return true;
    }
  };

  void ProcGroupBy(QueryStep* _step_) {
    // TODO: test handle no key col, seems working
    auto step = dynamic_cast<GroupByStep*>(_step_);
    Result & prev_rst = step->get_prev(0)->get_rst();
    ResultRowCompareEqual comparer(step->get_key_cols(), prev_rst.get_schema());
    ResultRowHasher hasher(step->get_key_cols(), prev_rst.get_schema());
    std::unordered_map<ResultRow, AggregateCols, 
                       ResultRowHasher, ResultRowCompareEqual> 
      rst_list{0, hasher, comparer};
    // LOG_VERBOSE("collecting a new row, the alias of each col is:");
    // for (size_t j = 0; j < prev_rst.get_cols(); j++) {
    //   LOG_VERBOSE("col %d, alias is %s", j, prev_rst.get_col_alias(j).c_str());
    // }
    for (size_t i = 0; i < prev_rst.get_rows(); i++) {
      const ResultRow & r = prev_rst.get_row(i);
      auto iter = rst_list.find(r);
      if (iter == rst_list.end()) {
        rst_list[r] = AggregateCols{};
        iter = rst_list.find(r);
        // todo: if the row is put to a key, but filtered out by all result row, the key should be deleted
        // or, make a new col, if it does not match any filter
        // but current quereis do not require this.
        iter->second.init(step->_rst_cols, prev_rst.get_schema(), comparer._key_cols, r, &prev_rst);
      }

      AggregateCols & collectors = iter->second;
      collectors.collect(r);
    }

    for (auto iter : rst_list) {
      iter.second.write_back(step->get_rst());
    }
  }

  struct ResultRowCompareOrder {
    std::vector<size_t> _key_cols;
    std::vector<bool> _order;
    std::vector<Result::ColumnType> _col_types;
    ResultRowCompareOrder(std::vector<size_t> key_cols, std::vector<bool> order, std::vector<Result::ColumnType> col_types) :
        _key_cols(key_cols), _order(order), _col_types(col_types) {
      // _key_cols = key_cols;
      // _order = order;
    }
    bool operator()(const ResultRow& lhs, const ResultRow& rhs) const {
      for (size_t i = 0; i < _key_cols.size(); i++) {
        size_t col = _key_cols.at(i);
        bool increase = _order.at(i);
        CompareOp op = increase ? CompareOp::kL : CompareOp::kG;
        ResultItem r1 = lhs.at(col), r2 = rhs.at(col);
        if (compare_item(r1, r2, _col_types.at(col), _col_types.at(col), CompareOp::kEq)) continue;
        return compare_item(r1, r2, _col_types.at(col), _col_types.at(col), op);
        // switch (_col_types[col]) {
        //   case Result::kNode: {
        //     Node *n1 = (Node*)r1, *n2 = (Node*)r2;
        //     if (n1->_id == n2->_id) continue;
        //     return (n1->_id < n2->_id) ^ increase;
        //   }
        //   case Result::kEdge : {
        //     throw QueryException("Makes no sense if directly compare edge?");
        //     break;
        //   }
        //   case Result::kDOUBLE : {
        //     double &d1 = *(double*)&r1, &d2 = *(double*)&r2;
        //     if (d1 == d2) continue;
        //     return (d1 < d2) ^ increase;
        //   }
        //   case Result::kSTRING :
        //   case Result::kUINT8 :
        //   case Result::kUINT32 :
        //   case Result::kUINT64 : {
        //     if (r1 == r2) continue;
        //     return (r1 < r2) ^ increase;
        //     // if (r1 > r2) return true;
        //     // if (r1 < r2) return false;
        //     // break;
        //   }
        // }
      }
      return true;
    }

  };

  void ProcSort(QueryStep* _step_) {
    auto step = dynamic_cast<SortStep*>(_step_);
    Result & r = step->get_rst();

    if (step->_key_cols.size() != 0) {
      ResultRowCompareOrder compare(step->_key_cols, step->_order, step->get_rst().get_schema());
      std::sort(r.get_rst_table().begin(), r.get_rst_table().end(), compare);
    }
    std::vector<ResultRow> & table = r.get_rst_table();
    if (step->_limit > table.size()) step->_limit = table.size();
    if (step->_skip > step->_limit) step->_skip = step->_limit;
    if (step->_skip != 0) {
      for (size_t i = 0; i + step->_skip < step->_limit; i++) {
        table.at(i).swap(table.at(i+step->_skip));
      }
    }
    table.resize(step->_limit - step->_skip);
    r.update_rows();
    table.shrink_to_fit();
  }


  void ProcFilter(QueryStep* _step_) {
    auto step = dynamic_cast<FilterStep*>(_step_);
    Result & prev_rst1  = step->get_prev(0)->get_rst(),
           & prev_rst2  = step->get_prev(1)->get_rst(),
           & r = step->get_rst();
    size_t src_col1 = step->get_src_col(0), src_col2 = step->get_src_col(1);

    ResultItem op2 = prev_rst2.get(0, src_col2);
    Result::ColumnType ty1 = prev_rst1.get_type(src_col1), 
                        ty2 = prev_rst2.get_type(src_col2);
    for (size_t i = 0; i < prev_rst1.get_rows(); i++) {
      if (prev_rst1.get_rows() == prev_rst2.get_rows()) {
        op2 = prev_rst2.get(i, src_col2);
      }
      ResultItem op1 = prev_rst1.get(i, src_col1);
      bool match = compare_item(op1, op2, ty1, ty2, step->_filter_op);
      // LOG_VERBOSE("comparing \"%s\" to Jun, rst is %d", StringServer::get()->get(op1).c_str(), match);
      if (match) {
        r.append_row(i, prev_rst1);
      }
    }
  }
  void ProcFilterSimple(QueryStep* _step_) {
    auto step = dynamic_cast<FilterSimpleStep*>(_step_);
    Result & prev_rst = step->get_prev(0)->get_rst(),
           & r = step->get_rst();
    size_t src_col = step->get_src_col(0);
    for (size_t i = 0; i < prev_rst.get_rows(); i++) {
      ResultItem val = prev_rst.get(i, src_col);
      bool match = step->if_match(val);
      if (match) {
        r.append_row(i, prev_rst);
      }
    }
  }

  void ProcFilterByLabel(QueryStep* _step_) {
    auto step = dynamic_cast<FilterByLabelStep*>(_step_);
    Result & prev_rst  = step->get_prev(0)->get_rst(),
           & r = step->get_rst();
    size_t src_col = step->get_src_col();
    Result::ColumnType src_ty = prev_rst.get_type(src_col);
    if (src_ty != Result::kEdge && src_ty != Result::kNode)
      throw QueryException("filter label works on edge or node");
    if (step->_filter_op != kEq && step->_filter_op != kNe)
      throw QueryException("label can only be compared using ==/!=");
    for (size_t i = 0; i < prev_rst.get_rows(); i++) {
      label_t src_label;
      if (src_ty == Result::kEdge) 
        src_label = ((Edge*)prev_rst.get(i, src_col))->_label;
      else 
        src_label = ((Node*)prev_rst.get(i, src_col))->_type;

      bool match;
      if (step->_filter_op == kEq)
        match = (src_label == step->get_label());
      else 
        match = (src_label != step->get_label());
      if (match) {
        r.append_row(i, prev_rst);
      }
    }
  }
  void ProcPlacePropCol(QueryStep* _step_) {
    auto step = dynamic_cast<PlacePropCol*>(_step_);
    Result & r  = step->get_rst();
    for (size_t i = 0; i < r.get_rows(); i++) {
      ResultItem item = 0;
      if (r.get_type(step->get_src_col()) == Result::kNode) {
        Node* n = (Node*)r.get(i, step->get_src_col());
        if (n != nullptr) {
          _schema->get_prop(n->_prop, n->_type, step->_prop_idx, (uint8_t*)&item);
        }
      } else if (r.get_type(step->get_src_col()) == Result::kEdge) {
        Edge* e = (Edge*)r.get(i, step->get_src_col());
        if (e != nullptr) {
          _schema->get_prop(e->_prop, e->_label, step->_prop_idx, (uint8_t*)&item);
        }
      } else {
        throw QueryException("Only node & edge has prop");
      }
      r.set(i, step->get_col_to_put(), item);
    }
  }
  void ProcPlacePropColBack(QueryStep* _step_) {
    auto step = dynamic_cast<PlacePropColBack*>(_step_);
    Result & src_rst = step->get_prev(0)->get_rst();
    Result & dst_rst  = step->get_rst();
    for (size_t i = 0; i < dst_rst.get_rows(); i++) {
      ResultItem item = src_rst.get( src_rst.get_rows() == 1 ? 0 : i,  step->get_src_col());
      if (dst_rst.get_type(step->get_col_to_put()) == Result::kNode) {
        Node* n = (Node*)dst_rst.get(i, step->get_col_to_put());
        if (n == nullptr) throw QueryException("currently there should be no place back to null ptr");
        _schema->set_prop(n->_prop, n->_type, step->_prop_idx, (uint8_t*)&item);
      } else if (dst_rst.get_type(step->get_col_to_put()) == Result::kEdge) {
        Edge* e = (Edge*)dst_rst.get(i, step->get_col_to_put());
        if (e == nullptr) throw QueryException("currently there should be no place back to null ptr");
        _schema->set_prop(e->_prop, e->_label, step->_prop_idx, (uint8_t*)&item);
      } else {
        throw QueryException("Only node & edge has prop");
      }
    }
  }

  void ProcAlgeoOp(QueryStep* _step_) {
    auto step = dynamic_cast<AlgeoStep*>(_step_);
    Result & r1 = step->get_prev(0)->get_rst();
    Result & r2 = step->get_prev(1)->get_rst();
    Result & r  = step->get_rst();
    size_t col1 = step->get_src_col(0), col2 = step->get_src_col(1);
    Result::ColumnType ty1 = r1.get_type(col1), ty2 = r2.get_type(col2);
    bool prev1_single = (r1.get_rows() == 1), 
         prev2_single = (r2.get_rows() == 1);
    if (!prev1_single && !prev2_single) {
      if (r1.get_rows() !=  r2.get_rows()) {
        throw QueryException("Mismatch r1 and r2");
      }
    }
    size_t n_rows = std::max(r1.get_rows(),  r2.get_rows());

    for (size_t i = 0; i < n_rows; i++) {
      ResultItem v1 = r1.get(i, col1), v2 = r2.get(i, col2);
      r.set(i, step->get_col_to_put(0), algeo_item(v1, v2, ty1, ty2, step->_op));
    }
  }
  VFuture ProcGetNode(QueryStep* _step_) {
    auto step = dynamic_cast<GetNodeStep*>(_step_);
    if (step->get_reuse_prev_rst()) {
      throw QueryException("rst reuse not implemented");
    }
    QueryStep* prev = step->get_prev(0);
    // step->get_rst() = prev->get_rst(); // make a copy
    size_t col_to_put = step->get_col_to_put();
    size_t src_col = step->get_src_col();
    switch (prev->get_rst().get_type(src_col)) {
      case Result::kInnerNodeId: {
        if (step->get_label() == 0) {
          throw Unimplemented("Unimplemented get unknown label");
        }
        auto f = folly::makeFuture();
        for (size_t i = 0; i < prev->get_rst().get_rows(); i++) {
          internal_id_t id = prev->get_rst().get(i, src_col);
          f = std::move(f).thenValue([this, step, id](folly::Unit){
            return _ccgraph->GetNodeViaInternalId(id, step->get_cc_ctx());
          }).thenValue([this, step, col_to_put, i](Node* n){
            step->get_rst().set(i, col_to_put, (ResultItem)n);
          });
        }
        return f;
      }
      case Result::kLabeledNodeId: {
        if (step->get_label() == 0) {
          throw Unimplemented("Unimplemented get unknown label");
        }
        auto f = folly::makeFuture();
        for (size_t i = 0; i < prev->get_rst().get_rows(); i++) {
          labeled_id_t id(prev->get_rst().get(i, src_col), step->get_label());
          f = std::move(f).thenValue([this, step, id](folly::Unit){
            return _ccgraph->GetNodeViaLabeledId(step->get_label(), id, step->get_cc_ctx());
          }).thenValue([this, step, col_to_put, i](Node* n){
            step->get_rst().set(i, col_to_put, (ResultItem)n);
          });
        }
        return f;
      }
      case Result::kEdge: {
        // auto prev = dynamic_cast<EdgeRstListStep*>(_prev_);
        VFuture f = folly::makeFuture();
        // fixed: set the src_node_col correctly
        size_t src_node_col = step->get_rst().get_col_idx_by_alias(step->_src_node_col_alias);
        if (prev->get_rst().get_type(src_node_col) != Result::kNode) {
          throw QueryException("get node from edge, but src_node_col is not node");
        }
        for (size_t i = 0; i < prev->get_rst().get_rows(); i++) {
          Edge* e = (Edge*)(prev->get_rst().get(i, src_col));
          if (e == nullptr) {
            step->get_rst().set(i, col_to_put, 0);
            continue;
          }
          Node* src_n = (Node*)prev->get_rst().get(i, src_node_col);
          Node* dst_n;
          if (src_n == nullptr || e == nullptr) 
            throw QueryException("Incorret handling null pointer");

          if (src_n->_internal_id == e->_internal_id1) 
            dst_n = e->_node2;
          else if (src_n->_internal_id == e->_internal_id2) 
            dst_n = e->_node1;
          else 
            throw QueryException("get node from edge, but src node does match edge's id");
          
          f = std::move(f).thenValue([this, step, dst_n](folly::Unit){
            return _ccgraph->AccessNode(dst_n, true, step->get_cc_ctx());
          }).thenValue([step, i, col_to_put](Node* n){
            step->get_rst().set(i, col_to_put, (ResultItem)n);
          });
        }
        return f;
      }
      default: throw QueryException("Wrong prev type");
    }
  }
  labeled_id_t extract_node_id(Result & row, size_t i, size_t j) {
    ResultItem val = row.get(i, j);
    Result::ColumnType ty = row.get_type(j);
    if (ty == Result::kLabeledNodeId) {
      // fixed: check that const step has the correct label
      if (row.get_rows() != 1) {throw QueryException("is labeled node id is in result, the step must be a const step. The label should also be set");}
      return labeled_id_t(row.get(i, j), row.get_const_label());
    }
    if (ty != Result::kNode) throw QueryException("Node id must be extract from correct type");
    if (val == 0) throw QueryException("Incorrect handle of null node result");
    return ((Node*)val)->_external_id;
  }
  VFuture ProcGetSingleEdge(QueryStep* _step_) {
    auto step = dynamic_cast<GetSingleEdgeStep*>(_step_);
    auto & prev_rst1 = step->get_prev(0)->get_rst(), 
         & prev_rst2 = step->get_prev(1)->get_rst();
    // r = prev_rst1;
    auto f = folly::makeFuture();
    if (prev_rst2.get_rows() == 0) return f;
    labeled_id_t id2 = extract_node_id(prev_rst2, 0, step->get_src_col(1));
    for (size_t i = 0; i < prev_rst1.get_rows(); i++) {
      if (prev_rst1.get(i, step->get_src_col(0)) == 0 && prev_rst1.get_type(step->get_src_col(0)) == Result::kNode) {
        step->get_rst().set(i, step->get_col_to_put(), 0);
        continue;
      }
      labeled_id_t id1= extract_node_id(prev_rst1, i, step->get_src_col(0));
      if (prev_rst2.get_rows() == prev_rst1.get_rows()) 
        id2 = extract_node_id(prev_rst2, i, step->get_src_col(1));
      if (step->get_dir() == dir_in) {
        labeled_id_t t = id1; id1 = id2; id2 = t;
      }
      f = std::move(f).thenValue([this, step, id1, id2](folly::Unit){
        return _ccgraph->GetEdgeViaLabeledId(step->get_label(), id1, id2, step->get_cc_ctx());
      }).thenValue([this, step, i, id1, id2](Edge* e){
        if (e == nullptr && step->get_dir() == dir_bidir) {
          return _ccgraph->GetEdgeViaLabeledId(step->get_label(), id2, id1, step->get_cc_ctx())
            .thenValue([this, step, i](Edge* e){
              step->get_rst().set(i, step->get_col_to_put(), ResultItem(e));
            });
        }
        step->get_rst().set(i, step->get_col_to_put(), ResultItem(e));
        return folly::makeFuture();
      });
    }
    return f;
  }
  VFuture ProcGetAllNeighbour(QueryStep* _step_) {
    auto step = dynamic_cast<GetAllNeighbourStep*>(_step_);
    auto prev = step->get_prev(0);
    // step->get_rst().copy_schema(prev->get_rst());
    // auto prev = dynamic_cast<NodeRstListStep*>(_step_->get_prev(0));
    // std::vector<Node*> start_nodes = prev->get_rst();
    auto f = folly::makeFuture();
    // todo: assert the type is node;
    // bug fix: there is racing!!!!!!
    for (size_t i = 0; i < prev->get_rst().get_rows(); i++) {
      f = std::move(f).thenValue([this, prev, step, i](folly::Unit){
        Node* n = (Node*)(prev->get_rst().get(i, step->get_src_col()));
        if (n == nullptr) {
          step->get_rst().append_row(i, prev->get_rst());
          step->get_rst().set(step->get_rst().get_rows()-1, step->get_col_to_put(), 0);
          return folly::makeFuture();
        }
        return _ccgraph->GetAllNeighbour(step->get_label(), n->_internal_id, step->get_dir(), step->get_cc_ctx())
          .thenValue([this, step, i, prev](sptr<std::vector<Node*>> rst){
            bool placed = false;
            for (Node* n : *rst) {
              if (step->_filter_node_label && n->_type != step->_node_label) {
                continue;
              }
              step->get_rst().append_row(i, prev->get_rst());
              step->get_rst().set(step->get_rst().get_rows()-1, step->get_col_to_put(), (ResultItem)n);
              placed = true;
            }
            if (!placed && step->_optional) {
              step->get_rst().append_row(i, prev->get_rst());
              step->get_rst().set(step->get_rst().get_rows()-1, step->get_col_to_put(), 0);
            }
          });
      });
    }
    return f;
  }
  VFuture ProcGetAllEdge(QueryStep* _step_) {
    auto step = dynamic_cast<GetAllEdgeStep*>(_step_);
    auto prev = step->get_prev(0);
    auto f = folly::makeFuture();
    // todo: assert the type is node;
    for (size_t i = 0; i < prev->get_rst().get_rows(); i++) {
      f = std::move(f).thenValue([this, prev, step, i](folly::Unit){
        Node* n = (Node*)(prev->get_rst().get(i, step->get_src_col()));
        if (n == nullptr) {
          step->get_rst().append_row(i, prev->get_rst());
          step->get_rst().set(step->get_rst().get_rows()-1, step->get_col_to_put(), 0);
          return folly::makeFuture();
        }
        return _ccgraph->GetAllEdge(step->get_label(), n->_internal_id, step->get_dir(), step->get_cc_ctx())
          .thenValue([this, step, i, prev](sptr<std::vector<Edge*>> rst){
            if (rst->size() == 0 && step->_optional) {
              step->get_rst().append_row(i, prev->get_rst());
              step->get_rst().set(step->get_rst().get_rows()-1, step->get_col_to_put(), 0);
            }
            for (Edge* e : *rst) {
              step->get_rst().append_row(i, prev->get_rst());
              step->get_rst().set(step->get_rst().get_rows()-1, step->get_col_to_put(), (ResultItem)e);
            }
          });
      });
    }
    return f;
  }
  VFuture GetAllNeighbourVarLenCore(GetAllNeighbourVarLenStep* step) {
    auto f = folly::makeFuture();
    if (step->has_rqst() == false) return f;
    auto prev = (step->get_prev(0));
    // LOG_VERBOSE("before pop rqst, the head rqst is %llu", std::get<0>(step->_rqst.front()));
    std::tuple<internal_id_t, label_t, size_t, size_t> rqst = step->pop_rqst();
    // LOG_VERBOSE("after pop rqst, the  rqst is %llu", std::get<0>(rqst));

    return _ccgraph->GetAllNeighbour(step->get_label(), std::get<0>(rqst), step->get_dir(), step->get_cc_ctx())
      // .via(folly::getGlobalCPUExecutor())
      .thenValue([this, step, rqst, prev](std::shared_ptr<std::vector<Node*>> rst){
        size_t depth=std::get<2>(rqst)+1;
        size_t src_row = std::get<3>(rqst);
        for (Node* n : *rst) {
          if (step->should_place_to_rst(n, depth, src_row)) {
            step->get_rst().append_row(src_row, prev->get_rst());
            step->get_rst().set(step->get_rst().get_rows()-1, step->get_col_to_put(), (ResultItem)n);
            step->get_rst().set(step->get_rst().get_rows()-1, step->get_col_to_put(1), depth);
          }
          if (step->terminate(n, depth) == false) {
            // duplicated request is handled inside
            step->append_rqst(n->_internal_id, n->_type, depth, src_row);
          }
        }
        if (step->has_rqst()) {
            return GetAllNeighbourVarLenCore(step);
          // });
        }
        return folly::makeFuture();
      });


    // auto rst = _ccgraph->GetAllNeighbour(step->get_label(), std::get<0>(rqst), step->get_dir(), step->get_cc_ctx()).wait().get();
    // size_t depth=std::get<2>(rqst)+1;
    // size_t src_row = std::get<3>(rqst);
    // for (Node* n : *rst) {
    //   if (step->should_place_to_rst(n, depth, src_row)) {
    //     step->get_rst().append_row(src_row, prev->get_rst());
    //     step->get_rst().set(step->get_rst().get_rows()-1, step->get_col_to_put(), (ResultItem)n);
    //     step->get_rst().set(step->get_rst().get_rows()-1, step->get_col_to_put(1), depth);
    //   }
    //   if (step->terminate(n, depth) == false) {
    //     // duplicated request is handled inside
    //     step->append_rqst(n->_internal_id, n->_type, depth, src_row);
    //   }
    // }
    // if (step->has_rqst()) return GetAllNeighbourVarLenCore(step);
    // return folly::makeFuture();

  }
  VFuture ProcGetAllNeigbhourVarLen(QueryStep* _step_) {
    auto step = dynamic_cast<GetAllNeighbourVarLenStep*>(_step_);
    auto prev = (_step_->get_prev(0));
    // step->get_rst().copy_schema(prev->get_rst());
    // std::vector<Node*> start_nodes = prev->get_rst();
    for (size_t i = 0; i < prev->get_rst().get_rows(); i++) {
      Node* n = (Node*)(prev->get_rst().get(i, step->get_src_col()));
      if (n == nullptr) throw QueryException("incorrect handlement of null ptr");
      if (step->_terminate(n, 0) == false) {
        step->append_rqst(n->_internal_id, n->_type, 0, i);
      }
      // special care for start node: we allow back edge on the path.
      // this is fine for ldbc query with node depth>1, but not ok for start node, which has depth=0
      if (step->should_place_to_rst(n, 0, i)) {
        step->get_rst().append_row(i, prev->get_rst());
        step->get_rst().set(step->get_rst().get_rows()-1, step->get_col_to_put(), (ResultItem)n);
        step->get_rst().set(step->get_rst().get_rows()-1, step->get_col_to_put(1), 0);
      } else if (step->_place_to_rst(n, 0) == false) {
        step->_hist_rsts.insert(std::make_tuple(n->_internal_id, i));
      }
      // explain: if min_depth=0, start node is palced into rst;
      // if min_depth=1, start node may be insert into rst due to back edge, which should be avoided.
      // back edge for further node is however acceptable
    }
    // auto f = folly::makeFuture();
    // f = std::move(f).thenValue([this, step](folly::Unit){
    return GetAllNeighbourVarLenCore(step).thenValue([](folly::Unit){
      // LOG_VERBOSE("get varlen future is ready");
    });
    // });
    // return f;
  }

  VFuture ProcCreateNode(QueryStep* _step_) {
    auto step = dynamic_cast<CreateNodeStep*>(_step_);
    auto prev = step->get_prev(0);
    if (prev->get_rst().get_type(step->get_src_col()) != Result::kLabeledNodeId)
      throw QueryException("Create node src must be labeled node id");
    // auto f = folly::makeFuture();
    // todo: do we need multi row insertion?
    if (prev->get_rst().get_rows() != 1) {
      throw QueryException("Only support single insertion");
    }
    labeled_id_t id = prev->get_rst().get(0, step->get_src_col());
    id.label = step->get_label();
    return _ccgraph->InsertNode(step->get_label(), id, step->get_cc_ctx())
      .thenValue([this, step](Node* n){
        step->get_rst().set(0, step->get_col_to_put(), (ResultItem)n);
      });
    // for (size_t i = 0; i < prev->get_rst().get_rows(); i++) {
    //   labeled_id_t id = prev->get_rst().get(i, step->get_src_col());
    //   id.label = step->get_label();
    //   f = std::move(f).thenValue([this, id, step](folly::Unit){
    //     return _ccgraph->InsertNode(step->get_label(), id, step->get_cc_ctx());
    //   }).thenValue([this, step, i](Node* n){
    //     step->get_rst().set(i, step->get_col_to_put(), (ResultItem)n);
    //   });
    // }
    // return f;
  }

  VFuture ProcUpdateNode(QueryStep* _step_) {
    auto step = dynamic_cast<UpdateNodeStep*>(_step_);
    auto prev = step->get_prev(0);
    Result::ColumnType src_col_type = prev->get_rst().get_type(step->get_src_col());
    if (src_col_type != Result::kLabeledNodeId &&
        src_col_type != Result::kNode)
      throw QueryException("Update node src must be node id or node");
    auto f = folly::makeFuture();
    for (size_t i = 0; i < prev->get_rst().get_rows(); i++) {
      f = std::move(f).thenValue([this, step, src_col_type, prev, i](folly::Unit){
        if (src_col_type == Result::kLabeledNodeId) {
          labeled_id_t id;
          id = prev->get_rst().get(i, step->get_src_col());
          id.label = step->get_label();
          return _ccgraph->UpdateNodeViaLabeledId(step->get_label(), id, step->get_cc_ctx());
        }
        else {// todo: may need to check the label, see if matching
          internal_id_t id = ((Node*)(prev->get_rst().get(i, step->get_src_col())))->_internal_id;
          return _ccgraph->UpdateNodeViaInternalId(id, step->get_cc_ctx());
        }
      }).thenValue([this, step, i](Node* n){
        step->get_rst().set(i, step->get_col_to_put(), (ResultItem)n);
      });
    }
    return f;
  }

  VFuture ProcDeleteNode(QueryStep* _step_) {
    auto step = dynamic_cast<DeleteNodeStep*>(_step_);
    auto prev = step->get_prev(0);
    Result::ColumnType src_col_type = prev->get_rst().get_type(step->get_src_col());
    if (src_col_type != Result::kUINT64 &&
        src_col_type != Result::kNode)
      throw QueryException("Delete node src must be node id or node");
    auto f = folly::makeFuture();
    for (size_t i = 0; i < prev->get_rst().get_rows(); i++) {
      if (src_col_type == Result::kUINT64) {
        labeled_id_t id = prev->get_rst().get(i, step->get_src_col());
        id.label = step->get_label();
        f = std::move(f).thenValue([this, id, step](folly::Unit){
          return _ccgraph->DeleteNodeViaLabeledId(step->get_label(), id, step->get_cc_ctx());
        }).thenValue([this, step, i](Node* n){
          // we don't need to place the result?
          // step->get_rst().set(i, step->get_col_to_put(), (ResultItem)n);
        });
      } else {// todo: may need to check the label, see if matching
        internal_id_t id = ((Node*)(prev->get_rst().get(i, step->get_src_col())))->_internal_id;
      f = std::move(f).thenValue([this, id, step](folly::Unit){
        return _ccgraph->DeleteNodeViaInternalId(id, step->get_cc_ctx());
      }).thenValue([this, step, i](Node* n){
          // we don't need to place the result?
        // step->get_rst().set(i, step->get_col_to_put(), (ResultItem)n);
      });
      }
    }
    return f;
  }


  VFuture ProcCreateEdge(QueryStep* _step_) {
    auto step = dynamic_cast<CreateEdgeStep*>(_step_);
    auto & prev1 = step->get_prev(0)->get_rst();
    auto & prev2 = step->get_prev(1)->get_rst();
    bool prev1_single = (prev1.get_rows() == 1), 
         prev2_single = (prev2.get_rows() == 1);
    if (!prev1_single && !prev2_single) {
      if (prev1.get_rows() !=  prev2.get_rows()) {
        throw QueryException("Mismatch prev1 and prev2");
      }
    }
    size_t n_rows = std::max(prev1.get_rows(),  prev2.get_rows());
    auto f = folly::makeFuture();
    for (size_t i = 0; i < n_rows; i++) {
      labeled_id_t id1 = extract_node_id(prev1, prev1_single?0:i, step->get_src_col(0));
      labeled_id_t id2 = extract_node_id(prev2, prev2_single?0:i, step->get_src_col(1));
      f = std::move(f).thenValue([this, id1, id2, step](folly::Unit)mutable{
        if (step->get_dir() == dir_in) {
          labeled_id_t t = id1; id1 = id2; id2 = t;
        }
        return _ccgraph->CreateEdgeViaLabeledId(step->get_label(), id1, id2, step->get_cc_ctx());
      }).thenValue([this, step, i](Edge* e){
        step->get_rst().set(i, step->get_col_to_put(), (ResultItem)e);
      });
    }
    return f;
  }

  labeled_id_t extract_id1(Result & row, size_t i, size_t j) {
    ResultItem val = row.get(i, j);
    Result::ColumnType ty = row.get_type(j);
    if (ty == Result::kLabeledNodeId) {
      // fixme: check that const step has the correct label
      if (row.get_rows() != 1) {throw QueryException("is labeled node id is in result, the step must be a const step. The label should also be set");}
      return labeled_id_t(row.get(i, j), row.get_const_label());
    }
    if (val == 0) throw QueryException("Incorrect handle of null pointer ");
    if (ty == Result::kNode) return ((Node*)val)->_external_id;
    if (ty == Result::kEdge) return ((Edge*)val)->_external_id1;
    throw QueryException("Node id must be extract from correct type");
  }
  labeled_id_t extract_id2(Result & row, size_t i, size_t j) {
    ResultItem val = row.get(i, j);
    Result::ColumnType ty = row.get_type(j);
    if (ty == Result::kLabeledNodeId) {
      // fixme: check that const step has the correct label
      if (row.get_rows() != 1) {throw QueryException("is labeled node id is in result, the step must be a const step. The label should also be set");}
      return labeled_id_t(row.get(i, j), row.get_const_label());
    }
    if (val == 0) throw QueryException("Incorrect handle of null pointer ");
    if (ty == Result::kNode) return ((Node*)val)->_external_id;
    if (ty == Result::kEdge) return ((Edge*)val)->_external_id2;
    throw QueryException("Node id must be extract from correct type");
  }
  // todo: when updating using Edge*, there should be no need to go through 
  // the index again!!!! This is like the index-free adjacency case
  VFuture ProcUpdateEdge(QueryStep* _step_) {
    auto step = dynamic_cast<UpdateEdgeStep*>(_step_);
    auto & prev1 = step->get_prev(0)->get_rst();
    auto & prev2 = step->get_prev(1)->get_rst();
    bool prev1_single = (prev1.get_rows() == 1), 
         prev2_single = (prev2.get_rows() == 1);
    if (!prev1_single && !prev2_single) {
      if (prev1.get_rows() !=  prev2.get_rows()) {
        throw QueryException("Mismatch prev1 and prev2");
      }
    }
    size_t n_rows = std::max(prev1.get_rows(),  prev2.get_rows());
    auto f = folly::makeFuture();
    for (size_t i = 0; i < n_rows; i++) {
      labeled_id_t id1 = extract_id1(prev1, prev1_single?0:i, step->get_src_col(0));
      labeled_id_t id2 = extract_id2(prev2, prev2_single?0:i, step->get_src_col(1));
      f = std::move(f).thenValue([this, id1, id2, step](folly::Unit){
        return _ccgraph->UpdateEdge(step->get_label(), id1, id2, step->get_cc_ctx());
      }).thenValue([this, step, i](Edge* e){
        step->get_rst().set(i, step->get_col_to_put(), (ResultItem)e);
      });
    }
    return f;
  }

  // todo: when deleting using Edge*, there should be no need to go through 
  // the index again!!!! This is like the index-free adjacency case
  VFuture ProcDeleteEdge(QueryStep* _step_) {
    auto step = dynamic_cast<DeleteEdgeStep*>(_step_);
    auto & prev1 = step->get_prev(0)->get_rst();
    auto & prev2 = step->get_prev(1)->get_rst();
    bool prev1_single = (prev1.get_rows() == 1), 
         prev2_single = (prev2.get_rows() == 1);
    if (!prev1_single && !prev2_single) {
      if (prev1.get_rows() !=  prev2.get_rows()) {
        throw QueryException("Mismatch prev1 and prev2");
      }
    }
    size_t n_rows = std::max(prev1.get_rows(),  prev2.get_rows());
    auto f = folly::makeFuture();
    for (size_t i = 0; i < n_rows; i++) {
      labeled_id_t id1 = extract_id1(prev1, prev1_single?0:i, step->get_src_col(0));
      labeled_id_t id2 = extract_id2(prev2, prev2_single?0:i, step->get_src_col(1));
      f = std::move(f).thenValue([this, id1, id2, step](folly::Unit){
        return _ccgraph->DeleteEdge(step->get_label(), id1, id2, step->get_cc_ctx());
      }).thenValue([this, step, i](Edge*){});
    }
    return f;
  } 

 public:
  Worker() = delete;
  Worker(CCGraph* _ccgraph, SchemaManager* _schema) : _ccgraph(_ccgraph), _schema(_schema) {
    // this->_ccgraph = _ccgraph;
    // this->_schema = _schema;
  }
  VFuture ProcessOneStep(QueryStep* step) {
    step->set_started();
    auto f = folly::makeFuture();
    switch (step->get_type()) {
      case kNoOp: {
        LOG_VERBOSE("Processing NoOp");
        break;
      }
      case kIntConst: {
        LOG_VERBOSE("Processing IntConst");
        break;
      }
      case kConst: {
        LOG_VERBOSE("Processing Const");
        break;
      }
      case kNodeId: {
        LOG_VERBOSE("Processing NodeId");
        break;
      }
      case kAlgeo: {
        LOG_VERBOSE("Processing Algeo");
        ProcAlgeoOp(step); break;
      }

      case kGetNode: {
        LOG_VERBOSE("Processing GetNode");
        f=std::move(ProcGetNode(step)); break;
      }
      case kGetSingleEdge: {
        LOG_VERBOSE("Processing GetSingleEdge");
        f = std::move(ProcGetSingleEdge(step)); break;
      }

      case kGetAllNeighbor: {
        LOG_VERBOSE("Processing GetAllNeighbor");
        f=std::move(ProcGetAllNeighbour(step)); break;
      }
      case kGetAllNeighborVarLen: {
        LOG_VERBOSE("Processing GetAllNeighborVarLen");
        f=std::move(ProcGetAllNeigbhourVarLen(step)).thenValue([](folly::Unit){
          // LOG_VERBOSE("porcess one step: get var len is ready");
        }); break;
      }
      case kGetAllEdge: {
        LOG_VERBOSE("Processing GetAllEdge");
        f=std::move(ProcGetAllEdge(step)); break;
      }

      case kCreateNode: {
        LOG_VERBOSE("Processing CreateNode");
        f=std::move(ProcCreateNode(step)); break;
      }
      case kUpdateNode: {
        LOG_VERBOSE("Processing UpdateNode");
        f=std::move(ProcUpdateNode(step)); break;
      }
      case kDeleteNode: {
        LOG_VERBOSE("Processing DeleteNode");
        f=std::move(ProcDeleteNode(step)); break;
      }

      case kCreateEdge: {
        LOG_VERBOSE("Processing CreateEdge");
        f=std::move(ProcCreateEdge(step)); break;
      }
      case kUpdateEdge: {
        LOG_VERBOSE("Processing UpdateEdge");
        f=std::move(ProcUpdateEdge(step)); break;
      }
      case kDeleteEdge: {
        LOG_VERBOSE("Processing DeleteEdge");
        f=std::move(ProcDeleteEdge(step)); break;
      }

      case kGetEdgeVia: {
        LOG_VERBOSE("Processing GetEdgeVia");
        throw Unimplemented("No prop index");
      }
      case kPlaceProp: {
        LOG_VERBOSE("Processing PlaceProp");
        ProcPlacePropCol(step); break;
      }
      case kPlacePropBack: {
        LOG_VERBOSE("Processing PlacePropBack");
        ProcPlacePropColBack(step); break;
      }
      case kIf: {
        LOG_VERBOSE("Processing If");
        ProcIf(step); break;
      }
      case kEndIf: {
        LOG_VERBOSE("Processing EndIf");
        break;
      }
      case kMerge: {
        LOG_VERBOSE("Processing Merge");
        throw Unimplemented("No need to implement merge now");
      }
      case kFilter: {
        LOG_VERBOSE("Processing Filter");
        ProcFilter(step); break;
      }
      case kFilterSimple: {
        LOG_VERBOSE("Processing FilterSimple");
        ProcFilterSimple(step); break;
      }
      case kFilterByLabel: {
        LOG_VERBOSE("Processing FilterByLabel");
        ProcFilterByLabel(step); break;
      }
      case kGroupBy: {
        LOG_VERBOSE("Processing GroupBy");
        ProcGroupBy(step); break;
      }
      case kSort: {
        LOG_VERBOSE("Processing Sort");
        ProcSort(step); break;
      }
    }
    step->set_done();
    return f;
  }
  VFuture ProcessStepRecursive(QueryStep* step) {
    return ProcessOneStep(step)
      // .via(folly::getGlobalCPUExecutor())
      .thenValue([this, step](folly::Unit){
        if (Config::get()->print_mid_rst) {
          LOG_VERBOSE("printing mid result");
          step->get_rst().print();
        }
        size_t nexts = step->get_next_count();
        if (nexts > 1) {
          LOG_ERROR("More than one next, not supported");
          throw Unimplemented("Sequential execution");
        } else if (nexts == 0) {
          LOG_VERBOSE("no more next steps");
          return folly::makeFuture();
        }
        // LOG_VERBOSE("running next step");
        QueryStep* next_step = step->get_next(0);
        return ProcessStepRecursive(next_step);
      });
  }
  VFuture ProcessQuery(std::shared_ptr<Query> q) {
    // the initial point of processing a query, no reentrant
    QueryStep* start = q->get_first_step();
    return ProcessStepRecursive(start)
      .thenValue([this, q](folly::Unit){
        LOG_DEBUG("no error, trying to commit");
        if (_ccgraph->CommitCheck(q->get_cc_ctx())) {
          q->write_final_rst();
          _ccgraph->Commit(q->get_cc_ctx());
          q->_rc = kOk;
        } else {
          _ccgraph->Abort(q->get_cc_ctx());
          q->_rc = kAbort;
        }
        // todo: reply to client
      }).thenError(folly::tag_t<std::exception&>{}, [this,q](std::exception const & e){
        if (dynamic_cast<const AbortException*>(&e)) {
          // std::cout << "Abort Error : " <<  e.what() << "\n";
          _ccgraph->Abort(q->get_cc_ctx());
          q->_rc = kAbort;
        } else {
          LOG_INFO("Fatal error: %s", e.what());
          // todo: exit gracefully
          exit(1);
        }
      });
  }
};