#pragma once
#include "query_step.hpp"
#include "schema.hpp"
#include <vector>

// class CCContex;

enum QueryRstColType {
  kNodeLabeledId,
  kLabel,
  kEdgeLabeledId1,
  kEdgeLabeledId2,
  kProp,
  kColValDirect,
  kPropOr,
  // kIsNull,
};

struct QueryRstCol {
  Result::ColumnType ty;
  size_t col_idx;
  QueryRstColType col_type;
  size_t prop_idx;
  std::string alias;

  size_t prop_idx2; // for ldbc, which returns either content or imagefile
};

class Query {
  friend class SeqQueryBuilder;
 private:
  std::vector<QueryStep*> _steps;
  std::vector<Result*> _rst_list ;
  QueryStep* _first_step;
  std::vector<QueryRstCol> rst_cols;

  Result final_rst;

  uint64_t _ts;

  uint64_t _txnid;
  CCContex* _cc_ctx = nullptr;

 public:
  RetCode _rc;
  std::string _abort_msg;
  QueryStep* get_first_step() { return _first_step; }
  QueryStep* get_step(const size_t idx) { return _steps.at(idx); }
  size_t get_nstep() { return _steps.size(); }
  CCContex* get_cc_ctx() const { return _cc_ctx; }
  uint64_t get_ts() const { return _ts; }
  uint64_t get_txnid() const { return _txnid; }

  void set_ts(uint64_t ts) { this->_ts = ts; }
  void set_txnid(uint64_t txnid) { this->_txnid = txnid; }
  void set_cc_ctx(CCContex* cc_ctx) { this->_cc_ctx = cc_ctx; }
  void write_final_rst() {
    Result & last_rst = _steps.back()->get_rst();
    for (size_t dst_col = 0; dst_col < rst_cols.size(); dst_col++) {
      final_rst.append_schema(rst_cols.at(dst_col).ty, rst_cols.at(dst_col).alias);
    }

    for (size_t row = 0; row < last_rst.get_rows(); row++) {
      auto & r_to_write = final_rst.append_row();
      for (size_t dst_col = 0; dst_col < rst_cols.size(); dst_col++) {
        size_t src_col = rst_cols.at(dst_col).col_idx;
        switch (rst_cols.at(dst_col).col_type) {
          // case kIsNull: {
          //   if (last_rst.get_type(src_col) == Result::kEdge || last_rst.get_type(src_col) == Result::kNode) {
          //     r_to_write[dst_col] = last_rst.get(row, src_col) == 0 ? StringServer::get()->touch("TRUE") : StringServer::get()->touch("FALSE");
          //   } else {
          //     throw FatalException("Can not extract prop from non-pointer col");
          //   }
          //   break;
          // }
          case kColValDirect: {
            if (last_rst.get_type(src_col) == Result::kEdge || last_rst.get_type(src_col) == Result::kNode) {
              r_to_write.at(dst_col) = last_rst.get(row, src_col) == 0 ? 0 : 1;
            } else {
              r_to_write.at(dst_col) = last_rst.get(row, src_col);
            }
            break;
          }
          case kProp: {
            if (last_rst.get_type(src_col) == Result::kNode) {
              Node* n = (Node*)last_rst.get(row, src_col);
              SchemaManager::get()->get_prop(n->_prop, n->_type, rst_cols.at(dst_col).prop_idx, (uint8_t*)(&r_to_write.at(dst_col)));
              break;
            } else if (last_rst.get_type(src_col) == Result::kEdge) {
              Edge* e = (Edge*)last_rst.get(row, src_col);
              SchemaManager::get()->get_prop(e->_prop, e->_label, rst_cols.at(dst_col).prop_idx, (uint8_t*)(&r_to_write.at(dst_col)));
              break;
            } else {
              throw FatalException("Can not extract prop from non-pointer col");
            }
          }
          case kPropOr: {
            ResultItem val1, val2;
            uint8_t* prop_head; label_t type;
            if (last_rst.get_type(src_col) == Result::kNode) {
              Node* n = (Node*)last_rst.get(row, src_col);
              prop_head = n->_prop; type = n->_type;
            } else if (last_rst.get_type(src_col) == Result::kEdge) {
              Edge* e = (Edge*)last_rst.get(row, src_col);
              prop_head = e->_prop; type = e->_label;
            } else {
              throw FatalException("Can not extract prop from non-pointer col");
            }
            SchemaManager::get()->get_prop(prop_head, type, rst_cols.at(dst_col).prop_idx, (uint8_t*)&val1);
            SchemaManager::get()->get_prop(prop_head, type, rst_cols.at(dst_col).prop_idx2, (uint8_t*)&val2);
            if (val1 == 0) r_to_write.at(dst_col) = val2;
            else r_to_write.at(dst_col) = val1;
            break;
          }
          case kEdgeLabeledId1: {
            Edge* e = (Edge*)last_rst.get(row, src_col);
            r_to_write.at(dst_col) = e->_external_id1.id;
            break;
          }
          case kEdgeLabeledId2: {
            Edge* e = (Edge*)last_rst.get(row, src_col);
            r_to_write.at(dst_col) = e->_external_id2.id;
            break;
          }
          case kNodeLabeledId: {
            Node* n = (Node*)last_rst.get(row, src_col);
            r_to_write.at(dst_col) = n->_external_id.id;
            break;
          }
          case kLabel: throw Unimplemented("No query should return label as rst");
        }
      }
    }
  }

  Result& get_final_rst() {
    return final_rst;
  }
};