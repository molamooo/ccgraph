#pragma once

#include "node.hpp"
#include "edge.hpp"

#include "exceptions.hpp"

#include "string_server.hpp"
#include "config.hpp"

#include <vector>
#include <unordered_set>
#include <iostream>

using ResultItem = uint64_t;

using ResultRow = std::vector<ResultItem>;

class Result {
  friend class SeqQueryBuilder;
 public:
  enum ColumnType {
    // same as in type.hpp
    kUINT64 = 0,
    kUINT32,
    kUINT8,
    kDOUBLE,
    kSTRING,
    kINT64,
    
    kInnerNodeId,
    kLabeledNodeId,
    kNode,
    kEdge,

    kSet,
  };
 private:
  label_t _label_for_labeled_id = 0;
  // std::vector<ResultItem> _rsts;
  // std::vector<std::unordered_set<ResultItem>*> _stored_sets;
  std::vector<ResultRow> _rsts;
  size_t _n_cols = 0, _n_rows = 0;
  std::vector<ColumnType> _col_types;
  std::vector<std::string> _col_alias;
 public:
  Result() {}
  Result(const Result & rst) {
    throw FatalException("Not allowed to copy a rst. If build with same schema, use set_schema instead");
  }
  Result(Result && rst) {
    _label_for_labeled_id = rst._label_for_labeled_id;
    // _stored_sets = std::move(rst._stored_sets);
    _rsts = std::move(rst._rsts);
    _n_cols = rst._n_cols;
    _n_rows = rst._n_rows;
    _col_types = std::move(rst._col_types);
    _col_alias = std::move(rst._col_alias);
    rst._n_rows = 0;
  }
  Result & operator=(Result && rst) {
    _label_for_labeled_id = rst._label_for_labeled_id;
    // _stored_sets = std::move(rst._stored_sets);
    _rsts = std::move(rst._rsts);
    _n_cols = rst._n_cols;
    _n_rows = rst._n_rows;
    _col_types = std::move(rst._col_types);
    _col_alias = std::move(rst._col_alias);
    rst._n_rows = 0;
    return *this;
  }
  Result & operator=(const Result & rst) {
    throw FatalException("Not allowed to copy a rst. If build with same schema, use set_schema instead");
  }

  void copy_schema(const Result & rst) {
    _rsts.clear();
    _n_cols = rst._n_cols;
    _n_rows = 0;
    _col_types = rst._col_types;
    _col_alias = rst._col_alias;
  }
  std::vector<ColumnType> get_schema() const { return _col_types; }
  void set_schema(std::vector<ColumnType> types,
                  std::vector<std::string> col_alias={}) {
    _rsts.clear();
    _rsts.shrink_to_fit();
    _n_rows = 0;
    _col_types = types;
    _n_cols = types.size();

    if (col_alias.size() != 0 && col_alias.size() != _n_cols) {
      throw FatalException("set schema, but the size of col_alias is not the same as types");
    }
    if (col_alias.size() == 0) {
      _col_alias = std::vector<std::string>(_n_cols, "");
    } else {
      _col_alias = col_alias;
    }
  }

  void force_change_schema(const size_t col, const ColumnType ty) {
    _col_types[col] = ty;
  }
  void append_schema(ColumnType ty, std::string col_alias="") {
    _col_types.push_back(ty);
    _n_cols++;
    for (auto & row : _rsts) {
      row.resize(_n_cols);
    }
    _col_alias.push_back(col_alias);
  }

  ResultRow & append_row() {
    _n_rows++;
    _rsts.push_back(ResultRow(_n_cols));
    return _rsts.at(_rsts.size()-1);
  }
  // void append_row(const ResultRow & row) {
  //   if (row.size() != _n_cols) throw QueryException("Mismatch col num");
  //   _rsts.push_back(row);
  //   _n_rows++;
  // }
  void append_row(const size_t i, const Result & rst) {
    // todo: check schema
    for (size_t col = 0; col < rst.get_cols(); col++) {
      if (rst.get_type(col) != get_type(col)) throw QueryException("Mismatch col type");
    }
    _rsts.push_back(rst._rsts.at(i));
    // todo: expend to fit?
    _rsts.back().resize(_n_cols);
    _n_rows++;
  }
  const ResultRow & get_row(size_t i) const {
    return _rsts.at(i);
  }

  size_t get_rows() const { return _n_rows; }
  void update_rows() { _n_rows = _rsts.size(); }
  size_t get_cols() const { return _n_cols; }

  ColumnType get_type(size_t col) const { return _col_types.at(col); }
  std::string get_col_alias(size_t col) const {return _col_alias.at(col);}
  size_t get_col_idx_by_alias(std::string alias) const {
    for (size_t i = 0; i < _col_alias.size(); i++) {
      if (_col_alias[i] == alias) return i;
    }
    throw FatalException(Formatter() << "The col alias is not found:" << alias);
  }
  ResultItem get(size_t i, size_t j) const { return _rsts.at(i).at(j); }
  static std::string get_string(const ResultItem val, const ColumnType ty) {
    std::stringstream ss;
    switch (ty) {
      case Result::kNode:
      case Result::kEdge: {
        // special case only for ldbc, which wants "isNew"
        if (val == 0) return "TRUE";
        return "FALSE";
      }
      case Result::kDOUBLE: {
        double d = *(double*)&val;
        ss << d;
        return ss.str();
      }
      case Result::kUINT8:  {
        uint8_t d = *(uint8_t*)&val;
        ss << d;
        return ss.str();
      }
      case Result::kUINT32:  {
        uint32_t d = *(uint32_t*)&val;
        ss << d;
        return ss.str();
      }
      case Result::kInnerNodeId:
      case Result::kLabeledNodeId:
      case Result::kUINT64: {
        uint64_t d = *(uint64_t*)&val;
        ss << d;
        return ss.str();
      }
      case Result::kINT64: {
        int64_t d = *(int64_t*)&val;
        ss << d;
        return ss.str();
      }
      case Result::kSTRING: { return "\"" + StringServer::get()->get(val) + "\""; }
      case Result::kSet: { return StringServer::get()->get(val); }
      default: throw FatalException("Unreachable");
    }
  }
  std::string get_string(size_t i, size_t j) const {
    return get_string(get(i, j), get_type(j));
  }
  void print() {
    // std::cout << "|";
    for (size_t j = 0; j < get_cols(); j++) {
      if (j > 0) std::cout << ", ";
      std::cout << get_col_alias(j);
    }
    std::cout << "\n";
    if (get_rows() == 0) std::cout << "< result is empty >\n";
    for (size_t i = 0; i < std::min(get_rows(), Config::get()->mid_rst_max_row); i++) {
      // std::cout << "|";
      for (size_t j = 0; j < get_cols(); j++) {
      if (j > 0) std::cout << ", ";
        std::cout << get_string(i, j);
      }
      std::cout << "\n";
    }
    if (get_rows() >= Config::get()->mid_rst_max_row) {
      std::cout << "This is not all. we have " << get_rows() << " rows in total\n";
    }
  }
  void set(size_t i, size_t j, ResultItem v) { _rsts.at(i)[j] = v; }

  std::vector<ResultRow> & get_rst_table() { return _rsts; }

  // void track_set(ResultItem set) {
  //   auto set_ptr = (std::unordered_set<ResultItem>*)set;
  //   _stored_sets.push_back(set_ptr);
  // }

  // void append_to_set(ResultItem set, ResultItem v) {
  //   auto set_ptr = (std::unordered_set<ResultItem>*)set;
  //   set_ptr->insert(v);
  // }
  label_t get_const_label() {return _label_for_labeled_id;}
  void set_const_label(const label_t l) {_label_for_labeled_id = l;}
};