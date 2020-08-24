#pragma once
#include "ldbc_c1.hpp"
#include "ali.hpp"

#include <string>

class QueryBuilder {
 public:
  void build_query(std::string query_name, std::vector<std::string> params, Query* q_ptr) {
    if (query_name == "ldbc_c1") {
      LDBCQuery1 q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_c2") {
      LDBCQuery2 q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_c3") {
      LDBCQuery3 q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_c4") {
      LDBCQuery4 q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_c5") {
      LDBCQuery5 q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_c6") {
      LDBCQuery6 q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_c7") {
      LDBCQuery7 q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_c8") {
      LDBCQuery8 q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_c9") {
      LDBCQuery9 q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_c10") {
      LDBCQuery10 q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_c11") {
      LDBCQuery11 q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_c12") {
      LDBCQuery12 q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_neighbour") {
      LDBCNeighbour q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_insert_person") {
      LDBCInsertPerson q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_insert_knows") {
      LDBCInsertKnows q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_query_person") {
      LDBCQuerPerson q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_insert_edge") {
      LDBCInsertEdge q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_insert_node") {
      LDBCInsertNode q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_query_comment") {
      LDBCQueryComment q; q.build(params, q_ptr);
    } else if (query_name == "ldbc_query_post") {
      LDBCQueryPost q; q.build(params, q_ptr);
    } else if (query_name == "ali_login") {
      AliTxn::AliLogin q; q.build(params, q_ptr);
    } else if (query_name == "ali_query_node") {
      AliTxn::AliQueryNode q; q.build(params, q_ptr);
    } else {
      throw FatalException(Formatter() << "No such query: " << query_name);
    }
  }
};