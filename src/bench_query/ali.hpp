#pragma once

#include "query_step.hpp"
#include "build_seq.hpp"

#include "string_server.hpp"

namespace AliTxn {

SchemaManager* _schema = SchemaManager::get();

static label_t Node_L, Edge_L;

static void check_ali_label_initialized() {
  static bool ready = false;
  static std::mutex mu;
  if (ready == true) {
    return;
  }
  mu.lock();
  if (ready == true) {
    mu.unlock(); return;
  }
  Node_L = _schema->get_label_by_name("`2048`");
  Edge_L = _schema->get_label_by_name("123456789");
  mu.unlock();
}

class AliLogin {
 private:
  uint64_t _person_id;

 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_ali_label_initialized();
    _person_id = std::stoull(params[0]);
    SeqQueryBuilder builder;

    StepCtx start_node_id_ctx = builder.put_const(Result::kLabeledNodeId, _person_id, "param_person_id", Node_L);
    StepCtx const_1 = builder.put_const(Result::kUINT64, 1, "1");

    start_node_id_ctx
      .get_node(Node_L, "param_person_id", "start_person_node")
      .filter_simple([](ResultItem v){return v != 0;}, "start_person_node")
      // fix: the original ldbc result contains the start node. this is wrong, but we need to be the same
      .get_neighbour_varlen(Edge_L, dir_out, 0, std::numeric_limits<uint64_t>::max(), "start_person_node", {"friend_node", "depth"}) // done: both direction
      .update_node(Node_L, "friend_node", "node_to_write")
      // .place_prop(_schema->get_prop_idx(Node_L, "version"), Result::kUINT64, "node_to_write", "version")
      // .algeo(MathOp::kAdd, const_1, {"version", "1"}, "updated_version")
      // .place_prop_back(Node_L, "version", Result::kUINT64, "updated_version", "node_to_write")
      .algeo(MathOp::kAdd, const_1, {"node_to_write", "1"}, _schema->get_prop_idx(Node_L, "version"))
      .select_group(
        {}, 
        {"friend_node"}, 
        {GroupByStep::kCount}, 
        {{}}, 
        {"count"})
      ;
    builder.new_col_to_ret(Result::kUINT64, "count", "count");
    builder.write_to_query(q_ptr);
  }
};


class AliQueryNode {
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_ali_label_initialized();
    uint64_t _person_id = std::stoull(params[0]);
    SeqQueryBuilder builder;

    StepCtx start_node_id_ctx = builder.put_const(Result::kLabeledNodeId, _person_id, "param_person_id", Node_L);

    start_node_id_ctx
      .get_node(Node_L, "param_person_id", "start_person_node")
      .filter_simple([](ResultItem v){return v != 0;}, "start_person_node");
    builder.new_col_to_ret(Result::kUINT64, "start_person_node", _schema->get_prop_idx(Node_L, "version"), "version");
    builder.write_to_query(q_ptr);
  }
};

};