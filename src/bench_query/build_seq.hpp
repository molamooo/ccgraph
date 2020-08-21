#include "query_step.hpp"
#include "query.hpp"
#include "schema.hpp"

class SeqQueryBuilder;
struct StepCtx {
  using str = std::string;
  size_t _step_id;
  QueryStep* _step = nullptr;
  SeqQueryBuilder* _builder = nullptr;
  // std::vector<size_t> _wrote_cols;
  // std::vector<size_t> _key_cols;
  // StepCtx get_node(label_t label, StepCtx* col_src = nullptr, std::string dst_col_alias="");
  StepCtx get_node(label_t label, std::string src_col_alias, std::string dst_col_alias="");
  // min_depth <= depth <= max_depth
  // StepCtx get_neighbour_varlen(label_t label, dir_t dir, size_t min_depth, size_t max_depth, StepCtx* col_src = nullptr, std::string dst_col_alias="");
  StepCtx get_neighbour_varlen(label_t label, dir_t dir, size_t min_depth, size_t max_depth, std::string src_col_alias, std::vector<std::string> dst_col_alias={"", ""});

  // StepCtx get_all_neighbour(label_t label, dir_t dir, StepCtx* col_src = nullptr, std::string dst_col_alias="");
  StepCtx get_all_neighbour(label_t label, dir_t dir, std::string src_col_alias, std::string dst_col_alias="", bool optional=false);
  StepCtx get_all_neighbour(label_t label, dir_t dir, std::string src_col_alias, std::string dst_col_alias, label_t dst_node_label, bool optional = false);

  // StepCtx place_prop(size_t prop_idx, Result::ColumnType ty, StepCtx* col_src = nullptr, std::string dst_col_alias="");
  StepCtx place_prop(label_t label, std::string prop_name, Result::ColumnType ty, std::string src_col_alias, std::string dst_col_alias);
  StepCtx place_prop(size_t prop_idx, Result::ColumnType ty, std::string src_col_alias, std::string dst_col_alias="");

  // StepCtx filter(StepCtx & oprand, CompareOp op, StepCtx* col_src = nullptr);
  StepCtx filter(StepCtx & oprand, CompareOp op, std::vector<std::string> src_col_alias);
  // StepCtx filter_simple(std::function<bool(ResultItem val)> lambda, StepCtx* col_src = nullptr);
  StepCtx filter_simple(std::function<bool(ResultItem val)> lambda, std::string src_col_alias);
  // StepCtx filter_label(label_t label, CompareOp op, StepCtx* col_src = nullptr);
  StepCtx filter_label(label_t label, CompareOp op, std::string src_col_alias);

  // StepCtx get_all_edge(label_t label, dir_t dir, StepCtx* col_src = nullptr, std::string dst_col_alias="");
  StepCtx get_all_edge(label_t label, dir_t dir, std::string src_col_alias, std::string dst_col_alias="", bool optional=false);

  // StepCtx get_single_edge(label_t label, dir_t dir, StepCtx* second_id, StepCtx* col_src=nullptr, std::string dst_col_alias="");
  StepCtx get_single_edge(label_t label, dir_t dir, StepCtx* second_id, std::vector<std::string> src_col_alias, std::string dst_col_alias="");
  StepCtx get_single_edge(label_t label, dir_t dir, std::vector<std::string> src_col_alias, std::string dst_col_alias="");

  // StepCtx algeo(StepCtx & oprand2, size_t src_col2, MathOp op, StepCtx* col_src = nullptr, std::string dst_col_alias="");
  StepCtx algeo(MathOp op, StepCtx & oprand2, std::vector<std::string> src_col_alias, std::string dst_col_alias="");
  StepCtx algeo(MathOp op, std::vector<std::string> src_col_alias, std::string dst_col_alias="");
  StepCtx sort(std::vector<std::string> src_cols_alias, std::vector<bool> orders, uint64_t limit = UINT64_MAX, uint64_t skip = 0);
  StepCtx select_group(
    std::vector<std::string> key_cols_alias,
    std::vector<std::string> src_cols_alias,
    std::vector<GroupByStep::Aggregator> group_ops,
    std::vector<std::vector<FilterCtx>> filters,
    std::vector<std::string> dst_cols_alias);

  StepCtx place_prop_back(label_t label, std::string prop_name, Result::ColumnType ty, StepCtx & val_ctx, std::string src_col_alias, std::string dst_col_alias);
  StepCtx insert_node(label_t label, std::string src_col_alias, std::string dst_col_alias);
  StepCtx insert_edge(label_t label, StepCtx & node2, std::vector<std::string> src_cols_alias, dir_t dir, std::string dst_col_alias);

  StepCtx put_const(Result::ColumnType ty, ResultItem val, std::string col_alias = "", label_t const_label = 0);
};

class SeqQueryBuilder {
 public:
  size_t _step_id = 0;

  std::vector<QueryStep*> steps;
  std::vector<Result*> result_list;

  QueryStep* first_step = nullptr;

  std::vector<QueryRstCol> rst_cols;

  std::unordered_map<std::string, std::string> col_get_all_edge_to_col_src_node;

  void write_to_query(Query* q) {
    q->_first_step = first_step;
    q->_steps = std::move(steps);
    q->_rst_list = std::move(result_list);
    q->rst_cols = std::move(rst_cols);
  }

  void new_col_to_ret(Result::ColumnType ty,  std::string col_alias, std::string dst_col_alias="") {
    size_t col = steps.back()->get_rst().get_col_idx_by_alias(col_alias);
    Result* final_table = &steps.back()->get_rst();
    Result::ColumnType col_type = final_table->get_type(col);
    if (ty != Result::kUINT64 && (col_type == Result::kEdge || col_type == Result::kNode)) throw FatalException("the rst col to return must be value only when only providing a col");
    rst_cols.push_back({ty, col, QueryRstColType::kColValDirect, std::numeric_limits<size_t>::max(), dst_col_alias});
  }

  void new_col_to_ret(Result::ColumnType ty,  std::string col_alias, size_t prop_idx, std::string dst_col_alias="") {
    size_t col = steps.back()->get_rst().get_col_idx_by_alias(col_alias);
    Result* final_table = &steps.back()->get_rst();
    Result::ColumnType col_type = final_table->get_type(col);
    if (col_type != Result::kEdge && col_type != Result::kNode) throw FatalException("the rst col to return must be prop when providing a col and the prop idx");
    rst_cols.push_back({ty, col, QueryRstColType::kProp, prop_idx, dst_col_alias});
  }

  void new_col_to_ret(Result::ColumnType ty,  std::string col_alias, QueryRstColType spectial_col_type, std::string dst_col_alias="") {
    size_t col = steps.back()->get_rst().get_col_idx_by_alias(col_alias);
    if (spectial_col_type == kColValDirect || spectial_col_type == kProp)
      throw FatalException("spectial col type must meta");
    if (spectial_col_type == kLabel)
      throw Unimplemented("Current no query should return label as result");
    Result* final_table = &steps.back()->get_rst();
    Result::ColumnType col_type = final_table->get_type(col);
    if (col_type != Result::kEdge && col_type != Result::kNode) 
      throw FatalException("the rst col to return must be prop when providing a col and meta to ret");
    rst_cols.push_back({ty, col, spectial_col_type, std::numeric_limits<size_t>::max(), dst_col_alias});
  }


  void reset() { _step_id = 0; }

  // place a 1*1 const.
  StepCtx put_const(Result::ColumnType ty, ResultItem val, std::string col_alias = "", label_t const_label = 0) {
    ConstStep* step = new ConstStep;
    steps.push_back(step);
    if (first_step == nullptr) first_step = step;
    StepCtx ctx; ctx._step = step; ctx._step_id = this->_step_id++;; ctx._builder = this;

    Result* rst = new Result;
    result_list.push_back(rst);
    step->set_rst(rst); 
    rst->append_schema(ty, col_alias);
    rst->set_const_label(const_label);

    // step->set_col_to_put({0});
    // ctx._wrote_cols = {0};

    rst->append_row();
    rst->set(0, 0, val);
    // LOG_VERBOSE("just put a const, print the result:");
    // rst->print();
    return ctx;
  }
};

StepCtx StepCtx::put_const(Result::ColumnType ty, ResultItem val, std::string col_alias, label_t const_label) {
  ConstStep* step = new ConstStep;
  this->_builder->steps.push_back(step);
  if (_builder->first_step == nullptr) _builder->first_step = step;
  StepCtx ctx; ctx._step = step; ctx._step_id = this->_step_id++;; ctx._builder = this->_builder;

  Result* rst = new Result;
  _builder->result_list.push_back(rst);
  step->set_rst(rst); 
  rst->append_schema(ty, col_alias);
  rst->set_const_label(const_label);

  // step->set_col_to_put({0});
  // ctx._wrote_cols = {0};

  this->_step->append_next(step);

  rst->append_row();
  rst->set(0, 0, val);
  // LOG_VERBOSE("just put a const, print the result:");
  // rst->print();
  return ctx;
}

/**
 * @brief if get node is from edges in result table, the label can be ignored
 *    person->create->message(comment, post)
 * 
 */
StepCtx StepCtx::get_node(label_t label, std::string src_col_alias, std::string dst_col_alias) {
  // share the same result table.
  GetNodeStep* step = new GetNodeStep;
  _builder->steps.push_back(step);
  if (_builder->first_step == nullptr) _builder->first_step = step;
  StepCtx ctx; ctx._step = step; ctx._step_id = this->_step_id++; ctx._builder = this->_builder;

  step->set_rst(&this->_step->get_rst());

  size_t col_to_put = step->get_rst().get_cols();
  step->get_rst().append_schema(Result::kNode, dst_col_alias);

  this->_step->append_next(step);
  step->set_prev({this->_step});

  // for those who put result for get node, always place at first:
  // const, get_edge
  step->set_src_col({step->get_rst().get_col_idx_by_alias(src_col_alias)});
  step->set_col_to_put({col_to_put});
  // ctx._wrote_cols = {col_to_put};

  step->set_label(label);
  if (_builder->col_get_all_edge_to_col_src_node.find(src_col_alias) != _builder->col_get_all_edge_to_col_src_node.end())
    step->_src_node_col_alias = _builder->col_get_all_edge_to_col_src_node[src_col_alias];

  // LOG_VERBOSE("just made a get node step, print the result:");
  // step->get_rst().print();
  return ctx;
}


StepCtx StepCtx::get_neighbour_varlen(label_t label, dir_t dir, size_t min_depth, size_t max_depth, std::string src_col_alias, std::vector<std::string> dst_col_alias) {
  // do not share the same result table
  GetAllNeighbourVarLenStep* step = new GetAllNeighbourVarLenStep;
  _builder->steps.push_back(step);
  if (_builder->first_step == nullptr) _builder->first_step = step;
  StepCtx ctx; ctx._step = step; ctx._step_id = this->_step_id++; ctx._builder = this->_builder;

  // step->set_rst(&this->_step->get_rst());
  step->set_rst(new Result);
  _builder->result_list.push_back(&step->get_rst());
  step->get_rst().copy_schema(this->_step->get_rst());

  size_t col_to_put = step->get_rst().get_cols();
  step->get_rst().append_schema(Result::kNode, dst_col_alias[0]);
  size_t depth_col = step->get_rst().get_cols();
  step->get_rst().append_schema(Result::kUINT64, dst_col_alias[1]);

  this->_step->append_next(step);
  step->set_prev({this->_step});

  step->set_src_col({step->get_rst().get_col_idx_by_alias(src_col_alias)});
  step->set_col_to_put({col_to_put, depth_col});
  // ctx._wrote_cols = {col_to_put, depth_col};

  step->set_label(label);
  step->set_dir(dir);
  // step->_depth_col = depth_col;
  step->_terminate = [min_depth, max_depth](Node*, size_t depth) {
    // then reach max_depth, terminate
    return depth >= max_depth;
  };
  step->_place_to_rst = [min_depth, max_depth](Node*, size_t depth) {
    // then reach max_depth, terminate
    return depth <= max_depth && depth >= min_depth;
  };
  return ctx;
}


StepCtx StepCtx::get_all_neighbour(label_t label, dir_t dir, std::string src_col_alias, std::string dst_col_alias, label_t dst_node_label, bool optional) {
  StepCtx ctx = get_all_neighbour(label, dir, src_col_alias, dst_col_alias, optional);
  GetAllNeighbourStep* step = (GetAllNeighbourStep*)ctx._step;
  step->_filter_node_label = true;
  step->_node_label = dst_node_label;
  return ctx;
}
StepCtx StepCtx::get_all_neighbour(label_t label, dir_t dir, std::string src_col_alias, std::string dst_col_alias, bool optional) {
  // do not share the same result table
  // if (col_src == nullptr) col_src = this;
  GetAllNeighbourStep* step = new GetAllNeighbourStep;
  _builder->steps.push_back(step);
  if (_builder->first_step == nullptr) _builder->first_step = step;
  StepCtx ctx; ctx._step = step; ctx._step_id = this->_step_id++; ctx._builder = this->_builder;

  // step->set_rst(&this->_step->get_rst());
  step->set_rst(new Result);
  _builder->result_list.push_back(&step->get_rst());
  step->get_rst().copy_schema(this->_step->get_rst());

  size_t col_to_put = step->get_rst().get_cols();
  step->get_rst().append_schema(Result::kNode, dst_col_alias);

  this->_step->append_next(step);
  step->set_prev({this->_step});

  step->set_src_col({step->get_rst().get_col_idx_by_alias(src_col_alias)});
  step->set_col_to_put({col_to_put});
  // ctx._wrote_cols = {col_to_put};

  step->set_label(label);
  step->set_dir(dir);
  step->_optional = optional;
  return ctx;
}

StepCtx StepCtx::place_prop(label_t label, std::string prop_name, Result::ColumnType ty, std::string src_col_alias, std::string dst_col_alias) {
  size_t prop_idx = SchemaManager::get()->get_prop_idx(label, prop_name);
  return place_prop(prop_idx, ty, src_col_alias, dst_col_alias);
}

StepCtx StepCtx::place_prop(size_t prop_idx, Result::ColumnType ty, std::string src_col_alias, std::string dst_col_alias) {
  // share the same result table.
  // if (col_src == nullptr) col_src = this;
  PlacePropCol* step = new PlacePropCol;
  _builder->steps.push_back(step);
  if (_builder->first_step == nullptr) _builder->first_step = step;
  StepCtx ctx; ctx._step = step; ctx._step_id = this->_step_id++; ctx._builder = this->_builder;

  step->set_rst(&this->_step->get_rst());

  size_t col_to_put = step->get_rst().get_cols();
  step->get_rst().append_schema(ty, dst_col_alias);

  this->_step->append_next(step);
  step->set_prev({this->_step});

  step->set_src_col({step->get_rst().get_col_idx_by_alias(src_col_alias)});
  step->set_col_to_put({col_to_put});
  // ctx._wrote_cols = {col_to_put};

  step->_prop_idx = prop_idx;
  return ctx;
}

StepCtx StepCtx::place_prop_back(label_t label, std::string prop_name, Result::ColumnType ty, StepCtx & val_ctx, std::string src_col_alias, std::string dst_col_alias) {
  // share the same result table.
  PlacePropColBack* step = new PlacePropColBack;
  _builder->steps.push_back(step);
  if (_builder->first_step == nullptr) _builder->first_step = step;
  StepCtx ctx; ctx._step = step; ctx._step_id = this->_step_id++; ctx._builder = this->_builder;

  step->set_rst(&this->_step->get_rst());

  this->_step->append_next(step);
  step->set_prev({val_ctx._step});

  size_t src_col = val_ctx._step->get_rst().get_col_idx_by_alias(src_col_alias);
  size_t col_to_put = step->get_rst().get_col_idx_by_alias(dst_col_alias);
  step->set_src_col({src_col});
  step->set_col_to_put({col_to_put});

  step->_prop_idx = SchemaManager::get()->get_prop_idx(label, prop_name);
  return ctx;
}

StepCtx StepCtx::filter(StepCtx & oprand, CompareOp op, std::vector<std::string> src_col_alias) {
  // do not share the same result table.
  FilterStep* step = new FilterStep;
  _builder->steps.push_back(step);
  if (_builder->first_step == nullptr) _builder->first_step = step;
  StepCtx ctx; ctx._step = step; ctx._step_id = this->_step_id++; ctx._builder = this->_builder;

  // step->set_rst(&this->_step->get_rst());
  step->set_rst(new Result);
  _builder->result_list.push_back(&step->get_rst());
  step->get_rst().copy_schema(this->_step->get_rst());

  // size_t col_to_put = step->get_rst().get_cols();
  // step->get_rst().append_schema(ty);

  this->_step->append_next(step);
  step->set_prev({this->_step, oprand._step});

  step->set_src_col({step->get_rst().get_col_idx_by_alias(src_col_alias[0]), oprand._step->get_rst().get_col_idx_by_alias(src_col_alias[1])});
  // step->set_col_to_put({col_to_put});

  step->_filter_op = op;
  return ctx;
}

StepCtx StepCtx::filter_simple(std::function<bool(ResultItem val)> lambda, std::string src_col_alias) {
  // do not share the same result table.
  FilterSimpleStep* step = new FilterSimpleStep;
  _builder->steps.push_back(step);
  if (_builder->first_step == nullptr) _builder->first_step = step;
  StepCtx ctx; ctx._step = step; ctx._step_id = this->_step_id++; ctx._builder = this->_builder;

  // step->set_rst(&this->_step->get_rst());
  step->set_rst(new Result);
  _builder->result_list.push_back(&step->get_rst());
  step->get_rst().copy_schema(this->_step->get_rst());

  // size_t col_to_put = step->get_rst().get_cols();
  // step->get_rst().append_schema(ty);

  this->_step->append_next(step);
  step->set_prev({this->_step});

  step->set_src_col({step->get_rst().get_col_idx_by_alias(src_col_alias)});
  // step->set_col_to_put({col_to_put});

  step->if_match = lambda;
  return ctx;
}

StepCtx StepCtx::filter_label(label_t label, CompareOp op, std::string src_col_alias) {
  // do not share the same result table.
  if (op != kEq && op != kNe) throw FatalException("label compare must be ==/!=");
  FilterByLabelStep* step = new FilterByLabelStep;
  _builder->steps.push_back(step);
  if (_builder->first_step == nullptr) _builder->first_step = step;
  StepCtx ctx; ctx._step = step; ctx._step_id = this->_step_id++; ctx._builder = this->_builder;

  // step->set_rst(&this->_step->get_rst());
  step->set_rst(new Result);
  _builder->result_list.push_back(&step->get_rst());
  step->get_rst().copy_schema(this->_step->get_rst());

  // size_t col_to_put = step->get_rst().get_cols();
  // step->get_rst().append_schema(ty);

  this->_step->append_next(step);
  step->set_prev({this->_step});

  step->set_src_col({step->get_rst().get_col_idx_by_alias(src_col_alias)});
  // step->set_col_to_put({col_to_put});

  step->_filter_op = op;
  step->set_label(label);
  return ctx;
}


StepCtx StepCtx::get_all_edge(label_t label, dir_t dir, std::string src_col_alias, std::string dst_col_alias, bool optional) {
  // do not share the same result table
  GetAllEdgeStep* step = new GetAllEdgeStep;
  _builder->steps.push_back(step);
  if (_builder->first_step == nullptr) _builder->first_step = step;
  StepCtx ctx; ctx._step = step; ctx._step_id = this->_step_id++; ctx._builder = this->_builder;

  // step->set_rst(&this->_step->get_rst());
  step->set_rst(new Result);
  _builder->result_list.push_back(&step->get_rst());
  step->get_rst().copy_schema(this->_step->get_rst());

  size_t col_to_put = step->get_rst().get_cols();
  step->get_rst().append_schema(Result::kEdge, dst_col_alias);

  this->_step->append_next(step);
  step->set_prev({this->_step});

  step->set_src_col({step->get_rst().get_col_idx_by_alias(src_col_alias)});
  step->set_col_to_put({col_to_put});
  // ctx._wrote_cols = {col_to_put};

  step->set_label(label);
  step->set_dir(dir);
  step->_optional = optional;
  _builder->col_get_all_edge_to_col_src_node[dst_col_alias] = src_col_alias;
  return ctx;
}

StepCtx StepCtx::get_single_edge(label_t label, dir_t dir, std::vector<std::string> src_col_alias, std::string dst_col_alias) {
  return get_single_edge(label, dir, this, src_col_alias, dst_col_alias);
}
StepCtx StepCtx::get_single_edge(label_t label, dir_t dir, StepCtx* second_id, std::vector<std::string> src_col_alias, std::string dst_col_alias) {
  // share the same result table
  GetSingleEdgeStep* step = new GetSingleEdgeStep;
  _builder->steps.push_back(step);
  if (_builder->first_step == nullptr) _builder->first_step = step;
  StepCtx ctx; ctx._step = step; ctx._step_id = this->_step_id++; ctx._builder = this->_builder;

  step->set_rst(&this->_step->get_rst());

  size_t col_to_put = step->get_rst().get_cols();
  step->get_rst().append_schema(Result::kEdge, dst_col_alias);

  this->_step->append_next(step);
  step->set_prev({this->_step, second_id->_step});

  step->set_src_col({step->get_rst().get_col_idx_by_alias(src_col_alias[0]), step->get_rst().get_col_idx_by_alias(src_col_alias[1])});
  step->set_col_to_put({col_to_put});
  // ctx._wrote_cols = {col_to_put};

  step->set_label(label);
  step->set_dir(dir);
  return ctx;
}

StepCtx StepCtx::algeo(MathOp op, std::vector<std::string> src_col_alias, std::string dst_col_alias) {
  // share the same result table
  return algeo(op, *this, src_col_alias, dst_col_alias);
}
StepCtx StepCtx::algeo(MathOp op, StepCtx & oprand2, std::vector<std::string> src_col_alias, std::string dst_col_alias) {
  // share the same result table
  AlgeoStep* step = new AlgeoStep;
  _builder->steps.push_back(step);
  if (_builder->first_step == nullptr) _builder->first_step = step;
  StepCtx ctx; ctx._step = step; ctx._step_id = this->_step_id++; ctx._builder = this->_builder;

  step->set_rst(&this->_step->get_rst());
  size_t src_col1 = step->get_rst().get_col_idx_by_alias(src_col_alias[0]);
  size_t src_col2 = step->get_rst().get_col_idx_by_alias(src_col_alias[1]);

  size_t col_to_put = step->get_rst().get_cols();
  if (step->get_rst().get_type(src_col1) != 
      oprand2._step->get_rst().get_type(src_col2))
    throw FatalException("Mismatch algeo type");
  step->get_rst().append_schema(oprand2._step->get_rst().get_type(src_col2), dst_col_alias);

  this->_step->append_next(step);
  step->set_prev({this->_step, oprand2._step});

  step->set_src_col({src_col1, src_col2});
  step->set_col_to_put({col_to_put});
  // ctx._wrote_cols = {col_to_put};

  step->_op = op;
  return ctx;
}

StepCtx StepCtx::sort(std::vector<std::string> src_cols_alias, std::vector<bool> orders, uint64_t limit, uint64_t skip) {
  // share the same result table
  SortStep* step = new SortStep;
  _builder->steps.push_back(step);
  if (_builder->first_step == nullptr) _builder->first_step = step;
  StepCtx ctx; ctx._step = step; ctx._step_id = this->_step_id++; ctx._builder = this->_builder;

  step->set_rst(&this->_step->get_rst());

  // size_t col_to_put = step->get_rst().get_cols();
  // step->get_rst().append_schema(Result::kEdge);

  this->_step->append_next(step);
  step->set_prev({this->_step});

  std::vector<size_t> src_cols;
  for (std::string alias : src_cols_alias) {
    src_cols.push_back(step->get_rst().get_col_idx_by_alias(alias));
  }

  step->_key_cols = src_cols;
  step->_order = orders;
  step->_limit = limit;
  step->_skip = skip;

  return ctx;
}

Result::ColumnType group_rst_type(GroupByStep::Aggregator op, Result::ColumnType src_type) {
  switch (op) {
    case GroupByStep::kAvg: return Result::kDOUBLE;
    case GroupByStep::kCount: return Result::kUINT64;
    case GroupByStep::kMax: return src_type;
    case GroupByStep::kMin: return src_type;
    case GroupByStep::kSum: return src_type;
    case GroupByStep::kFirst: return src_type;
    // case GroupByStep::kMakeSet: return Result::kSet;
    case GroupByStep::kMakeSet: return Result::kSTRING;
    default: throw FatalException("Unreachable");
  }
}

StepCtx StepCtx::select_group(
    std::vector<std::string> key_cols_alias,
    std::vector<std::string> src_cols_alias,
    std::vector<GroupByStep::Aggregator> group_ops, 
    std::vector<std::vector<FilterCtx>> filters,
    std::vector<std::string> dst_cols_alias) {
  // do not share the same result table
  // if (col_src == nullptr) col_src = this;
  GroupByStep* step = new GroupByStep;
  _builder->steps.push_back(step);
  if (_builder->first_step == nullptr) _builder->first_step = step;
  StepCtx ctx; ctx._step = step; ctx._step_id = this->_step_id++; ctx._builder = this->_builder;

  step->set_rst(new Result);
  _builder->result_list.push_back(&step->get_rst());

  for (size_t i = 0; i < key_cols_alias.size(); i++) {
    size_t key_col = this->_step->get_rst().get_col_idx_by_alias(key_cols_alias.at(i));
    Result::ColumnType key_type = this->_step->get_rst().get_type(key_col);
    step->get_rst().append_schema(key_type, key_cols_alias.at(i));
    step->append_key_col(key_col);
  }

  size_t cur_col_to_put = step->get_rst().get_cols();
  std::vector<GroupByStep::GroupCtx> group_ctxs;

  for (size_t i = 0; i < src_cols_alias.size(); i++) {
    size_t src_col = this->_step->get_rst().get_col_idx_by_alias(src_cols_alias.at(i));
    step->get_rst().append_schema(group_rst_type( group_ops[i], this->_step->get_rst().get_type(src_col) ), dst_cols_alias.at(i));
    GroupByStep::GroupCtx group_ctx;
    group_ctx._op = group_ops[i];
    group_ctx._src_col = src_col;
    group_ctx._dst_col = cur_col_to_put;
    group_ctx._filters = filters[i];
    // ctx._wrote_cols.push_back(cur_col_to_put);
    group_ctxs.push_back(group_ctx);
    cur_col_to_put++;
  }

  step->_rst_cols = group_ctxs;

  this->_step->append_next(step);
  step->set_prev({this->_step});

  return ctx;
}

StepCtx StepCtx::insert_node(label_t label, std::string src_col_alias, std::string dst_col_alias) {
  CreateNodeStep* step = new CreateNodeStep;
  _builder->steps.push_back(step);
  if (_builder->first_step == nullptr) _builder->first_step = step;
  StepCtx ctx; ctx._step = step; ctx._step_id = this->_step_id++; ctx._builder = this->_builder;


  step->set_rst(&this->_step->get_rst());

  size_t col_to_put = step->get_rst().get_cols();
  step->get_rst().append_schema(Result::kNode, dst_col_alias);

  this->_step->append_next(step);
  step->set_prev({this->_step});

  step->set_src_col({step->get_rst().get_col_idx_by_alias(src_col_alias)});
  step->set_col_to_put({col_to_put});

  step->set_label(label);
  return ctx;
}


StepCtx StepCtx::insert_edge(label_t label, StepCtx & node2, std::vector<std::string> src_cols_alias, dir_t dir, std::string dst_col_alias) {
  CreateEdgeStep* step = new CreateEdgeStep;
  _builder->steps.push_back(step);
  if (_builder->first_step == nullptr) _builder->first_step = step;
  StepCtx ctx; ctx._step = step; ctx._step_id = this->_step_id++; ctx._builder = this->_builder;


  step->set_rst(&this->_step->get_rst());

  size_t col_to_put = step->get_rst().get_cols();
  step->get_rst().append_schema(Result::kEdge, dst_col_alias);

  this->_step->append_next(step);
  step->set_prev({this->_step, node2._step});

  step->set_src_col({this->_step->get_rst().get_col_idx_by_alias(src_cols_alias[0]),
                     node2._step->get_rst().get_col_idx_by_alias(src_cols_alias[1])});
  step->set_col_to_put({col_to_put});

  step->set_label(label);
  step->set_dir(dir);
  return ctx;
}
