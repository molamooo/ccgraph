#pragma once
#include "config.hpp"
#include "exceptions.hpp"

#include "lock/lock.hpp"
#include "graphstore/obj_allocator.hpp"
#include "graphstore/schema.hpp"
#include "transaction/ccgraph.hpp"
#include "executor/worker.hpp"

#include "graphstore/graph_loader.hpp"

#include "bench_query/query_builder.hpp"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/GlobalExecutor.h>

class CCFrameworkCore {
 public:
  using VFuture = CCManager2PL::VFuture;
  using VPromise = CCManager2PL::VPromise;
  template<typename T>
  using Future = CCManager2PL::Future<T>;
  template<typename T>
  using Promise = CCManager2PL::Promise<T>;
  template<typename T>
  using vec=std::vector<T>;
  template<typename T>
  using sptr=std::shared_ptr<T>;
 private:

  bool _initialized = false;
  // Config _config;

  // for 2pl
  LockManager* _lock_manager;
  CCManager2PL* _cc_manager;

  Allocator* _allocator;
  SchemaManager* _schema;
  NodeIndex* _node_index;
  LabelEdgeIndex* _edge_index;

  CCGraph* _ccgraph;

  Worker* _worker;

  std::shared_ptr<folly::CPUThreadPoolExecutor> _executor_ptr;

  QueryBuilder* _query_builder;

  std::atomic<uint64_t> _ts_txnid;

  void checkInitialized() {
    if (!_initialized) throw FatalException("Please initialize ccf core first");
  }

  Future<sptr<Query>> runQueryOnce(std::string qname, std::vector<std::string> params, measure_ctx* _measure_ctx=nullptr) {
    auto q = std::make_shared<Query>();
    auto ts = _ts_txnid.fetch_add(1);
    q->set_ts(ts); q->set_txnid(ts);
    CCContex* ctx = new CCContex;
    q->set_cc_ctx(ctx);
    ctx->timestamp = ts;
    ctx->txn_id = ts;

    ctx->_measure_ctx = (_measure_ctx == nullptr)?new measure_ctx : _measure_ctx;

    _query_builder->build_query(qname, params, q.get());

    for (size_t i = 0; i < q->get_nstep(); i++) {
      q->get_step(i)->set_cc_ctx(ctx);
    }

    if (_measure_ctx == nullptr) {
      ctx->_measure_ctx->txn_time.start();
    }

#ifdef NO_THEN
    _worker->ProcessQuery(q).get();
    return q;
#else
    return _worker->ProcessQuery(q).thenValue([q](folly::Unit)->sptr<Query> {
      LOG_DEBUG("No fatal error? directly return the final rst");
      return q;
    });
#endif
  }
 public:
  void init() {
    if (_initialized) throw FatalException("Already initialized");
    // _config = config;
    Config & config = *Config::get();

    _schema = SchemaManager::get();;
    _schema->init(config.schema_file);
    // _schema->print();
    _allocator = new Allocator(*_schema);

    _node_index = new NodeIndex(_schema->get_min_label(), _schema->get_min_edge_label()-1, _allocator);
    _edge_index = new LabelEdgeIndex(_schema->get_min_edge_label(), _schema->get_max_label(), _allocator);

    _lock_manager = new LockManager(
      config.count_lock_node_index,
      config.count_lock_node_property,
      config.count_lock_adjacency_index,
      config.count_lock_edge_property
    );

    _cc_manager = new CCManager2PL(_lock_manager, _allocator, _schema, _node_index, _edge_index);

    _ccgraph = new CCGraph(_cc_manager);

    _worker = new Worker(_ccgraph, _schema);

    _executor_ptr = std::make_shared<folly::CPUThreadPoolExecutor>(config.thread_cnt);

    folly::setCPUExecutor(_executor_ptr);
    folly::setCPUExecutorToGlobalCPUExecutor();

    _query_builder = new QueryBuilder;

    _ts_txnid.store(1);

    _initialized = true;
  }
  ~CCFrameworkCore() {
    delete _query_builder;
    delete _worker;
    delete _ccgraph;
    delete _cc_manager;
    delete _lock_manager;
    delete _node_index;
    delete _edge_index;
    delete _allocator;
  }
  void loadGraph(const std::string & description_file) {
    GraphLoader loader(_node_index, _edge_index, _schema, [this](std::string qname, std::vector<std::string> params)->RetCode{
      return runQueryOnce(qname, params).wait().get()->_rc;
    });
    loader.prepare(description_file);
    loader.doLoad();
  }

  Future<sptr<Query>> runQuery(std::string qname, std::vector<std::string> params, bool retry = false, measure_ctx* _measure_ctx = nullptr) {
    auto f = runQueryOnce(qname, params, _measure_ctx);
    if (!retry) return f;
#ifdef NO_THEN
    sptr<Query> q_ptr = f.wait().value();
#else
    return std::move(f).thenValue([this, qname, params, retry](std::shared_ptr<Query> q_ptr)->Future<sptr<Query>>{
#endif
      if (q_ptr->_rc == kOk || q_ptr->_rc == kAbort || !retry) {
        q_ptr->get_cc_ctx()->_measure_ctx->txn_time.stop();
        delete q_ptr->get_cc_ctx();
        q_ptr->set_cc_ctx(nullptr);
        return q_ptr;
      }
      if (q_ptr->_rc != kConflict) throw FatalException("fatal error should not enter then value!");
      measure_ctx* m =  q_ptr->get_cc_ctx()->_measure_ctx;
      m->retries.inc();
      q_ptr->get_cc_ctx()->_measure_ctx = nullptr;
      delete q_ptr->get_cc_ctx();
      q_ptr->set_cc_ctx(nullptr);
      return runQuery(qname, params, true, m);
#ifdef NO_THEN
#else
    });
#endif
  }
};