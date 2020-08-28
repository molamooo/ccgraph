#pragma once

#include "type.hpp"
#include "lock.hpp"
#include "cc_ctx.hpp"
#include "schema.hpp"
#include "obj_allocator.hpp"
#include "exceptions.hpp"
#include "node_index.hpp"
#include "edge_index.hpp"
#include <folly/futures/Future.h>
#include <folly/executors/GlobalExecutor.h>

enum OpType {
  kRead=0,
  kWrite,
};
class CCManager {
 private:
  using VFuture=folly::Future<folly::Unit>;
  using VPromise=folly::Promise<folly::Unit>;
  template<typename T>
  using Future=folly::Future<T>;
  template<typename T>
  using Promise=folly::Promise<T>;
 protected:
  NodeIndex* _node_index = nullptr;
  LabelEdgeIndex* _edge_index = nullptr;

 public:
  CCManager() {}
  CCManager(NodeIndex* node_index, LabelEdgeIndex* edge_index) : 
    _node_index(node_index), _edge_index(edge_index) {}
  virtual ~CCManager() {}
  // virtual VFuture GrantAccessNProp(
  //   const internal_id_t n_id, const bool share, CCContex* ctx);
  // virtual VFuture GrantAccessAdjIdx(
  //   const label_t e_l, const internal_id_t n_id, 
  //   const dir_t dir, const bool share, CCContex* ctx);
  // virtual VFuture GrantAccessEProp(
  //   const label_t e_l, const internal_id_t id1, 
  //   const internal_id_t id2, const bool share, CCContex* ctx);
  // virtual VFuture ReleaseAccessNProp(
  //   const internal_id_t n_id, const bool share, CCContex* ctx);
  // virtual VFuture ReleaseAccessAdjIdx(
  //   const label_t e_l, const internal_id_t n_id, 
  //   const dir_t dir, const bool share, CCContex* ctx);
  // virtual VFuture ReleaseAccessEProp(
  //   const label_t e_l, const internal_id_t id1, 
  //   const internal_id_t id2, const bool share, CCContex* ctx);
  virtual bool CommitCheck(CCContex* ctx) = 0;
  virtual void Commit(CCContex* ctx) = 0;
  virtual void Abort(CCContex* ctx) = 0;
  // virtual Future<Node*> GetNode(
  //     const label_t label, const internal_id_t id,
  //     CCContex* ctx) = 0;
};


class CCManager2PL : public CCManager {
 private:
  
  using VFuture=folly::Future<folly::Unit>;
  using VPromise=folly::Promise<folly::Unit>;
  template<typename T>
  using Future=folly::Future<T>;
  template<typename T>
  using Promise=folly::Promise<T>;
  template<typename T>
  using vec=std::vector<T>;
  template<typename T>
  using sptr=std::shared_ptr<T>;

  LockManager* _locks;
  Allocator* _allocator;
  SchemaManager* _schema;

  folly::Executor::KeepAlive<folly::Executor> * _executor;

  VFuture GrantAccess(uint64_t hashed, const bool share, CCContex* ctx) {
    // LOG_VERBOSE("locking %llu with share=%d", hashed, share);
    auto start_time = ctx->_measure_ctx->cc_time.start();
    int state = ctx->Locked(hashed);
    if (state == 0) {
      // new lock to be granted
      ctx->_measure_ctx->used_locks.inc();

      auto f = _locks->GetLock(hashed).LockAsync(share, ctx);
      if (f.isReady()) {
        f.value();
        ctx->_lock_history[hashed] = share;
        ctx->_measure_ctx->cc_time.stop();
        return folly::makeFuture();
      } else {
        // todo: the measurement is cross different executor, maybe we need to do measurement in lock
        return std::move(f).via(folly::getGlobalCPUExecutor())
          .thenValue([this, hashed, share, ctx](folly::Unit){
            ctx->_lock_history[hashed] = share;
            { // measurement
              measure_ctx* m = ctx->_measure_ctx;
              m->cc_time.stop();
              m->lock_block_time.stop(m->cc_time.start_point);
              m->blocked_locks.inc();
            }
          });
      }
    }
    if (state == 2 || share) {
      measure_ctx* m = ctx->_measure_ctx;
      m->cc_time.stop();
      m->reused_locks.inc();
      return folly::makeFuture().via(folly::getGlobalCPUExecutor());
    }
    ctx->_measure_ctx->used_locks.inc();
    auto f = _locks->GetLock(hashed).UpgradeAsync(ctx);
    if (f.isReady()) {
      f.value();
      ctx->_lock_history[hashed] = share;
      ctx->_measure_ctx->cc_time.stop();
      return folly::makeFuture();
    } else {
      return std::move(f).via(folly::getGlobalCPUExecutor())
        .thenValue([this, hashed, share, ctx](folly::Unit){
          ctx->_lock_history[hashed] = share;
          {
            measure_ctx* m = ctx->_measure_ctx;
            m->cc_time.stop();
            m->lock_block_time.stop(m->cc_time.start_point);
            m->blocked_locks.inc();
          }
        });
    }
  }
  VFuture GrantAccessNProp(
      const labeled_id_t n_id, const bool share, CCContex* ctx) {
    uint64_t hashed = _locks->HashNodeProp(n_id);
    return GrantAccess(hashed, share, ctx);
  }
  VFuture GrantAccessAdjIdx(
    const label_t e_l, const labeled_id_t n_id, 
    const dir_t dir, const bool share, CCContex* ctx) {
    uint64_t hashed = _locks->HashAdjIdx(e_l, n_id, dir);
    return GrantAccess(hashed, share, ctx);
  }
  VFuture GrantAccessEProp(
      const label_t e_l, const labeled_id_t id1, 
      const labeled_id_t id2, const bool share, CCContex* ctx) {
    uint64_t hashed = _locks->HashEdgeProp(e_l, id1, id2);
    return GrantAccess(hashed, share, ctx);
  }
 public:
  CCManager2PL(
      LockManager* lm, Allocator* al, SchemaManager* sm,
      NodeIndex* ni, LabelEdgeIndex* ei) :
    CCManager(ni, ei),
    _locks(lm),
    _allocator(al),
    _schema(sm)
    { }
  Future<Node*> GetNodeViaInternalId(
      const internal_id_t inner_id,
      CCContex* ctx) {
    // share on node
    Node* node = _node_index->GetNodeViaInternalId(inner_id);
    return GrantAccessNProp(node->_external_id, true, ctx).thenValue([this, inner_id, ctx, node](folly::Unit)->Node*{
      // Node* node = _node_index->GetNodeViaInternalId(inner_id);
      if (ctx->_org_nodes.find(inner_id) != ctx->_org_nodes.end()) {
        return node;
      }
      return node;
    });
  }
  Future<Node*> GetNodeViaLabeledId(
      const label_t label, const labeled_id_t labeled_id,
      CCContex* ctx) {
    // share on node
    // LOG_VERBOSE("get node via labeled id: label=%d,id.label=%d, id.id=%llu", label, labeled_id.label, labeled_id.id);
    return GrantAccessNProp(labeled_id, true, ctx).thenValue([this, label, labeled_id, ctx](folly::Unit)->Node*{
      Node* node = _node_index->GetNodeViaLabeledId(label, labeled_id);
      if (node == nullptr) return nullptr;
      if (ctx->_org_nodes.find(node->_internal_id) != ctx->_org_nodes.end()) {
        return node;
      }
      return node;
    });
  }
  Future<Node*> AccessNode(
      Node* node, const bool share,
      CCContex* ctx) {
    // share/exclusive on node
    return GrantAccessNProp(node->_external_id, share, ctx)
      .thenValue([this, inner_id=node->_internal_id, ctx, node, share](folly::Unit)->Node*{
        if (ctx->_org_nodes.find(inner_id) != ctx->_org_nodes.end()) {
          return node;
        }
        if (share) return node;
        Node* backup = (Node*)_allocator->Alloc(node->_type);
        memcpy(backup, node, _schema->get_prop_size(node->_type) + sizeof(Node));
        ctx->_org_nodes[inner_id] = backup;
        return node;
      });
  }
  Future<Node*> InsertNode(
      label_t label, labeled_id_t external_id,
      CCContex* ctx) {
    // exclusive on node
    return GrantAccessNProp(external_id, false, ctx)
      .thenValue([this, ctx, external_id, label](folly::Unit)->Node*{
        bool created;
        Node* n = _node_index->TouchNode(label, external_id, created);
        if (!created) {
          throw AbortException("key exists");
        }
        if (ctx->_org_nodes.find(n->_internal_id) != ctx->_org_nodes.end()) {
          return n;
        }
        ctx->_org_nodes[n->_internal_id] = nullptr;
        return n;
      });
  }
  Future<Node*> UpdateNodeViaLabeledId(
      const label_t label, const labeled_id_t external_id,
      CCContex* ctx) {
    // exclusive on node
    return GrantAccessNProp(external_id, false, ctx)
      .thenValue([this, ctx, external_id, label](folly::Unit)->Node*{
        Node* n = _node_index->GetNodeViaLabeledId(label, external_id);
        if (n == nullptr) throw AbortException("key not exist");
        if (ctx->_org_nodes.find(n->_internal_id) != ctx->_org_nodes.end()) {
          return n;
        }
        Node* backup = (Node*)_allocator->Alloc(n->_type);
        memcpy(backup, n, _schema->get_prop_size(n->_type) + sizeof(Node));
        ctx->_org_nodes[n->_internal_id] = backup;
        return n;
      });
  }
  Future<Node*> UpdateNodeViaInternalId(
      const internal_id_t inner_id,
      CCContex* ctx) {
    // exclusive on node
    Node* node = _node_index->GetNodeViaInternalId(inner_id);
    if (node == nullptr) throw FatalException("internal key not exist");
    return GrantAccessNProp(node->_external_id, false, ctx)
      .thenValue([this, ctx, n=node](folly::Unit)->Node*{
        if (ctx->_org_nodes.find(n->_internal_id) != ctx->_org_nodes.end()) {
          return n;
        }
        Node* backup = (Node*)_allocator->Alloc(n->_type);
        memcpy(backup, n, _schema->get_prop_size(n->_type) + sizeof(Node));
        ctx->_org_nodes[n->_internal_id] = backup;
        return n;
      });
  }
  Future<Node*> DeleteNodeViaLabeledId(
      const label_t label, const labeled_id_t ex_id,
      CCContex* ctx) {
    // exclusive on node
    auto n_ptr = std::make_shared<Node*>(nullptr);
    auto f = GrantAccessNProp(ex_id, false, ctx);
    f = std::move(f).thenValue([this, n_ptr, label, ex_id](folly::Unit){
      *n_ptr = _node_index->GetNodeViaLabeledId(label, ex_id);
      if (*n_ptr == nullptr) throw AbortException("Node not exist");
    });
    for (label_t l = _schema->get_min_edge_label(); l <= _schema->get_max_label(); l++) {
      f = std::move(f).thenValue([this, l, ctx, ex_id](folly::Unit){
        return GrantAccessAdjIdx(l, ex_id, dir_out, false, ctx);
      }).thenValue([this, l, ctx, ex_id](folly::Unit){
        return GrantAccessAdjIdx(l, ex_id, dir_in, false, ctx);
      });
    }
    auto outs = std::make_shared<std::unordered_map<label_t, std::vector<Edge*>>>();
    auto ins = std::make_shared<std::unordered_map<label_t, std::vector<Edge*>>>();
    for (label_t l = _schema->get_min_edge_label(); l <= _schema->get_max_label(); l++) {
      f = std::move(f).thenValue([this, l, ctx, outs, ins, n_ptr](folly::Unit){
        (*outs)[l]={}; (*ins)[l] = {};
        _edge_index->GetAllEdge(l, (*n_ptr)->_internal_id, dir_out, outs->at(l));
        _edge_index->GetAllEdge(l, (*n_ptr)->_internal_id, dir_in, ins->at(l));
        auto f1 = folly::makeFuture();
        for (Edge* e : outs->at(l)) {
          f1 = std::move(f1).thenValue([e, ctx, this](folly::Unit){
            return GrantAccessEProp(e->_label, e->_external_id1, e->_external_id2, false, ctx);
          });
        }
        for (Edge* e : ins->at(l)) {
          f1 = std::move(f1).thenValue([e, ctx, this](folly::Unit){
            return GrantAccessEProp(e->_label, e->_external_id1, e->_external_id2, false, ctx);
          });
        }
        return f1;
      });
    }

    f = std::move(f).thenValue([this, label, ex_id, ctx, outs, ins](folly::Unit){
      Node* n = _node_index->DeleteNode(label, ex_id);
      if (n == nullptr) throw AbortException("Node not exist");
      if (ctx->_org_nodes.find(n->_internal_id) == ctx->_org_nodes.end()) {
        // fix: directly place the deleted node to backup
        // Node* backup = (Node*)_allocator->Alloc(n->_type);
        // memcpy(backup, n, _schema->get_prop_size(n->_type) + sizeof(Node));
        ctx->_org_nodes[n->_internal_id] = n;
      }
      for (label_t l = _schema->get_min_edge_label(); l <= _schema->get_max_label(); l++) {
        for (Edge* e : outs->at(l)) {
          if (ctx->_org_edges.find(std::make_tuple(l, e->_internal_id1, e->_internal_id2)) == ctx->_org_edges.end()) {
            // fix: directly place the deleted edge to backup
            // Edge* backup = (Edge*)_allocator->Alloc(e->_label);
            // memcpy(backup, e, _schema->get_prop_size(e->_label) + sizeof(Edge));
            ctx->_org_edges[std::make_tuple(l, e->_internal_id1, e->_internal_id2)] = e;
          }
        }
        for (Edge* e : ins->at(l)) {
          if (ctx->_org_edges.find(std::make_tuple(l, e->_internal_id1, e->_internal_id2)) == ctx->_org_edges.end()) {
            // fix: directly place the deleted edge to backup
            // Edge* backup = (Edge*)_allocator->Alloc(e->_label);
            // memcpy(backup, e, _schema->get_prop_size(e->_label) + sizeof(Edge));
            ctx->_org_edges[std::make_tuple(l, e->_internal_id1, e->_internal_id2)] = e;
          }
        }
        outs->at(l).clear(); outs->at(l).shrink_to_fit();
        ins->at(l).clear(); ins->at(l).shrink_to_fit();
        // do not recycle these edge, since we are directly storing them as backup
        _edge_index->DeleteAllEdge(l, n->_internal_id, false);
      }
    });
    
    return std::move(f).thenValue([](folly::Unit)->Node*{return nullptr;});
    // exclusive on adj index
    // exclusive on adj edge
  }

  Future<Node*> DeleteNodeViaInternalId(const internal_id_t in_id, CCContex* ctx) {
    Node* n = _node_index->GetNodeViaInternalId(in_id);
    if (n == nullptr) throw FatalException("Internal id does not exist");
    return DeleteNodeViaLabeledId(n->_type, n->_external_id, ctx);
  }


  Future<Edge*> GetEdgeViaInternalId(
      const label_t label, const internal_id_t in_id1, const internal_id_t in_id2,
      CCContex* ctx) {
    // share on edge
    Node* n1 = _node_index->GetNodeViaInternalId(in_id1), *n2 = _node_index->GetNodeViaInternalId(in_id2);
    if (n1 == nullptr || n2 == nullptr) throw FatalException("Internal id does not exist");
    return GrantAccessEProp(label, n1->_external_id, n2->_external_id, true, ctx).thenValue([this, label, in_id1, in_id2, ctx](folly::Unit)->Edge*{
      Edge* e = _edge_index->GetEdge(label, in_id1, in_id2);
      if (ctx->_org_edges.find(std::make_tuple(label, in_id1, in_id2)) != ctx->_org_edges.end()) {
        return e;
      }
      return e;
    });
  }
  Future<Edge*> GetEdgeViaLabeledId(
      const label_t label, const labeled_id_t ex_id1, const labeled_id_t ex_id2,
      CCContex* ctx) {
    // share on edge
    // auto in_id_ptr = std::make_shared<std::tuple<internal_id_t, internal_id_t>>();
    if (ex_id1.label == 0 || ex_id2.label == 0) throw FatalException("query using labeled_id, but the label is zero.");
    auto f = GrantAccessEProp(label, ex_id1, ex_id2, true, ctx).thenValue([this, label, ex_id1, ex_id2, ctx](folly::Unit)->Edge*{
      Node* n1 = _node_index->GetNodeViaLabeledId(ex_id1), 
          * n2 = _node_index->GetNodeViaLabeledId(ex_id2);
      if (n1 == nullptr || n2 == nullptr) return nullptr;
      internal_id_t in_id1 = n1->_internal_id, in_id2 = n2->_internal_id;
      Edge* e = _edge_index->GetEdge(label, in_id1, in_id2);
      if (ctx->_org_edges.find(std::make_tuple(label, in_id1, in_id2)) != ctx->_org_edges.end()) {
        return e;
      }
      return e;
    });
    return f;
  }
  // Future<Edge*> InsertEdgeViaInternalId(
  //     label_t label, internal_id_t in_id1, internal_id_t in_id2,
  //     CCContex* ctx) {
  //   // exclusive on edge
  //   Node* n1 = _node_index->GetNodeViaInternalId(in_id1), *n2 = _node_index->GetNodeViaInternalId(in_id2);
  //   if (n1 == nullptr || n2 == nullptr) throw FatalException("Internal id does not exist");
  //   return GrantAccessEProp(label, n1->_external_id, n2->_external_id, false, ctx)
  //   // exclusive on adj index
  //     .thenValue([this, ctx, n1, label](folly::Unit){
  //       return GrantAccessAdjIdx(label, n1->_external_id, dir_out, false, ctx);
  //     }).thenValue([this, ctx, n2, label](folly::Unit){
  //       return GrantAccessAdjIdx(label, n2->_external_id, dir_in, false, ctx);
  //     }).thenValue([this, ctx, in_id1, in_id2, label, n1, n2](folly::Unit)->Edge*{
  //       bool created;
  //       Edge* e = _edge_index->TouchEdge(label, in_id1, in_id2, created);
  //       if (!created) {
  //         throw AbortException("edge exists");
  //       }
  //       e->_label = label;e->_internal_id1 = in_id1; e->_internal_id2 = in_id2;
  //       e->_external_id1 = n1->_external_id; e->_external_id2 = n2->_external_id;
  //       // todo: more elegent
  //       e->_node1 = n1; e->_node2 = n2;
  //       if (ctx->_org_edges.find(std::make_tuple(label, in_id1, in_id2)) != ctx->_org_edges.end()) {
  //         return e;
  //       }
  //       ctx->_org_edges[std::make_tuple(label, in_id1, in_id2)] = nullptr;
  //       return e;
  //     });
  // }
  Future<Edge*> InsertEdgeViaLabeledId(
      label_t label, labeled_id_t ex_id1, labeled_id_t ex_id2,
      CCContex* ctx) {
    // exclusive on edge
    // the node must be locked, otherwise it may not exist, but the edge is still successfully inserted.
    auto f = folly::makeFuture();
    return std::move(f).thenValue([this, ctx, ex_id1](folly::Unit){
      return GrantAccessNProp(ex_id1, true, ctx);
    }).thenValue([this, ctx, ex_id2](folly::Unit){
      return GrantAccessNProp(ex_id2, true, ctx);
    }).thenValue([label, ctx, ex_id1, ex_id2, this](folly::Unit){
      return GrantAccessEProp(label, ex_id1, ex_id2, false, ctx);
    }).thenValue([this, ctx, ex_id1, label](folly::Unit){
    // exclusive on adj index
      return GrantAccessAdjIdx(label, ex_id1, dir_out, false, ctx);
    }).thenValue([this, ctx, ex_id2, label](folly::Unit){
      return GrantAccessAdjIdx(label, ex_id2, dir_in, false, ctx);
    }).thenValue([this, ctx, ex_id1, ex_id2, label](folly::Unit)->Edge*{
      Node* n1 = _node_index->GetNodeViaLabeledId(ex_id1), 
          * n2 = _node_index->GetNodeViaLabeledId(ex_id2);
      if (n1 == nullptr || n2 == nullptr) throw AbortException("Insert edge, but the node does not exists");
      bool created;
      internal_id_t in_id1 = n1->_internal_id, in_id2 = n2->_internal_id;
      Edge* e = _edge_index->TouchEdge(label, in_id1, in_id2, created);
      if (!created) {
        throw AbortException("edge exists");
      }
      e->_label = label;e->_internal_id1 = in_id1; e->_internal_id2 = in_id2;
      e->_external_id1 = n1->_external_id; e->_external_id2 = n2->_external_id;
      // todo: more elegent
      e->_node1 = n1; e->_node2 = n2;
      if (ctx->_org_edges.find(std::make_tuple(label, in_id1, in_id2)) != ctx->_org_edges.end()) {
        return e;
      }
      ctx->_org_edges[std::make_tuple(label, in_id1, in_id2)] = nullptr;
      return e;
    });
  }
  Future<std::tuple<Edge*, bool>> UpsertEdge(
      label_t label, labeled_id_t ex_id1, labeled_id_t ex_id2,
      CCContex* ctx) {
    // exclusive on edge
    // the node must be locked, otherwise it may not exist, but the edge is still successfully inserted.
    auto f = folly::makeFuture();
    return std::move(f).thenValue([this, ctx, ex_id1 ,ex_id2, label](folly::Unit){
      return GrantAccessEProp(label, ex_id1, ex_id2, false, ctx);
    }).thenValue([this, ctx, ex_id1, ex_id2, label](folly::Unit){
      Node* n1 = _node_index->GetNodeViaLabeledId(ex_id1), *n2 = _node_index->GetNodeViaLabeledId(ex_id2);
      if (n1 == nullptr || n2 == nullptr) throw AbortException("Insert edge, but the node does not exists");
      Edge* e = _edge_index->GetEdge(label, n1->_internal_id, n2->_internal_id);
      if (e != nullptr) {
        // the update case
        if (ctx->_org_edges.find(std::make_tuple(label, n1->_internal_id, n2->_internal_id)) != ctx->_org_edges.end()) {
          return folly::makeFuture(std::make_tuple(e, false));
        }
        Edge* backup = (Edge*)_allocator->Alloc(e->_label);
        memcpy(backup, e, _schema->get_prop_size(e->_label) + sizeof(Edge));
        ctx->_org_edges[std::make_tuple(label, n1->_internal_id, n2->_internal_id)] = backup;
        return folly::makeFuture(std::make_tuple(e, false));
      } else {
        // the insert case
        return folly::makeFuture().thenValue([this, ctx, ex_id1](folly::Unit){
          return GrantAccessNProp(ex_id1, true, ctx);
        }).thenValue([this, ctx, ex_id2](folly::Unit){
          return GrantAccessNProp(ex_id2, true, ctx);
        }).thenValue([this, ctx, ex_id1, label](folly::Unit){
        // exclusive on adj index
          return GrantAccessAdjIdx(label, ex_id1, dir_out, false, ctx);
        }).thenValue([this, ctx, ex_id2, label](folly::Unit){
          return GrantAccessAdjIdx(label, ex_id2, dir_in, false, ctx);
        }).thenValue([this, ctx, ex_id1, ex_id2, label](folly::Unit){
          Node* n1 = _node_index->GetNodeViaLabeledId(ex_id1), 
              * n2 = _node_index->GetNodeViaLabeledId(ex_id2);
          if (n1 == nullptr || n2 == nullptr) throw AbortException("Insert edge, but the node does not exists");
          bool created;
          internal_id_t in_id1 = n1->_internal_id, in_id2 = n2->_internal_id;
          Edge* e = _edge_index->TouchEdge(label, in_id1, in_id2, created);
          if (!created) {
            throw FatalException("impossible to reach");
          }
          e->_label = label;e->_internal_id1 = in_id1; e->_internal_id2 = in_id2;
          e->_external_id1 = n1->_external_id; e->_external_id2 = n2->_external_id;
          // todo: more elegent
          e->_node1 = n1; e->_node2 = n2;
          if (ctx->_org_edges.find(std::make_tuple(label, in_id1, in_id2)) != ctx->_org_edges.end()) {
            return std::make_tuple(e, true);
          }
          ctx->_org_edges[std::make_tuple(label, in_id1, in_id2)] = nullptr;
            return std::make_tuple(e, true);
        });
      }
    });

  }
  Future<Edge*> UpdateEdge(
      const label_t label, const labeled_id_t ex_id1, const labeled_id_t ex_id2,
      CCContex* ctx) {
    // exclusive on edge
    return GrantAccessEProp(label, ex_id1, ex_id2, false, ctx)
      .thenValue([this, ctx, ex_id1, ex_id2, label](folly::Unit)->Edge*{
        Node* n1 = _node_index->GetNodeViaLabeledId(ex_id1), *n2 = _node_index->GetNodeViaLabeledId(ex_id2);
        if (n1 == nullptr || n2 == nullptr) throw FatalException("insert edge, but node id does not exist");
        Edge* e = _edge_index->GetEdge(label, n1->_internal_id, n2->_internal_id);
        if (e == nullptr) throw AbortException("key not exist");
        if (ctx->_org_edges.find(std::make_tuple(label, n1->_internal_id, n2->_internal_id)) != ctx->_org_edges.end()) {
          return e;
        }
        Edge* backup = (Edge*)_allocator->Alloc(e->_label);
        memcpy(backup, e, _schema->get_prop_size(e->_label) + sizeof(Edge));
        ctx->_org_edges[std::make_tuple(label, n1->_internal_id, n2->_internal_id)] = backup;
        return e;
      });
  }
  Future<Edge*> DeleteEdge(
      const label_t label, const labeled_id_t ex_id1, const labeled_id_t ex_id2,
      CCContex* ctx) {
    // exclusive on edge
    return GrantAccessEProp(label, ex_id1, ex_id2, false, ctx)
    // exclusive on adj index
      .thenValue([this, ctx, ex_id1, label](folly::Unit){
        return GrantAccessAdjIdx(label, ex_id1, dir_out, false, ctx);
      }).thenValue([this, ctx, ex_id2, label](folly::Unit){
        return GrantAccessAdjIdx(label, ex_id2, dir_in, false, ctx);
      }).thenValue([this, ctx, ex_id1, ex_id2, label](folly::Unit)->Edge*{
        Node* n1 = _node_index->GetNodeViaLabeledId(ex_id1), *n2 = _node_index->GetNodeViaLabeledId(ex_id2);
        if (n1 == nullptr || n2 == nullptr) throw FatalException("Internal id does not exist");
        Edge* e = _edge_index->DeleteEdge(label, n1->_internal_id, n2->_internal_id);
        if (e == nullptr) throw AbortException("key not exist");
        if (ctx->_org_edges.find(std::make_tuple(label, n1->_internal_id, n2->_internal_id)) != ctx->_org_edges.end()) {
          return nullptr;
        }
        // fix: directly place the deleted edge to backup
        // Edge* backup = (Edge*)_allocator->Alloc(e->_label);
        // memcpy(backup, e, _schema->get_prop_size(e->_label) + sizeof(Edge));
        ctx->_org_edges[std::make_tuple(label, n1->_internal_id, n2->_internal_id)] = e;
        return nullptr;
      });
  }

  Future<sptr<vec<Edge*>>> GetAllEdge(
      const label_t label, const internal_id_t in_id, const dir_t dir,
      CCContex* ctx) {
    // share on edge
    Node* n = _node_index->GetNodeViaInternalId(in_id);
    if (n == nullptr) throw FatalException("Internal id does not exist");
    auto edge_list = std::make_shared<std::vector<Edge*>>();
    auto f = GrantAccessAdjIdx(label, n->_external_id, dir, true, ctx)
      .thenValue([this, label, in_id, dir, ctx, edge_list](folly::Unit){
        _edge_index->GetAllEdge(label, in_id, dir, *edge_list);

        auto f1 = folly::makeFuture();
        for (Edge* e : *edge_list) {
          f1 = std::move(f1).thenValue([this, e, ctx](folly::Unit){
            return GrantAccessEProp(e->_label, e->_external_id1, e->_external_id2, true, ctx);
          });
        }
        return f1;
      }).thenValue([edge_list](folly::Unit){
        return edge_list;
      });
    return f;
  }
  Future<sptr<vec<Node*>>> GetAllNeighbour(
      const label_t label, const internal_id_t in_id, const dir_t dir,
      CCContex* ctx) {
    // share on edge
    Node* n = _node_index->GetNodeViaInternalId(in_id);
    if (n == nullptr) throw FatalException("Internal id does not exist");
    // LOG_VERBOSE("get all neighbour of label=%d, ex_id=%d, in_id=%llu,", label, n->_external_id.id, in_id.id);
    auto node_list = std::make_shared<std::vector<Node*>>();
    return GrantAccessAdjIdx(label, n->_external_id, dir, true, ctx)
      .thenValue([this, label, in_id, dir, ctx, node_list](folly::Unit){
        _edge_index->GetAllNeighbour(label, in_id, dir, *node_list);

        auto f1 = folly::makeFuture();
        for (Node* n : *node_list) {
          f1 = std::move(f1).thenValue([this, n, ctx](folly::Unit){
            return GrantAccessNProp(n->_external_id, true, ctx);
          });
        }
        return f1;
      }).thenValue([node_list](folly::Unit){
        return node_list;
      });
  }
  bool CommitCheck(CCContex* ctx) {
    return true;
  }
  void Commit(CCContex* ctx) {
    for (auto iter : ctx->_lock_history) {
      _locks->GetLock(iter.first).Unlock(iter.second, ctx);
    }
    for (auto & iter : ctx->_org_nodes) {
      if (iter.second == nullptr) {
        // this is a node inserted, just ignore it
        continue;
      }
      _allocator->Free(iter.second->_type, iter.second);
    }
    for (auto & iter : ctx->_org_edges) {
      if (iter.second == nullptr) {
        // this is a node inserted, just ignore it
        continue;
      }
      _allocator->Free(iter.second->_label, iter.second);
    }
  }
  void Abort(CCContex* ctx) {
    for (auto & iter : ctx->_org_nodes) {
      if (iter.second == nullptr) {
        // this is a node inserted, just ignore it
        Node* n = _node_index->DeleteNodeViaInternalId(iter.first);
        _allocator->Free(n->_type, n);
      } else {
        Node* backup = iter.second;
        bool c;
        // fix: the backup should be placed back, since edge is pointing to it.
        Node* n = _node_index->RestoreNode(backup, c);
        if (c ==false) { // not using the backup, thus this is the update case
          memcpy(n, backup, _schema->get_prop_size(n->_type) + sizeof(Node));
          _allocator->Free(iter.second->_type, iter.second);
        }
      }
    }
    for (auto & iter : ctx->_org_edges) {
      if (iter.second == nullptr) {
        // this is a node inserted, just ignore it
        Edge* e = _edge_index->DeleteEdge(std::get<0>(iter.first), std::get<1>(iter.first), std::get<2>(iter.first));
        _allocator->Free(e->_label, e);
      } else {
        Edge* backup = iter.second;
        bool c;
        Edge* e = _edge_index->RestoreEdge(backup, c);
        if (!c) {
          memcpy(e, backup, _schema->get_prop_size(e->_label) + sizeof(Edge));
          _allocator->Free(backup->_label, backup);
        }
      }
    }
    for (auto iter : ctx->_lock_history) {
      _locks->GetLock(iter.first).Unlock(iter.second, ctx);
    }
  }
};