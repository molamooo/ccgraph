#pragma once

#include "edge.hpp"
#include "node.hpp"
#include "cc_ctx.hpp"
#include "cc_manager.hpp"
#include "node_index.hpp"
#include <vector>
#include <memory>
#include <folly/futures/Future.h>

class CCGraph {
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

  CCManager2PL* _cc_manager;
 public:
  CCGraph() = delete;
  CCGraph(CCManager2PL* ccm) : _cc_manager(ccm) {}
  Future<Node*> GetNodeViaInternalId(
      const internal_id_t id,
      CCContex* ctx) {
    return _cc_manager->GetNodeViaInternalId(id, ctx);
  }
  Future<Node*> GetNodeViaLabeledId(
      const label_t label, const labeled_id_t id,
      CCContex* ctx) {
    return _cc_manager->GetNodeViaLabeledId(label, id, ctx);
  }
  /**
   * @brief Node may be accessed by index-free adjacency
   * Now we don't need the label to grant access to the node.
   * 
   * grant access, but do not go through the index to locate the node.
   */
  Future<Node*> AccessNode(
      Node* node, const bool share,
      CCContex* ctx) {
    return _cc_manager->AccessNode(node, share, ctx);
  }
  Future<Node*> InsertNode(
      label_t label, labeled_id_t id,
      CCContex* ctx) {
    return _cc_manager->InsertNode(label, id, ctx);
  }
  /**
   * @brief grant access, go through index to locate the node
   */
  Future<Node*> UpdateNodeViaLabeledId(
      const label_t label, const labeled_id_t id,
      CCContex* ctx) {
    return _cc_manager->UpdateNodeViaLabeledId(label, id, ctx);
  }
  Future<Node*> UpdateNodeViaInternalId(
      const internal_id_t id,
      CCContex* ctx) {
    return _cc_manager->UpdateNodeViaInternalId(id, ctx);
  }
  Future<Node*> DeleteNodeViaLabeledId(
      const label_t label, const labeled_id_t id,
      CCContex* ctx) {
    return _cc_manager->DeleteNodeViaLabeledId(label, id, ctx);
  }

  Future<Node*> DeleteNodeViaInternalId(
      const internal_id_t id,
      CCContex* ctx) {
    return _cc_manager->DeleteNodeViaInternalId(id, ctx);
  }


  Future<Edge*> GetEdgeViaInternalId(
      const label_t label, const internal_id_t id1, const internal_id_t id2,
      CCContex *ctx) {
    // share on edge
    return _cc_manager->GetEdgeViaInternalId(label, id1, id2, ctx);
  }
  Future<Edge*> GetEdgeViaLabeledId(
      const label_t label, const labeled_id_t id1, const labeled_id_t id2,
      CCContex *ctx) {
    // share on edge
    return _cc_manager->GetEdgeViaLabeledId(label, id1, id2, ctx);
  }
  Future<Edge*> CreateEdgeViaLabeledId(
      const label_t label, const labeled_id_t id1, const labeled_id_t id2, 
      CCContex *ctx) {
    // exclusive on edge
    // exclusive on adj index
    return _cc_manager->InsertEdgeViaLabeledId(label, id1, id2, ctx);
  }
  // Future<Edge*> CreateEdgeViaInternalId(
  //     const label_t label, const internal_id_t id1, const internal_id_t id2, 
  //     CCContex *ctx) {
  //   // exclusive on edge
  //   // exclusive on adj index
  //   return _cc_manager->Inser(label, id1, id2, ctx);
  // }
  Future<Edge*> UpdateEdge(
      const label_t label, const labeled_id_t id1, const labeled_id_t id2,
      CCContex *ctx) {
    // exclusive on edge
    return _cc_manager->UpdateEdge(label, id1, id2, ctx);
  }
  Future<std::tuple<Edge*, bool>> UpsertEdge(
      const label_t label, const labeled_id_t id1, const labeled_id_t id2,
      CCContex *ctx) {
    // exclusive on edge
    return _cc_manager->UpsertEdge(label, id1, id2, ctx);
  }
  Future<Edge*> DeleteEdge(
      const label_t label, const labeled_id_t id1, const labeled_id_t id2,
      CCContex *ctx) {
    // exclusive on edge
    // exclusive on adj index
    return _cc_manager->DeleteEdge(label, id1, id2, ctx);
  }
  Future<sptr<vec<Edge*>>> GetAllEdge(
      const label_t label, const internal_id_t id, const dir_t dir,
      CCContex *ctx) {
    // share on adj index
    // share on touched edge
    return _cc_manager->GetAllEdge(label, id, dir, ctx);
  }
  /**
   * @brief Only when there is secondary index
   */
  Future<sptr<vec<Edge*>>> GetEdgeViaProp(
      const label_t label, const internal_id_t id, const dir_t dir,
      const uint64_t prop_idx, void* low_bound, void* highbound, 
      CCContex *ctx) {
    // share on adj index
    throw Unimplemented("property index not implemented");
    // auto f2 = _cc_manager->GrantAccessAdjIdx(label, id1, dir, true, ctx);
    // share on touched edge
    // TODO
  }

  Future<sptr<vec<Node*>>> GetAllNeighbour(
      const label_t label, const internal_id_t id, const dir_t dir,
      CCContex *ctx) {
    // share on adj index
    // share on touch node
    return _cc_manager->GetAllNeighbour(label, id, dir, ctx);
  }

  void Commit(CCContex* ctx) {
    _cc_manager->Commit(ctx);
  }
  bool CommitCheck(CCContex* ctx) {
    return _cc_manager->CommitCheck(ctx);
  }
  void Abort(CCContex* ctx) {
    _cc_manager->Abort(ctx);
  }
};