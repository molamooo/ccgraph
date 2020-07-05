#pragma once

#include "edge.hpp"
#include "node.hpp"
#include "cc_ctx.hpp"
#include "cc_manager.hpp"
#include <vector>
#include <memory>
#include <folly/futures/Future.h>

class CCGraph {
 private:
  template<typename T>
  using vec=std::vector<T>;
  template<typename T>
  using sptr=std::shared_ptr<T>;
  template<typename T>
  using Future=folly::Future<T>;
  template<typename T>
  using Promise=folly::Promise<T>;

  CCManager* _cc_manager;
 public:
  Future<Node*> GetNode(
      const label_t label, const node_id_t id,
      CCContex* ctx) {
    // TODO
    // share on node
  }
  Future<Node*> InsertNode(
      const label_t label, const node_id_t id,
      const Node* node,
      CCContex* ctx) {
    // TODO
    // exclusive on node
  }
  Future<Node*> UpdateNode(
      const label_t label, const node_id_t id,
      const Node* node,
      CCContex* ctx) {
    // TODO
    // exclusive on node
  }
  Future<Node*> DeleteNode(
      const label_t label, const node_id_t id,
      CCContex* ctx) {
    // TODO
    // exclusive on node
  }
  Future<Edge*> GetEdge(
      const label_t label, const node_id_t id1, const node_id_t id2,
      CCContex *ctx) {
    // TODO
    // share on edge
  }
  Future<Edge*> CreateEdge(
      const label_t label, const node_id_t id1, const node_id_t id2,
      const Edge* edge,
      CCContex *ctx) {
    // TODO
    // exclusive on edge
    // exclusive on adj index
  }
  Future<Edge*> UpdateEdge(
      const label_t label, const node_id_t id1, const node_id_t id2,
      const Edge* edge,
      CCContex *ctx) {
    // TODO
    // exclusive on edge
  }
  Future<Edge*> DeleteEdge(
      const label_t label, const node_id_t id1, const node_id_t id2,
      CCContex *ctx) {
    // TODO
    // exclusive on edge
    // exclusive on adj index
  }
  Future<sptr<vec<Edge*>>> GetAllEdge(
      const label_t label, const node_id_t id1,
      CCContex *ctx) {
    // TODO
    // share on adj index
    // share on touched edge
  }
  /**
   * @brief Only when there is secondary index
   */
  Future<sptr<vec<Edge*>>> GetEdgeViaProp(
      const label_t label, const node_id_t id1,
      const uint64_t prop_idx, void* low_bound, void* highbound, 
      CCContex *ctx) {
    // TODO
    // share on adj index
    // share on touched edge
  }

  Future<sptr<vec<Node*>>> GetAllNeighbour(
      const label_t label, const node_id_t id1,
      CCContex *ctx) {
    // TODO
    // share on adj index
    // share on touch node
    // NO LOCK on edge
  }
};