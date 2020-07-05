#pragma once
#include "edge.hpp"
#include "node.hpp"
#include "concurrent_hash_index.hpp"
#include "second_index.hpp"
#include "obj_allocator.hpp"
#include <unordered_map>

/**
 * @brief 
 * map from label to indices of edges. The default index would be on id, and
 * user may add more indices on property
 * 
 */

class EdgeIndex {
 private:
  template<typename _K, typename _V>
  using umap = std::unordered_map<_K, _V>;
  template<typename _K, typename _V>
  using CHIdx = ConcurrentHashIndex<_K, _V>;
  CHIdx<node_id_t, CHIdx<node_id_t, Edge*>*> _id_index;
  // umap<PropType, CHIdx< node_id_t, CHIdx<node_id_t, Edge*>* >> _prop_index;

  Allocator* _edge_allocator;
  FixSizeAllocator _adj_list_allocator;
  const label_t _label;
 public:
  Edge* GetEdge(const node_id_t id1, const node_id_t id2) {
    try {
      return _id_index.Read(id1)->Read(id2);
    } catch (IndexException e) {
      return nullptr;
    }
  }
  Edge* TouchEdge(const node_id_t id1, const node_id_t id2) {
    // todo: prop index
    auto alloc_edge = _edge_allocator->get_allocator<Edge> (_label);
    std::function<ConcurrentHashIndex<node_id_t, Edge*>*()> alloc_adj_list = 
      [this](){return new (_adj_list_allocator.Alloc()) ConcurrentHashIndex<node_id_t, Edge*>();};
    return _id_index.ReadOrInsert(id1, &alloc_adj_list)->ReadOrInsert(id2, &alloc_edge);
  }
  Edge* DeleteEdge(const node_id_t id1, const node_id_t id2) {
    // todo: prop index
    try {
      return _id_index.Read(id1)->Delete(id2);
    } catch (IndexException e) {
      return nullptr;
    }
  }
};

class LabelEdgeIndex  {
 private:
  template<typename _K, typename _V>
  using umap = std::unordered_map<_K, _V>;
  umap<label_t, EdgeIndex> _index;
 public:
  Edge* GetNode(const label_t label, const node_id_t id1, const node_id_t id2) {
    try {
      return _index.at(label).GetEdge(id1, id2);
    } catch (std::out_of_range e) {
      throw IndexException("Edge label should be already in the index: %d", label);
    }
  }
  Edge* TouchNode(const label_t label, const node_id_t id1, const node_id_t id2) {
    try {
      return _index.at(label).TouchEdge(id1, id2);
    } catch (std::out_of_range e) {
      throw IndexException("Edge label should be already in the index: %d", label);
    }
  }
  Edge* DeleteNode(const label_t label, const node_id_t id1, const node_id_t id2) {
    try {
      return _index.at(label).DeleteEdge(id1, id2);
    } catch (std::out_of_range e) {
      throw IndexException("Edge label should be already in the index: %d", label);
    }
  }
};

