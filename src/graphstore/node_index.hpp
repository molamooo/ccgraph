#pragma once
#include "edge.hpp"
#include "node.hpp"
#include "concurrent_hash_index.hpp"
#include "second_index.hpp"
#include "obj_allocator.hpp"
#include <unordered_map>

/**
 * @brief 
 * map from label to indices of nodes. The default index would be on id, and
 * user may add more indices on property
 * 
 */
class NodeIndex  {
 private:
  template<typename _K, typename _V>
  using umap = std::unordered_map<_K, _V>;
  umap<label_t, ConcurrentHashIndex<node_id_t, Node*>> _id_index;
  umap<label_t, umap<PropType, SecondaryIndex<Node*>>> _prop_index;
  Allocator* _allocator;
 public:
  Node* GetNode(const label_t label, const node_id_t id) {
    try {
      return _id_index.at(label).Read(id);
    } catch (IndexException e) {
      return nullptr;
    }
  }
  Node* TouchNode(const label_t label, const node_id_t id) {
    auto lambda = _allocator->get_allocator<Node>(label);
    return _id_index.at(label).ReadOrInsert(id, &lambda);
  }
  Node* DeleteNode(const label_t label, const node_id_t id) {
    try {
      return _id_index.at(label).Delete(id);
    } catch (IndexException e) {
      return nullptr;
    }
  }
};

