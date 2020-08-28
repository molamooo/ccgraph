#pragma once
#include "edge.hpp"
#include "node.hpp"
#include "concurrent_hash_index.hpp"
#include "second_index.hpp"
#include "obj_allocator.hpp"
#include <unordered_map>
#include <atomic>

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
  umap<label_t, ConcurrentHashIndex<labeled_id_t, Node*>> _labeled_id_index;
  ConcurrentHashIndex<internal_id_t, Node*> _no_label_id_index;
  // umap<label_t, umap<PropType, SecondaryIndex<Node*>>> _prop_index;
  Allocator* _allocator;
  std::atomic<uint64_t> _next_id_to_use;
 public:
  NodeIndex() = delete;
  NodeIndex(label_t min_labl, label_t max_label, Allocator* al) : _allocator(al), _next_id_to_use(1) {
    for (label_t l = min_labl; l <= max_label; l++) {
      _labeled_id_index.emplace(std::piecewise_construct, std::forward_as_tuple(l), std::forward_as_tuple());
    }
    // _allocator = al;
    // _next_id_to_use.store(1);
  }
  ~NodeIndex() {
    // just let the allocator to be freed.
    // std::function<void(Node*)> lambda = [this](Node* n){
    //   _allocator->Free(n->_type, n);
    // };
    // _no_label_id_index.ProcessAll(lambda);
  }
  Node* GetNodeViaLabeledId(const label_t label, const labeled_id_t id) {
    try {
      return _labeled_id_index.at(label).Read(id);
    } catch (IndexException & e) {
      return nullptr;
    }
  }
  Node* GetNodeViaLabeledId(const labeled_id_t id) {
    return GetNodeViaLabeledId(id.label, id);
  }
  Node* GetNodeViaInternalId(const internal_id_t id) {
    try {
      return _no_label_id_index.Read(id);
    } catch (IndexException & e) {
      return nullptr;
    }
  }
  Node* RestoreNode(Node* backup, bool & created) {
    std::function<Node*()> lambda = [backup](){return backup;};
    Node* n = _labeled_id_index.at(backup->_type).ReadOrInsert(backup->_external_id, &lambda, created);
    if (created == false) return n;
    assert(n == backup);
    _no_label_id_index.Insert(backup->_internal_id, n);
    return n;
  }
  Node* TouchNode(const label_t label, const labeled_id_t id, bool & created) {
    auto lambda = _allocator->get_allocator<Node>(label);
    Node* n = _labeled_id_index.at(label).ReadOrInsert(id, &lambda, created);
    if (created == false) return n;
    internal_id_t in_id(_next_id_to_use.fetch_add(1));
    n->_internal_id = in_id;
    n->_external_id = id;
    n->_type = label;
    _no_label_id_index.Insert(in_id, n);
    return n;
  }
  Node* TouchNode(const labeled_id_t id, bool & created) {
    return TouchNode(id.label, id, created);
  }
  Node* InsertNode(const label_t label, const labeled_id_t id) {
    // throw index exception if node exists
    Node* n = (Node*)_allocator->Alloc(label);
    try {
      _labeled_id_index.at(label).Insert(id, n);
    } catch (IndexException& e) {
      _allocator->Free(label, n);
      throw e;
    }
    internal_id_t in_id(_next_id_to_use.fetch_add(1));
    n->_internal_id = in_id;
    n->_external_id = id;
    n->_type = label;
    try {
      _no_label_id_index.Insert(in_id, n);
    } catch (IndexException & e) {
      throw FatalException("Unexpected reuse of internal id");
    }
    return n;
  }
  Node* InsertNode(const labeled_id_t id) {
    return InsertNode(id.label, id);
  }
  /** deletion, let outside handle the recycle of node pointer */
  Node* DeleteNodeViaInternalId(const internal_id_t id) {
    try {
      Node* n = _no_label_id_index.Delete(id);
      _labeled_id_index.at(n->_type).Delete(n->_external_id);
      return n;
    } catch (IndexException & e) {
      return nullptr;
    }
  }
  Node* DeleteNode(const label_t label, const labeled_id_t id) {
    try {
      Node* n = _labeled_id_index.at(label).Delete(id);
      _no_label_id_index.Delete(n->_internal_id);
      return n;
    } catch (IndexException & e) {
      return nullptr;
    }
  }
  Node* DeleteNode(const labeled_id_t id) {
    return DeleteNode(id.label, id);
  }
};

