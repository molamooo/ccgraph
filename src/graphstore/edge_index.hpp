#pragma once
#include "edge.hpp"
#include "node.hpp"
#include "concurrent_hash_index.hpp"
#include "second_index.hpp"
#include "obj_allocator.hpp"
#include <unordered_map>
#include <unordered_set>

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
  CHIdx<internal_id_t, CHIdx<internal_id_t, Edge*>*> _id_index_in;
  CHIdx<internal_id_t, CHIdx<internal_id_t, Edge*>*> _id_index_out;
  // umap<PropType, CHIdx< internal_id_t, CHIdx<internal_id_t, Edge*>* >> _prop_index;

  Allocator* _edge_allocator;
  FixSizeAllocator _adj_list_allocator;
  const label_t _label;
 public:
  EdgeIndex() = delete;
  EdgeIndex(const label_t label, Allocator* allocator) : 
    _edge_allocator(allocator),
    _adj_list_allocator(sizeof(ConcurrentHashIndex<internal_id_t, Edge*>)),
    _label(label) {}
  Edge* GetEdge(const internal_id_t id1, const internal_id_t id2) {
    try {
      return _id_index_out.Read(id1)->Read(id2);
    } catch (IndexException & e) {
      return nullptr;
    }
  }
  /**
   * @brief the outside must take care of the node field in edge.
   */
  Edge* TouchEdge(const internal_id_t id1, const internal_id_t id2, bool & created) {
    // todo: prop index
    auto alloc_edge = _edge_allocator->get_allocator<Edge> (_label);
    std::function<ConcurrentHashIndex<internal_id_t, Edge*>*()> alloc_adj_list = 
      [this](){return new (_adj_list_allocator.Alloc()) ConcurrentHashIndex<internal_id_t, Edge*>();};
    // bool created;
    Edge* e = _id_index_out.ReadOrInsert(id1, &alloc_adj_list)->ReadOrInsert(id2, &alloc_edge, created);
    if (!created) return e;
    _id_index_in.ReadOrInsert(id2, &alloc_adj_list)->Insert(id1, e);
    e->_internal_id1 = id1;
    e->_internal_id2 = id2;
    return e;
  }
  Edge* DeleteEdge(const internal_id_t id1, const internal_id_t id2) {
    // todo: prop index
    try {
      Edge* e = _id_index_out.Read(id1)->Delete(id2);
      _id_index_in.Read(id2)->Delete(id1);
      return e;
    } catch (IndexException & e) {
      return nullptr;
    }
  }
  void GetAllEdge(const internal_id_t id, const dir_t dir, std::vector<Edge*> & rsts) {
    if (dir == dir_bidir) {
      std::unordered_set<internal_id_t> touched;
      {
        std::function<void(Edge*)> lambda = [&rsts, &touched](Edge* e)mutable{
          if (touched.find(e->_internal_id2) != touched.end()) return;
          rsts.push_back(e);
          touched.insert(e->_internal_id2);
        };
        try {
          ConcurrentHashIndex<internal_id_t, Edge *> * idx_ptr = _id_index_out.Read(id);
          idx_ptr->ProcessAll(lambda);
        } catch (IndexException & e) {}
      }
      {
        std::function<void(Edge*)> lambda = [&rsts, &touched](Edge* e)mutable{
          if (touched.find(e->_internal_id1) != touched.end()) return;
          rsts.push_back(e);
          touched.insert(e->_internal_id1);
        };
        try {
          ConcurrentHashIndex<internal_id_t, Edge *> * idx_ptr = _id_index_in.Read(id);
          idx_ptr->ProcessAll(lambda);
        } catch (IndexException & e) {}
      }
    } else if (dir == dir_out) {
      try {
        _id_index_out.Read(id)->ReadAll(rsts);
      } catch (IndexException & e) {}
    } else if (dir == dir_in) {
      try {
        _id_index_in.Read(id)->ReadAll(rsts);
      } catch (IndexException & e) {}
      // return nullptr;
    }
  }
  void GetAllNeighbour(const internal_id_t id, const dir_t dir, std::vector<Node*> & rsts) {
    if (dir == dir_bidir) {
      std::unordered_set<internal_id_t> touched;
      {
        std::function<void(Edge*)> lambda = [&rsts, &touched](Edge* e)mutable{
          if (touched.find(e->_internal_id2) != touched.end()) return;
          rsts.push_back(e->_node2);
          touched.insert(e->_internal_id2);
        };
        try {
          ConcurrentHashIndex<internal_id_t, Edge *> * idx_ptr = _id_index_out.Read(id);
          idx_ptr->ProcessAll(lambda);
        } catch (IndexException & e) {}
      }
      {
        std::function<void(Edge*)> lambda = [&rsts, &touched](Edge* e)mutable{
          if (touched.find(e->_internal_id1) != touched.end()) return;
          rsts.push_back(e->_node1);
          touched.insert(e->_internal_id1);
        };
        try {
          ConcurrentHashIndex<internal_id_t, Edge *> * idx_ptr = _id_index_in.Read(id);
          idx_ptr->ProcessAll(lambda);
        } catch (IndexException & e) {}
      }
    } else if (dir == dir_out) {
      std::function<void(Edge*)> lambda = [&rsts](Edge* e)mutable{
        rsts.push_back(e->_node2);
      };
      try {
        auto idx_ptr = _id_index_out.Read(id);
        idx_ptr->ProcessAll(lambda);
      } catch (IndexException & e) {}
    } else if (dir == dir_in) {
      std::function<void(Edge*)> lambda = [&rsts](Edge* e)mutable{
        rsts.push_back(e->_node1);
      };
      try {
        _id_index_in.Read(id)->ProcessAll(lambda);
      } catch (IndexException & e) {}
    }
  }
  void DeleteAllEdge(const internal_id_t id) {
    std::vector<Edge*> outs;
    _id_index_out.Read(id)->ReadAll(outs);
    for (Edge* e : outs) {
      DeleteEdge(e->_internal_id1, e->_internal_id2);
    }
    std::vector<Edge*> ins;
    _id_index_out.Read(id)->ReadAll(ins);
    for (Edge* e : ins) {
      DeleteEdge(e->_internal_id2, e->_internal_id1);
    }
  }
};

class LabelEdgeIndex  {
 private:
  template<typename _K, typename _V>
  using umap = std::unordered_map<_K, _V>;
  umap<label_t, EdgeIndex> _index;
 public:
  LabelEdgeIndex() = delete;
  LabelEdgeIndex(label_t min_edge_label, label_t max_edge_label, Allocator* al) {
    for (label_t l = min_edge_label; l <= max_edge_label; l++) {
      _index.emplace(std::piecewise_construct, 
                     std::forward_as_tuple(l),
                     std::forward_as_tuple(l, al));
    }
  }
  Edge* GetEdge(const label_t label, const internal_id_t id1, const internal_id_t id2) {
    try {
      return _index.at(label).GetEdge(id1, id2);
    } catch (std::out_of_range & e) {
      throw IndexException(Formatter() << "Edge label should be already in the index: " << label);
    }
  }
  Edge* TouchEdge(const label_t label, const internal_id_t id1, const internal_id_t id2, bool & created) {
    try {
      Edge* e = _index.at(label).TouchEdge(id1, id2, created);
      e->_label = label;
      // id is set in inner idx;
      // e->_internal_id1 = id1;
      // e->_internal_id2 = id2;
      return e;
    } catch (std::out_of_range & e) {
      throw IndexException(Formatter() << "Edge label should be already in the index: " << label);
    }
  }
  Edge* DeleteEdge(const label_t label, const internal_id_t id1, const internal_id_t id2) {
    try {
      return _index.at(label).DeleteEdge(id1, id2);
    } catch (std::out_of_range & e) {
      throw IndexException(Formatter() << "Edge label should be already in the index: " << label);
    }
  }
  void GetAllEdge(const label_t label, const internal_id_t id, const dir_t dir, std::vector<Edge*> & rsts) {
    try {
      _index.at(label).GetAllEdge(id, dir, rsts);
    } catch (std::out_of_range & e) {
      throw IndexException(Formatter() << "Edge label should be already in the index: " << label);
    }
  }
  void GetAllNeighbour(const label_t label, const internal_id_t id, const dir_t dir, std::vector<Node*> & rsts) {
    try {
      _index.at(label).GetAllNeighbour(id, dir, rsts);
    } catch (std::out_of_range & e) {
      throw IndexException(Formatter() << "Edge label should be already in the index: " << label);
    }
  }
  void DeleteAllEdge(const label_t label, const internal_id_t id) {
    try {
      _index.at(label).DeleteAllEdge(id);
    } catch (std::out_of_range & e) {
      throw IndexException(Formatter() << "Edge label should be already in the index: " << label);
    }
  }
};

