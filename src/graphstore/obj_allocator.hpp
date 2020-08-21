#pragma once

#include "schema.hpp"
#include "node.hpp"
#include "edge.hpp"
#include <tuple>
#include <boost/pool/pool.hpp>
#include <unordered_map>
#include <atomic>

// todo: unify this two allcator

class FixSizeAllocator {
 private:
  boost::pool<> _pool;
  std::atomic_flag _f = ATOMIC_FLAG_INIT;
 public:
  FixSizeAllocator(const uint64_t size) : _pool(size) {}
  void * Alloc() {
    while (_f.test_and_set());
    void * p = _pool.malloc();
    _f.clear();
    return p;
  };
  void Free(void * ptr) {
    while (_f.test_and_set());
    _pool.free(ptr);
    _f.clear();
  }
};

class Allocator {
 private:
  std::unordered_map<uint64_t, FixSizeAllocator> _pools;
  std::unordered_map<label_t, FixSizeAllocator*> _pools_by_label;
  const SchemaManager & _sm;
 public:
  Allocator(const SchemaManager & sm) : _pools(), _pools_by_label(), _sm(sm) {
    for (label_t l = sm.get_min_label(); l <= sm.get_max_label(); l++) {
      uint64_t size = sm.get_prop_size(l) + (sm.IsNode(l) ? sizeof(Node) : sizeof(Edge));
      if (_pools.find(size) != _pools.end()) continue;
      _pools.emplace(std::piecewise_construct, std::make_tuple(size), std::make_tuple(size));
    }
    for (label_t l = sm.get_min_label(); l <= sm.get_max_label(); l++) {
      uint64_t size = sm.get_prop_size(l) + (sm.IsNode(l) ? sizeof(Node) : sizeof(Edge));
      _pools_by_label[l] = &_pools.at(size);
    }
  }
  void* Alloc(const label_t l) {
    return _pools_by_label.at(l)->Alloc();
  }
  void Free(const label_t l, void* ptr) {
    _pools_by_label.at(l)->Free(ptr);
  }
  template<typename ValT>
  std::function<ValT*()> get_allocator(const label_t l) {
    FixSizeAllocator* pool = _pools_by_label.at(l);
    return [pool](){return (ValT*)(pool->Alloc());};
  }
};