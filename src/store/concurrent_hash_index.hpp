#pragma once
#include <unordered_map>
#include "rwlock.hpp"
#include "index_exception.hpp"

template<typename KeyT, typename ValT>
class ConcurrentHashIndex {
  std::unordered_map<KeyT, ValT> _index;
  // todo: this latch may need to be controled by cc protocol
  RWLock _latch;
 public:
  void Insert(const KeyT & key, const ValT & val) {
    _latch.Wlock();
    auto iter = _index.find(key);
    if (iter != _index.end()) {
      _latch.Wunlock();
      throw IndexException(Formatter() << "Key already exists : " << key);
    }
    _index[key] = val;
    _latch.Wunlock();
  }
  ValT ReadOrInsert(const KeyT & key, std::function<ValT()> *_allocator) {
    _latch.Rlock();
    auto iter = _index.find(key);
    if (iter != _index.end()) {
      ValT ret = iter->second;
      _latch.Runlock();
      return ret;
    }
    _latch.Runlock();

    _latch.Wlock();
    iter = _index.find(key);
    if (iter != _index.end()) {
      ValT ret = iter->second;
      _latch.Wunlock();
      return ret;
    }
    ValT val = (*_allocator)();
    _index[key] = val;
    _latch.Wunlock();
    return val;
  }
  ValT Read(const KeyT & key) {
    _latch.Rlock();
    auto iter = _index.find(key);
    if (iter == _index.end()) {
      throw IndexException(Formatter() << "No such key : " << key);
    } 
    ValT val = iter->second;
    _latch.Runlock();
    return val;
  }
  ValT Delete(const KeyT & key) {
    _latch.Wlock();
    auto iter = _index.find(key);
    if (iter == _index.end()) {
      throw IndexException(Formatter() << "No such key : " << key);
    }
    ValT val = iter->second;
    _index.erase(iter);
    _latch.Wunlock();
    return val;
  }
};