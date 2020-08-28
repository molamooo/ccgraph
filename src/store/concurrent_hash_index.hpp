#pragma once
#include "rwlock.hpp"
#include "index_exception.hpp"
#include <unordered_map>
#include <vector>

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
  void Insert(const KeyT & key, std::function<ValT()> *_allocator) {
    _latch.Wlock();
    auto iter = _index.find(key);
    if (iter != _index.end()) {
      _latch.Wunlock();
      throw IndexException(Formatter() << "Key already exists : " << key);
    }
    _index[key] = (*_allocator)();
    _latch.Wunlock();
  }
  ValT ReadOrInsert(const KeyT & key, std::function<ValT()> *_allocator, bool & created) {
    _latch.Rlock();
    auto iter = _index.find(key);
    if (iter != _index.end()) {
      ValT ret = iter->second;
      _latch.Runlock();
      created = false;
      return ret;
    }
    _latch.Runlock();

    _latch.Wlock();
    iter = _index.find(key);
    if (iter != _index.end()) {
      ValT ret = iter->second;
      _latch.Wunlock();
      created = false;
      return ret;
    }
    ValT val = (*_allocator)();
    _index[key] = val;
    _latch.Wunlock();
    created = true;
    return val;
  }
  ValT ReadOrInsert(const KeyT & key, std::function<ValT()> *_allocator) {
    bool _;
    return ReadOrInsert(key, _allocator, _);
  }
  ValT Read(const KeyT & key) {
    _latch.Rlock();
    auto iter = _index.find(key);
    if (iter == _index.end()) {
      _latch.Runlock();
      throw IndexException(Formatter() << "No such key : " << key);
    } 
    ValT val = iter->second;
    _latch.Runlock();
    return val;
  }
  void ReadAll(std::vector<ValT> & rsts) {
    _latch.Rlock();
    for (auto iter = _index.begin(); iter != _index.end(); iter++) {
      rsts.push_back(iter->second);
    }
    _latch.Runlock();
  }
  void ProcessAll(std::function<void(ValT)> & aggregator) {
    _latch.Rlock();
    for (auto iter = _index.begin(); iter != _index.end(); iter++) {
      aggregator(iter->second);
    }
    _latch.Runlock();
  }
  ValT Delete(const KeyT & key) {
    _latch.Wlock();
    auto iter = _index.find(key);
    if (iter == _index.end()) {
      _latch.Wunlock();
      throw IndexException(Formatter() << "No such key : " << key);
    }
    ValT val = iter->second;
    _index.erase(iter);
    _latch.Wunlock();
    return val;
  }
};