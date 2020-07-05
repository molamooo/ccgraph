#pragma once
#include <unordered_map>
#include <map>
#include "rwlock.hpp"
#include "exceptions.hpp"
#include <boost/any.hpp>

template<typename KeyT, typename ValT>
class ConcurrentRangeIndex {
  std::multimap<KeyT, ValT> _index;
  // std::unordered_map<KeyT, ValT> _index;
  RWLock _latch;
 public:
  class Iterator {
   private:
    ConcurrentRangeIndex<KeyT, ValT> & _index;
    typename std::multimap<KeyT, ValT>::iterator _start, _stop, _current;
    bool closed;
   public:
    Iterator(typename std::multimap<KeyT, ValT>::iterator start, typename std::multimap<KeyT, ValT>::iterator stop) {
      _start = start;
      _stop = stop;
      _current = _start;
      closed = false;
    }
    ValT Next() {
      if (closed) throw IndexException("Iterator closed");
      if (_current == _stop) throw IndexException("End of iterator");
      ValT val = _current->second;
      _current++;
      return val;
    }
    bool HasNext() {
      return (_current != _stop) && (!closed);
    }
    void Close() {
      if (!closed) _index._latch.Runlock();
      closed = true;
    }
    ~Iterator() {
      Close();
    }
  };
  ConcurrentRangeIndex<KeyT, ValT>() {}
  void Insert(const KeyT & key, const ValT & val) {
    _latch.Wlock();
    _index.insert({key, val});
    _latch.Wunlock();
  }
  Iterator Read(const KeyT & key) {
    _latch.Rlock();
    auto pair = _index.equal_range(key);
    Iterator ret = Iterator(pair.first, pair.second);
    if (ret.HasNext() == false) ret.Close();
    return ret;
  }
  void Delete(const KeyT & key, const ValT & val) {
    _latch.Wlock();

    for (auto iter = _index.find(key); iter != _index.end(); iter++) {
      if (iter->second != val) continue;
      _index.erase(iter);
      _latch.Wunlock();
      return;
    }
    _latch.Wunlock();
    throw IndexException(Formatter() << "No such key : " << key);
  }
};