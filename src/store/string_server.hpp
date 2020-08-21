#pragma once
#include "rwlock.hpp"
#include "consts.hpp"
#include "exceptions.hpp"
#include <string>
#include <unordered_map>
#include <vector>

class StringServer {
 private:
  RWLock _latch;
  uint64_t _next_id;
  std::unordered_map<std::size_t, std::vector<uint64_t>> _buckets_of_offset;
  std::vector<std::string> _stored_string;
 public:
  StringServer() : _latch(), _next_id(0), _buckets_of_offset(), _stored_string() {
    touch("");
  }
  bool compare(const uint64_t id1, const uint64_t id2, CompareOp op) {
    if (id1 == 0 || id2 == 0) {
      switch (op) {
        case kEq: return (id1 == id2); break;
        case kNe: return (id1 != id2); break;
        case kG: 
        case kGe:
        case kL: 
        case kLE:
        default: throw FatalException("unreachable");
      }
    }
    bool rst;
    _latch.Rlock();
    const std::string & s1 = _stored_string.at(id1),
                      & s2 = _stored_string.at(id2);
    switch (op) {
      case kEq: rst = (s1== s2); break;
      case kNe: rst = (s1!= s2); break;
      case kG:  rst = (s1> s2); break;
      case kGe: rst = (s1>= s2); break;
      case kL:  rst = (s1< s2); break;
      case kLE: rst = (s1<= s2); break;
      default: throw FatalException("unreachable");
    }
    _latch.Runlock();
    return rst;
  }
  std::string get(uint64_t id) {
    _latch.Rlock();
    auto str = _stored_string.at(id);
    _latch.Runlock();
    return str;
  }
  uint64_t touch(std::string str) {

    std::size_t hashed = std::hash<std::string>()(str);
    _latch.Wlock();
    if (_buckets_of_offset.find(hashed) == _buckets_of_offset.end()) {
      _buckets_of_offset[hashed] = {_next_id};
      _stored_string.push_back(str);
      uint64_t id_to_ret = _next_id++;
      _latch.Wunlock();
      return id_to_ret;
    }

    auto & bucket = _buckets_of_offset[hashed];
    for (uint64_t offset : bucket) {
      if (_stored_string.at(offset) == str) {
        _latch.Wunlock();
        return offset;
      }
    }

    bucket.push_back(_next_id);
    _stored_string.push_back(str);
    uint64_t id_to_ret = _next_id++;
    _latch.Wunlock();
    return id_to_ret;
  }
  static StringServer* get() {
    static StringServer _singleton;
    return &_singleton;
  }
};