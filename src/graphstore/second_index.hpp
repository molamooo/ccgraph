#pragma once
#include "concurrent_range_index.hpp"
#include <boost/any.hpp>

template<typename ValT>
class SecondaryIndex {
 private:
  boost::any _index;
  PropType   _key_type;
 public:
  SecondaryIndex(const PropType key_type) : _key_type(key_type) {
    switch (key_type) {
      case kUINT8 : _index = ConcurrentRangeIndex<uint8_t, ValT>(); break;
      case kUINT32: _index = ConcurrentRangeIndex<uint32_t, ValT>(); break;
      case kUINT64: _index = ConcurrentRangeIndex<uint64_t, ValT>(); break;
      case kDOUBLE: _index = ConcurrentRangeIndex<double, ValT>(); break;
      case kSTRING: _index = ConcurrentRangeIndex<std::string, ValT>(); break;
      default: throw SchemaException(Formatter() << "Unknown property type to be used on node index: " << key_type);
    }
  }
  template<typename KeyT>
  inline void Insert(const KeyT & key, const ValT & val) {
    boost::any_cast<KeyT>(_index).Insert(key, val);
  }
  template<typename KeyT>
  inline typename ConcurrentRangeIndex<KeyT, ValT>::Iterator Read(const KeyT & key) {
    return boost::any_cast<KeyT>(_index).Read(key);
  }
  template<typename KeyT>
  inline void Delete(const KeyT & key, const ValT & val) {
    boost::any_cast<KeyT>(_index).Delete(key, val);
  }
};