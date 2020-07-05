#pragma once
#include "type.hpp"
#include "exceptions.hpp"
#include <cstdint>
#include <unordered_map>
#include <vector>
#include <string>
/**
 * @brief given a type, retrieve i-th property's type, size, offset
 * NO SUPPORT FOR DYNAMIC SCHEMA!
 * NO DEFAULT LABEL!
 * 
 */
uint64_t SizeOfPropType(const PropType t) {
  switch (t) {
    case kUINT64: return 8;
    case kUINT32: return 4;
    case kUINT8:  return 1;
    case kDOUBLE: return sizeof(double);
    case kSTRING: return sizeof(void*);
    default: throw SchemaException("Unknown property type: %d", t);
  }
}

struct PropDesc {
  PropType    _type;
  std::string _name;
  uint64_t    _offset;
};
struct LabelDesc {
  LabelDesc() {
    _prop_size = 0;
  }
  std::vector<PropDesc> _props;
  uint64_t _prop_size;
};

class SchemaManager {
  std::unordered_map<label_t, LabelDesc> _prop_lists;
  label_t _max_node_label; // starting from 1.
  label_t _max_edge_label; // starting from _max_node_label + 1
 private:
  void AddPropTo(const label_t label, const PropType p_type, const std::string & name) {
    if (_prop_lists.find(label) == _prop_lists.end()) {
      _prop_lists[label] = LabelDesc();
    }
    LabelDesc prop_list = _prop_lists.at(label);
    PropDesc prop;
    prop._type = p_type;
    prop._name = name;
    prop._offset = prop_list._prop_size;
    prop_list._props.push_back(prop);
    prop_list._prop_size += SizeOfPropType(p_type);
  }
  const LabelDesc & get_prop_desc_list(const label_t label) const {
    auto iter = _prop_lists.find(label);
    if (iter == _prop_lists.end()) throw SchemaException("Unknown label %d", label);
    return iter->second;
  }
 public:
  label_t get_label_by_name(const std::string & name) const {
    // todo
  }
  std::string get_label_name(const label_t label) const {
    // todo
  }
  size_t get_prop_size(const label_t label) const {
    return get_prop_desc_list(label)._prop_size;
  }
  inline size_t get_prop_count(const label_t label) const {
    return get_prop_desc_list(label)._props.size();
  }
  inline const PropDesc & get_prop_desc(const label_t label, const uint64_t idx) const {
    return get_prop_desc_list(label)._props[idx];
  }
  inline bool IsNode(const label_t label) const {
    if (label == 0 || label > _max_edge_label) throw SchemaException("Unknown label %d", label);
    return label <= _max_node_label;
  }
  inline label_t get_max_label() const {
    return _max_edge_label;
  }
  inline label_t get_min_label() const {
    return 1;
  }
};