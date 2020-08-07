#pragma once
#include "type.hpp"
#include "exceptions.hpp"
#include <cstdint>
#include <unordered_map>
#include <vector>
#include <string>
#include <cstring>
#include <fstream>
#include <iostream>
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
    case kINT64:  return 8;
    default: throw SchemaException(Formatter() << "Unknown property type: " << t);
  }
}

struct PropDesc {
  PropType    _type;
  std::string _name;
  uint64_t    _offset;
  PropDesc(PropType ty, std::string name, uint64_t offset) :
    _type(ty), _name(name), _offset(offset) {}
};
struct LabelDesc {
  LabelDesc() { }
  LabelDesc(std::string label_name)
    : _label_name(label_name) { }
  std::vector<PropDesc> _props;
  uint64_t _prop_size = 0;
  std::string _label_name = "";

  std::string _base_label = "";
};

class SchemaManager {
 public:
  // static uint64_t _idx_node_id;
  // static uint64_t _idx_edge_id1;
  // static uint64_t _idx_edge_id2;
  std::unordered_map<std::string, std::vector<std::string>> _base_to_possible_labels;
 private:
  std::unordered_map<label_t, LabelDesc> _prop_lists;
  label_t _max_node_label=0; // starting from 1.
  label_t _max_edge_label=0; // starting from _max_node_label + 1
  void AddPropTo(const label_t label, const PropType p_type, const std::string & name) {
    if (_prop_lists.find(label) == _prop_lists.end()) {
      throw FatalException("Must create the label first");
      // _prop_lists[label] = LabelDesc();
    }
    LabelDesc & prop_list = _prop_lists.at(label);
    PropDesc prop(p_type, name, prop_list._prop_size);
    prop_list._props.push_back(prop);
    prop_list._prop_size += SizeOfPropType(p_type);
  }
  const LabelDesc & get_prop_desc_list(const label_t label) const {
    auto iter = _prop_lists.find(label);
    if (iter == _prop_lists.end()) throw SchemaException(Formatter() << "Unknown label " << (int)label);
    return iter->second;
  }
  label_t make_new_node(std::string label_name) {
    if (_max_node_label != _max_edge_label) throw FatalException("Must make all nodes, then make edge");
    _max_node_label++;
    _max_edge_label++;

    label_t this_label = _max_node_label;
    LabelDesc ld(label_name);
    _prop_lists[this_label] = ld;
    return this_label;
  }
  label_t make_new_edge(std::string label_name) {
    _max_edge_label++;

    label_t this_label = _max_edge_label;
    LabelDesc ld(label_name);
    _prop_lists[this_label] = ld;
    return this_label;
  }
 public:
  /**
    * @brief when laoding data, only the (base)label and the external id is known.
    * use this to know which solid label to try out
    */
  std::vector<std::string> possible_label_to_try(std::string given_label) {
    if (_base_to_possible_labels.find(given_label) == _base_to_possible_labels.end()) return {given_label};
    return _base_to_possible_labels[given_label];
  }
  void init(const std::string & fname) {
    bool is_node = true;
    std::cout << "initializing schema using " << fname << std::endl;
    std::ifstream f(fname);
    char buf[200];
    while(f.getline(buf, 200)) {
      if (buf[0] == '\0') continue;
      if (buf[0] == '#') continue;
      if (std::strlen(buf) < 5) throw FatalException("Invalie schema file");
      if (std::strncmp(buf, "node", 4) == 0) {
        if (is_node == false) throw FatalException("Node must be placed together");
        // make_new_node();
      } else if (std::strncmp(buf, "edge", 4) == 0) {
        is_node = false;
        // make_new_edge();
      } else {
        throw FatalException("Must indicate edge or node");
      }

      char* next_bar = std::strchr(buf+5, '|');
      if (next_bar) *next_bar = '\0';
      char* possible_base = std::strchr(buf+5, ':');
      if (possible_base) {
        *possible_base = '\0';
        possible_base++;
      }
      std::string label_name(buf+5), base_label="";
      if (possible_base) base_label = possible_base;

      label_t this_label = is_node ? make_new_node(label_name) : make_new_edge(label_name);
      // std::cout << "find label " << label_name << ", base is \"" << base_label << "\"" << std::endl;
      _prop_lists[this_label]._base_label = base_label;
      if (_base_to_possible_labels.find(base_label) == _base_to_possible_labels.end()) {
        _base_to_possible_labels[base_label] = {};
      }
      _base_to_possible_labels[base_label].push_back(label_name);

      while (next_bar != nullptr) {
        // next_bar is the head of this property
        char* prop_name_head = next_bar + 1;
        next_bar = std::strchr(prop_name_head, '|');
        if (next_bar != nullptr) *next_bar = '\0';

        char* end_of_prop_name = std::strchr(prop_name_head, ':');
        if (end_of_prop_name == nullptr) throw FatalException("prop must be <name>:<type>");

        *end_of_prop_name = '\0';
        std::string prop_name(prop_name_head), prop_type(end_of_prop_name + 1);

        if (prop_type == "uint64")
          AddPropTo(this_label, PropType::kUINT64, prop_name);
        else if (prop_type == "uint32")
          AddPropTo(this_label, PropType::kUINT32, prop_name);
        else if (prop_type == "uint8")
          AddPropTo(this_label, PropType::kUINT8, prop_name);
        else if (prop_type == "int64")
          AddPropTo(this_label, PropType::kINT64, prop_name);
        else if (prop_type == "double")
          AddPropTo(this_label, PropType::kDOUBLE, prop_name);
        else if (prop_type == "string")
          AddPropTo(this_label, PropType::kSTRING, prop_name);
        else
          throw FatalException(Formatter() << "Unsupported prop type " << prop_type);
      }
    }
  }
  label_t get_label_by_name(const std::string & name) const {
    for (auto & pd : _prop_lists) {
      if (pd.second._label_name == name) {
        return pd.first;
      }
    }
    throw FatalException(Formatter() << "Not allowed to find an no-existing label:" << name);
  }
  std::string get_label_name(const label_t label) const {
    return _prop_lists.at(label)._label_name;
  }
  size_t get_prop_size(const label_t label) const {
    return get_prop_desc_list(label)._prop_size;
  }
  inline size_t get_prop_count(const label_t label) const {
    return get_prop_desc_list(label)._props.size();
  }
  size_t get_prop_idx(const label_t label, const std::string prop_name) const {
    auto ld = get_prop_desc_list(label);
    for (size_t i = 0; i < ld._props.size(); i++) {
      if (ld._props[i]._name == prop_name) {
        return i;
      }
    }
    throw FatalException(Formatter() << "label " << get_label_name(label) << " has no such property " << prop_name);
  }
  size_t get_prop_idx(const std::string label_name, const std::string prop_name) const {
    label_t label = get_label_by_name(label_name);
    auto ld = get_prop_desc_list(label);
    for (size_t i = 0; i < ld._props.size(); i++) {
      if (ld._props[i]._name == prop_name) {
        return i;
      }
    }
    throw FatalException(Formatter() << "label " << label_name << " has no such property " << prop_name);
  }
  inline const PropDesc & get_prop_desc(const label_t label, const uint64_t idx) const {
    return get_prop_desc_list(label)._props.at(idx);
  }
  inline bool IsNode(const label_t label) const {
    if (label == 0 || label > _max_edge_label) throw SchemaException(Formatter() << "Unknown label " << (int)label);
    return label <= _max_node_label;
  }
  inline label_t get_min_label() const {
    return 1;
  }
  inline label_t get_min_edge_label() const {
    return _max_node_label+1;
  }
  inline label_t get_max_label() const {
    return _max_edge_label;
  }
  void get_prop(uint8_t* props_head, const label_t label, const uint64_t idx, uint8_t* dst) {
    const PropDesc & pd = get_prop_desc(label, idx);
    props_head += pd._offset;
    memcpy(dst, props_head, SizeOfPropType(pd._type));
  }
  void set_prop(uint8_t* props_head, const label_t label, const uint64_t idx, uint8_t* src) {
    const PropDesc & pd = get_prop_desc(label, idx);
    props_head += pd._offset;
    memcpy(props_head, src, SizeOfPropType(pd._type));
  }
  void print() {
    for(auto iter = _prop_lists.begin(); iter != _prop_lists.end(); iter++) {
      auto ld = iter->second;
      std::cout << "label: " << (int)iter->first << " is " << iter->second._label_name << std::endl;
      for (auto pd : ld._props) {
        std::cout << "     prop: " << pd._name << std::endl;
      }
    }
  }
  static SchemaManager* get() {
    static SchemaManager sm;
    return &sm;
  }
};
