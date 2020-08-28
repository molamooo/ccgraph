#pragma once

#include "type.hpp"

struct Node;

/**
 * @brief edge_t must be allocated with size larger than sizeof(edge_t). the
 * properties will be stored right next to this object, and accessed through
 * _prop.
 * 
 */
struct Edge {
  label_t _label;
  internal_id_t _internal_id1, _internal_id2;
  labeled_id_t _external_id1, _external_id2;
  Node *_node1 = nullptr, *_node2 = nullptr;
  // Fixme: maybe need to support locating node with both label and id
  // label_t _label_node1, _label_node2;
  uint8_t _prop[0];
};