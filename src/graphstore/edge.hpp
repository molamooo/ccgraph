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
  node_id_t _id1, _id2;
  Node *_node1, *_node2;
  label_t _label;
  uint8_t _prop[0];
};