#pragma once
#include "type.hpp"

struct Node {
  node_id_t _id;
  label_t _type;
  uint8_t _prop[0];
};