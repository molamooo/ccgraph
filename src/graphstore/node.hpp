#pragma once
#include "type.hpp"

struct Node {
  internal_id_t _internal_id;
  label_t _type;
  labeled_id_t _external_id; // only unique in each type!
  uint8_t _prop[0];
};