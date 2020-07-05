#pragma once
#include <cstdint>

using node_id_t = uint64_t;
using label_t = uint8_t;
using dir_t = bool;


enum PropType {
  kUINT64 = 0,
  kUINT32,
  kUINT8,
  kDOUBLE,
  kSTRING,
};