#pragma once
#include <cstdint>
#include <functional>
#include <ostream>

using label_t = uint8_t;
struct internal_id_t {
  uint64_t id = 0;
  internal_id_t(const uint64_t & id) : id(id) { }
  internal_id_t() {}
  uint64_t operator*(const uint64_t & r) {
    return id * r;
  }
};

std::ostream & operator<<(std::ostream & os, internal_id_t const & id) {
  return os << id.id;
}

struct labeled_id_t {
  uint64_t id = 0;
  label_t label = 0;
  labeled_id_t(const uint64_t & id, const label_t & label) : id(id), label(label) {}
  labeled_id_t(const uint64_t & id) : id(id) {}
  labeled_id_t() {}
  uint64_t operator*(const uint64_t & r) {
    return id * r;
  }
  labeled_id_t & operator=(const labeled_id_t & r) {
    id = r.id;
    label = r.label;
    return *this;
  }
};

std::ostream & operator<<(std::ostream & os, labeled_id_t const & id) {
  return os << id.label << ":" << id.id;
}

bool operator==(const internal_id_t & l, const internal_id_t & r) {
  return l.id == r.id;
}
bool operator<(const internal_id_t & l, const internal_id_t & r) {
  return l.id < r.id;
}
bool operator>(const internal_id_t & l, const internal_id_t & r) {
  return l.id > r.id;
}
bool operator<=(const internal_id_t & l, const internal_id_t & r) {
  return l.id <= r.id;
}
bool operator>=(const internal_id_t & l, const internal_id_t & r) {
  return l.id >= r.id;
}
bool operator!=(const internal_id_t & l, const internal_id_t & r) {
  return l.id != r.id;
}
bool operator==(const labeled_id_t & l, const labeled_id_t & r) {
  return l.id == r.id && l.label == r.label;
}
bool operator==(const labeled_id_t & l, const uint64_t & r) {
  return l.id == r;
}
bool operator<(const labeled_id_t & l, const labeled_id_t & r) {
  if (l.label != r.label) return l.label < r.label;
  return l.id < r.id;
}
bool operator>(const labeled_id_t & l, const labeled_id_t & r) {
  if (l.label != r.label) return l.label > r.label;
  return l.id > r.id;
}
bool operator<=(const labeled_id_t & l, const labeled_id_t & r) {
  if (l.label != r.label) return l.label < r.label;
  return l.id <= r.id;
}
bool operator>=(const labeled_id_t & l, const labeled_id_t & r) {
  if (l.label != r.label) return l.label > r.label;
  return l.id >= r.id;
}
bool operator!=(const labeled_id_t & l, const labeled_id_t & r) {
  return l.id != r.id || l.label != r.label;
}

namespace std{
template<>
struct hash<internal_id_t> {
  std::size_t operator()(internal_id_t const & id) const noexcept {
    return hash<uint64_t>()(id.id);
  }
};
template<>
struct hash<labeled_id_t> {
  std::size_t operator()(labeled_id_t const & id) const noexcept {
    return hash<uint64_t>()(id.id);
  }
};
};

// using internal_id_t = uint64_t;
// false for in, true for out
// using dir_t = bool;

enum dir_t {
  dir_in = 0,
  dir_out,
  dir_bidir,
};
// const dir_t dir_in = false;
// const dir_t dir_out = true;


enum PropType {
  // same as in result.hpp
  kUINT64 = 0,
  kUINT32,
  kUINT8,
  kDOUBLE,
  kSTRING,
  kINT64,
};