#pragma once
#include <sstream>

class Formatter {
 private:
  std::stringstream _ss;
  Formatter(const Formatter &);
  Formatter & operator = (Formatter &);
 public:
  Formatter() : _ss() {}
  ~Formatter() {}

  template <typename T>
  Formatter & operator <<(const T & v) {
    _ss << v;
    return *this;
  }
  std::string str() const         { return _ss.str(); }
  operator std::string () const   { return _ss.str(); }
};