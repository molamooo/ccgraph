#pragma once
#include "param_exception.hpp"
class IndexException: public param_exception {
 public:
  using param_exception::param_exception;
  // IndexException(char const * fmt, ...) : param_exception(fmt) {}
};