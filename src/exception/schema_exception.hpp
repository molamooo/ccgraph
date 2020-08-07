#pragma once
#include "param_exception.hpp"
class SchemaException: public param_exception {
 public:
  using param_exception::param_exception;
  // SchemaException(char const * fmt, ...) : param_exception(fmt) {}
};