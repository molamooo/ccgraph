#pragma once
#include "param_exception.hpp"
class QueryException: public param_exception {
 public:
  using param_exception::param_exception;
  QueryException(char const * fmt, ...) : param_exception(fmt) {}
};