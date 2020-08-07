#pragma once
#include "type.hpp"
#include "formatter.hpp"
#include "logger.hpp"
#include <sstream>
#include <string>
#include <exception>
#include <cstdarg>
#include <cstdio>
#include <execinfo.h>

#define MAX_ERROR_LEN 256

class param_exception: public std::exception {
 private:
  mutable std::string _msg;
  void* stack_pointers[10];
  size_t size;
 public:
  // param_exception(char const * fmt, ...) : std::exception() {
  //   char err_msg[MAX_ERROR_LEN];
  //   va_list args;
  //   va_start(args, fmt);
  //   vsnprintf(err_msg, MAX_ERROR_LEN, fmt, args);
  //   va_end(args);
  //   _msg = err_msg;
  // }
  param_exception(std::string msg) : std::exception(), _msg(msg), size(backtrace(stack_pointers, 10)) {
    // LOG_VERBOSE("Making an exception, msg is: %s", msg.c_str());
  }
  virtual char const * what() const throw() {
    char** stacks = backtrace_symbols(stack_pointers, size);
    _msg += "\nTrace back:\n";
    for (size_t i = 0; i < size; i++) {
      _msg += stacks[i];
      _msg += "\n";
    }
    return _msg.c_str();
  }
};


class Unimplemented: public param_exception {
 public:
  using param_exception::param_exception;
  // Unimplemented(char const * fmt, ...) : param_exception(fmt) {}
};

class AbortException: public param_exception {
 public:
  using param_exception::param_exception;
  // AbortException(char const * fmt, ...) : param_exception(fmt) {}
};

class CCFail: public AbortException {
 public:
  using AbortException::AbortException;
  // CCFail(char const * fmt, ...) : AbortException(fmt) {}
};

class FatalException: public param_exception {
 public:
  using param_exception::param_exception;
  // FatalException(char const * fmt, ...) : param_exception(fmt) {}
};