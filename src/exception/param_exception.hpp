#pragma once
#include "type.hpp"
#include <sstream>
#include <string>
#include <exception>
#include <cstdarg>
#include <cstdio>

#define MAX_ERROR_LEN 256

#pragma once
#include "type.hpp"
#include "formatter.hpp"
#include <sstream>
#include <string>
#include <exception>
#include <cstdarg>
#include <cstdio>

#define MAX_ERROR_LEN 256

class param_exception: public std::exception {
 private:
  std::string _msg;
 public:
  param_exception(char const * fmt, ...) : std::exception() {
    char err_msg[MAX_ERROR_LEN];
    va_list args;
    va_start(args, fmt);
    vsnprintf(err_msg, MAX_ERROR_LEN, fmt, args);
    va_end(args);
    _msg = err_msg;
  }
  param_exception(std::string msg) : std::exception(), _msg(msg) {}
  virtual char const * what() const throw() {return _msg.c_str();}
};