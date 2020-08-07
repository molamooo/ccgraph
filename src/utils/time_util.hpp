#pragma once
#include "exceptions.hpp"
#include <cstdint>
#include <string>
#include <sstream>
#include <ctime>

struct date_time;
struct date {
  uint32_t year;
  uint8_t month, day;
  void init_ldbc(uint64_t date_compact) {
    day = date_compact % 100;
    date_compact /= 100;
    month = date_compact % 100;
    date_compact /= 100;
    year = date_compact;
  }
  uint64_t to_ldbc_date() {
    return year * 10000 + month * 100 + day;
  }
  std::string to_string() {
    char buf[4+2+2 + 1];
    sprintf(buf, "%4d%2d%2d", year, month, day);
    return buf;
  }
  date_time to_date_time();
};

struct date_time {
  std::tm _tm;
  uint32_t _millisec;
  void init_std(std::time_t tt, uint32_t ms) {
    gmtime_r(&tt, &_tm);
    _millisec = ms;
  }
  void init_ldbc(uint64_t time_compact) {
    _millisec = time_compact % 1000;
    time_compact /= 1000;
    _tm.tm_sec = time_compact % 100;
    time_compact /= 100;
    _tm.tm_min = time_compact % 100;
    time_compact /= 100;
    _tm.tm_hour = time_compact % 100;
    time_compact /= 100;
    _tm.tm_mday = time_compact % 100;
    time_compact /= 100;
    _tm.tm_mon = time_compact % 100;
    time_compact /= 100;
    _tm.tm_year = time_compact;
    _tm.tm_isdst = -1;
    // the wday and yday is ignored when conversion
  }

  static uint64_t minus_ldbc(uint64_t t1, uint64_t t2) {
    date_time dt1, dt2; dt1.init_ldbc(t1); dt2.init_ldbc(t2);
    return minus(dt1, dt2);
  }
  // calculate the gap, unit is millisecond
  static uint64_t minus(date_time t1, date_time t2) {
    uint64_t tt1 = timegm(&t1._tm), tt2 = timegm(&t2._tm);
    tt1 = tt1 * 1000 + t1._millisec;
    tt2 = tt2 * 1000 + t2._millisec;
    
    if (tt1 < tt2) throw FatalException("Unimplemented small time minus larger time");
    return tt1 - tt2;
  }
  std::string to_string() {
    char buf[4+2+2 + 2+2+2 +3 + 1];
    sprintf(buf, "%4d%2d%2d%2d%2d%2d%3d", _tm.tm_year, _tm.tm_mon, _tm.tm_mday, _tm.tm_hour, _tm.tm_min, _tm.tm_sec, _millisec);
    return buf;
  }
};

date_time date::to_date_time() {
  uint64_t ldbc_date_time = to_ldbc_date() * 100 * 100 * 100 * 1000;
  date_time rst;
  rst.init_ldbc(ldbc_date_time);
  return rst;
}