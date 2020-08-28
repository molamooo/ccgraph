#pragma once
#include <cstdint>
#include <chrono>

struct measure_item {
  uint64_t collect = 0;

  std::chrono::system_clock::time_point start_point;

  std::chrono::system_clock::time_point start() {
    return start_point = std::chrono::system_clock::now();
  }
  void stop() {
    stop(start_point);
  }
  void stop(const std::chrono::system_clock::time_point & s_p) {
    collect += std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - s_p).count();
  }
  void inc() {collect++;}
};

struct measure_ctx {
  /** for measurement */
  measure_item lock_block_time;
  measure_item cc_time;

  measure_item txn_time;

  measure_item used_locks;
  measure_item reused_locks;
  measure_item blocked_locks;

  // todo: should we clear lock count across retry?
  measure_item retries;
};
