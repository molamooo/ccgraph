#pragma once
#include <atomic>

struct SpinLock {
  std::atomic_flag _latch;
  void Lock() {
    while(_latch.test_and_set()) ;
  }
  void Unlock() {
    _latch.clear();
  }
};