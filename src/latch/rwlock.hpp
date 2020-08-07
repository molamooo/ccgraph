#pragma once
#include <shared_mutex>

struct RWLock {
  std::shared_timed_mutex _time_latch;
  // std::shared_mutex _latch;
  void Wlock() {
    _time_latch.lock();
    // _latch.lock();
  }
  void Wunlock() {
    _time_latch.unlock();
    // _latch.unlock();
  }
  void Rlock() {
    _time_latch.lock_shared();
    // _latch.lock_shared();
  }
  void Runlock() {
    _time_latch.unlock_shared();
    // _latch.unlock_shared();
  }
};