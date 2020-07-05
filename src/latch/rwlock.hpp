#pragma once
#include <shared_mutex>

struct RWLock {
  std::shared_mutex _latch;
  void Wlock() {
    _latch.lock();
  }
  void Wunlock() {
    _latch.unlock();
  }
  void Rlock() {
    _latch.lock_shared();
  }
  void Runlock() {
    _latch.unlock_shared();
  }
};