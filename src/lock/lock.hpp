#pragma once
#include "type.hpp"
#include <unordered_map>
#include <folly/futures/Future.h>
#include <shared_mutex>
#include <atomic>
#include <condition_variable>


static bool WWaiterBlockR = true;

struct AsyncRWLock {
  using VFuture=folly::Future<folly::Unit>;
  using VPromise=folly::Promise<folly::Unit>;
  std::mutex _m;
  std::condition_variable _cv;
  // std::atomic_flag _latch;
  bool wlocked;
  uint32_t reader=0, w_waiter=0;

  // std::shared_mutex _latch;
  void Wlock() {
    std::unique_lock<std::mutex> lk(_m);
    w_waiter++;
    while (wlocked || reader > 0) {
      _cv.wait(lk);
    }
    w_waiter--;
    wlocked = true;
    lk.unlock();
  }
  bool TryWlock() {
    std::unique_lock<std::mutex> lk(_m);
    if (wlocked || reader > 0) {
      lk.unlock(); return false;
    }
    wlocked = true;
    lk.unlock();
    return true;
  }
  VFuture WlockAsync() {
    auto p = VPromise();
    auto f = p.getFuture();
    std::thread([p=std::move(p), this]()mutable{
      Wlock();
      p.setValue();
    }).detach();
    return f;
  }
  void Wunlock() {
    std::unique_lock<std::mutex> lk(_m);
    wlocked = false;
    lk.unlock();
    _cv.notify_one();
  }
  void Rlock() {
    std::unique_lock<std::mutex> lk(_m);
    while (wlocked || (WWaiterBlockR && w_waiter > 0)) {
      _cv.wait(lk);
    }
    reader++;
    lk.unlock();
  }
  bool TryRlock() {
    std::unique_lock<std::mutex> lk(_m);
    if (wlocked || (WWaiterBlockR && w_waiter > 0)) {
      lk.unlock(); return false;
    }
    reader++;
    lk.unlock();
    return true;
  }
  VFuture RlockAsync() {
    auto p = VPromise();
    auto f = p.getFuture();
    std::thread([p=std::move(p), this]()mutable{
      Rlock();
      p.setValue();
    }).detach();
    return f;
  }
  void Runlock() {
    std::unique_lock<std::mutex> lk(_m);
    reader--;
    lk.unlock();
    _cv.notify_one();
  }
};

class LockManager {
 private:
  // std::vector<AsyncRWLock> _node_idx_locks;
  int _n_idx_lock_count = 0;
  int _node_p_lock_count;
  int _adj_idx_lock_count;
  int _edge_p_lock_count;
  // std::vector<AsyncRWLock> _node_p_locks;
  // std::vector<AsyncRWLock> _adj_idx_locks;
  // std::vector<AsyncRWLock> _edge_p_locks;
  std::vector<AsyncRWLock> _locks;

  const std::hash<uint64_t> _n_id_hasher = std::hash<uint64_t>();
  // uint64_t HashNodeIdx(const label_t n_l) {
  //   return n_l % _node_idx_locks.size();
  // }
  uint64_t HashNodeProp(const node_id_t n_id) {
    return _n_id_hasher(n_id) % _node_p_lock_count + _n_idx_lock_count;
  }
  uint64_t HashAdjIdx(const label_t e_l, const node_id_t n_id, const dir_t dir) {
    return (_n_id_hasher(n_id) * (e_l + 1)
           * (dir ? 18446744073709551557ull : 18446744073709551533ull) )
           % _adj_idx_lock_count + _n_idx_lock_count + _node_p_lock_count;
  }
  uint64_t HashEdgeProp(const label_t e_l, const node_id_t id1, const node_id_t id2) {
    return (_n_id_hasher(id1) * _n_id_hasher(id2) * (e_l + 1)) 
           % _edge_p_lock_count + _n_idx_lock_count + _node_p_lock_count + _adj_idx_lock_count ;
  }
 public:
  LockManager(int n_idx_locks, int n_p_locks, int adj_idx_locks, int e_p_locks) : 
      // _node_idx_locks(n_idx_locks), 
      // _node_p_locks(n_p_locks), 
      // _adj_idx_locks(adj_idx_locks), _edge_p_locks(e_p_locks),
      _locks(n_idx_locks+ n_p_locks+adj_idx_locks + e_p_locks) {
    // 
    assert(n_idx_locks == 0);
    _node_p_lock_count = n_p_locks;
    _adj_idx_lock_count = adj_idx_locks;
    _edge_p_lock_count = e_p_locks;
  }
  // AsyncRWLock & NIdx(const label_t n_l) { return _node_idx_locks.at(HashNodeIdx(n_l)); }
  AsyncRWLock & NProp(const node_id_t n_id) { return _locks.at(HashNodeProp(n_id)); }
  AsyncRWLock & AdjIdx(const label_t e_l, const node_id_t n_id, const dir_t dir) {
    return _locks.at(HashAdjIdx(e_l, n_id, dir));
  }
  AsyncRWLock & EProp(const label_t e_l, const node_id_t id1, const node_id_t id2) {
    return _locks.at(HashEdgeProp(e_l, id1, id2));
  }
};