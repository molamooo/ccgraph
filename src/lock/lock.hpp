#pragma once
#include "type.hpp"
#include "cc_ctx.hpp"
#include "exceptions.hpp"
#include <unordered_map>
#include <folly/futures/Future.h>
#include <shared_mutex>
#include <atomic>
#include <condition_variable>
#include <iostream>

// #define QUEUED_LOCK
#ifdef QUEUED_LOCK
#define RWLOCK_T AsyncQueueRWLock
#else
#define RWLOCK_T AsyncRWLock
#endif


std::atomic<uint64_t> locker_count(0);
static bool WWaiterBlockR = true;
enum DeadLockPolicy {
  kNoWait=0,
  kWaitDie,
};
static DeadLockPolicy _dead_lock = kWaitDie;

class AsyncRWLock {
 public:
 private:
  bool locked_me_share(CCContex* new_txn) {
    for (CCContex* reader : r_locker) {
      if (reader == new_txn) return true;
    }
    return false;
  }
  bool locked_me(CCContex* new_txn) {
    if (locked_me_share(new_txn)) return true;
    if (w_locker == new_txn) return true;
    return false;
  }
  bool should_abort(CCContex* new_txn) {
    if (_dead_lock == kNoWait) return true;
    if (w_locker != nullptr) {
      if (new_txn->timestamp > w_locker->timestamp) {
        // std::cout << "abort " << new_txn->txn_id << " due to conflict: wlocker is "<<w_locker->txn_id << "\n";
        return true;
      } else {
        return false;
      }
      return new_txn->txn_id > w_locker->txn_id;
    }
    for (CCContex* reader : r_locker) {
      if (new_txn->timestamp > reader->timestamp) {
        // std::cout << "abort " << new_txn->txn_id << " due to conflict: rlocker is "<<reader->txn_id << "\n";
        return true;
      }
      if (new_txn->txn_id > reader->txn_id) return true;
    }
    return false;
  }
  using VFuture=folly::Future<folly::Unit>;
  using VPromise=folly::Promise<folly::Unit>;
  std::mutex _m;
  std::condition_variable _cv;
  // std::atomic_flag _latch;
  bool wlocked = false;
  uint32_t reader=0, w_waiter=0;

  CCContex* w_locker = nullptr;
  std::vector<CCContex*> r_locker;

  // std::shared_mutex _latch;
  /**
   * @return true lock success
   * @return false 
   */
  void Wlock(CCContex* ctx) {
    std::unique_lock<std::mutex> lk(_m);
    if (w_locker == ctx) {
      lk.unlock();
      return;
    };
    if (locked_me_share(ctx)) {
      // need to upgrade the lock
      w_waiter++;
      while (wlocked || reader > 1) { // left 1, since I'm holding the read lock
        if (should_abort(ctx)) {
          lk.unlock();
          w_waiter--;
          throw CCFail(Formatter() << "abort " << ctx->txn_id << " due to conflict: wlocker is "<<w_locker->txn_id);
        }
        _cv.wait(lk);
      }
      w_waiter--;
      reader--;
      if (r_locker.size() != 1) throw FatalException("I'm not the only one?");
      r_locker.clear();
      wlocked = true;
      w_locker = ctx;
      lk.unlock();
    } else {
      // directly obtain the lock
      w_waiter++;
      while (wlocked || reader > 0) {
        if (should_abort(ctx)) {
          lk.unlock();
          w_waiter--;
          throw CCFail("abort due to conflict");
        }
        _cv.wait(lk);
      }
      w_waiter--;
      wlocked = true;
      w_locker = ctx;
      locker_count.fetch_add(1);
      lk.unlock();
    }
    
  }
  // bool TryWlock(CCContex* ctx) {
  //   std::unique_lock<std::mutex> lk(_m);
  //   if (w_locker == ctx) {
  //     lk.unlock();
  //     return true;
  //   }

  //   if (wlocked || reader > 0) {
  //     lk.unlock(); return false;
  //   }
  //   wlocked = true;
  //   w_locker = ctx;
  //   lk.unlock();
  //   return true;
  // }
  VFuture WlockAsync(CCContex* ctx) {
    auto p = VPromise();
    auto f = p.getFuture();
    // todo: use thread pool to do this
    std::thread([p=std::move(p), this, ctx]()mutable{
      p.setWith([this, ctx](){
        Wlock(ctx);
      });
      // try {
      //   Wlock(ctx);
      //   p.setValue();
      // } catch (std::exception & e) {
      //   std::cout << "abort a txn:" << e.what() << "\n";
      //   if (auto e_p = dynamic_cast<CCFail*>(&e)) {
      //     std::cout << "this is a abort exception:" << e_p->what() << "\n";
      //   }
      //   p.setException(e);
      // }
    }).detach();
    return f;
  }
  void Wunlock(CCContex* ctx) {
    std::unique_lock<std::mutex> lk(_m);
    if (locked_me(ctx) == false) throw FatalException("unlock without lock");
    wlocked = false;
    w_locker = nullptr;
    locker_count.fetch_sub(1);
    lk.unlock();
    _cv.notify_all();
  }
  void Rlock(CCContex* ctx) {
    std::unique_lock<std::mutex> lk(_m);
    if (locked_me(ctx)) {
      lk.unlock();
      return;
    }

    while (wlocked || (WWaiterBlockR && w_waiter > 0)) {
      if (should_abort(ctx)) {
        lk.unlock();
        throw CCFail("Abort due to conflict");
      }
      _cv.wait(lk);
    }
    r_locker.push_back(ctx);
    locker_count.fetch_add(1);
    reader++;
    lk.unlock();
  }
  // bool TryRlock(CCContex* ctx) {
  //   std::unique_lock<std::mutex> lk(_m);
  //   if (locked_me(ctx)) {
  //     lk.unlock();
  //     return true;
  //   }

  //   if (wlocked || (WWaiterBlockR && w_waiter > 0)) {
  //     lk.unlock(); return false;
  //   }
  //   r_locker.push_back(ctx);
  //   reader++;
  //   lk.unlock();
  //   return true;
  // }
  VFuture RlockAsync(CCContex* ctx) {
    auto p = VPromise();
    auto f = p.getFuture();
    // todo: use thread pool to do this
    std::thread([p=std::move(p), this, ctx]()mutable{
      p.setWith([this, ctx](){
        Rlock(ctx);
      });
      // try {
      //   Rlock(ctx);
      //   p.setValue();
      // } catch (const std::exception & e) {
      //   std::cout << "abort a txn:" << e.what() << "\n";
      //   if (auto e_p = dynamic_cast<const CCFail*>(&e)) {
      //     std::cout << "this is a abort exception:" << e_p->what() << "\n";
      //   }
      //   p.setException(e);
      // }
    }).detach();
    return f;
  }
  void Runlock(CCContex* ctx) {
    std::unique_lock<std::mutex> lk(_m);
    if (locked_me(ctx) == false) throw FatalException("unlock without lock");
    reader--;
    
    for (auto iter = r_locker.begin(); iter != r_locker.end(); iter++) {
      if (*iter.base() == ctx) {
        r_locker.erase(iter);
        break;
      }
    }
    locker_count.fetch_sub(1);
    lk.unlock();
    _cv.notify_all(); 
    // fixme: only notify one, what if notify reader but writer is waiting?
    // fixme: only notify one, but there might be another request that can only it can be granted:
    //   example: r1 r2, w3(blocked), 1upgrade(blocked), ur2, 3 is checked but waiting. 1 is never checked
  }
 public:
  void Upgrade(CCContex* ctx) {
    std::unique_lock<std::mutex> lk(_m);
    if (locked_me_share(ctx) == false) throw FatalException("Upgrade without share lock held");
    if (w_locker == ctx) throw FatalException("Wlock shouldn't be upgraded");
    
    w_waiter++;
    while (wlocked || reader > 1) { // left 1, since I'm holding the read lock
      if (should_abort(ctx)) {
        lk.unlock();
        w_waiter--;
        throw CCFail("abort due to conflict");
      }
      _cv.wait(lk);
    }
    w_waiter--;
    reader--;
    if (r_locker.size() != 1) throw FatalException("I'm not the only one?");
    r_locker.clear();
    wlocked = true;
    w_locker = ctx;
    lk.unlock();
  }
  void Lock(const bool share, CCContex* ctx) {
    if (share) return Rlock(ctx);
    return Wlock(ctx);
  }
  VFuture LockAsync(const bool share, CCContex* ctx) {
    if (share) return RlockAsync(ctx);
    return WlockAsync(ctx);
  }
  VFuture UpgradeAsync(CCContex* ctx) {
    auto p = VPromise();
    auto f = p.getFuture();
    // todo: use thread pool to do this
    std::thread([p=std::move(p), this, ctx]()mutable{
      p.setWith([this, ctx](){
        Upgrade(ctx);
      });
      // try {
      //   Upgrade(ctx);
      //   p.setValue();
      // } catch (std::exception & e) {
      //   std::cout << "abort a txn:" << e.what() << "\n";
      //   if (auto e_p = dynamic_cast<CCFail*>(&e)) {
      //     std::cout << "this is a abort exception:" << e_p->what() << "\n";
      //   }
      //   p.setException(e);
      // }
    }).detach();
    return f;
  }
  void Unlock(const bool share, CCContex* ctx) {
    if (share)
      Runlock(ctx);
    else 
      Wunlock(ctx);
  }
  
};

struct QueuedReq {
  using VFuture=folly::Future<folly::Unit>;
  using VPromise=folly::Promise<folly::Unit>;
  VPromise _p;
  bool _share;
  CCContex* _ctx;
  QueuedReq(bool share, CCContex* ctx) : _p(), _share(share), _ctx(ctx) {}
};

class AsyncQueueRWLock {
 private:
  using VFuture=folly::Future<folly::Unit>;
  using VPromise=folly::Promise<folly::Unit>;
  std::queue<QueuedReq> _waiters;
  std::queue<QueuedReq> _upgrade_waiters;
  // std::mutex _m;
  // std::condition_variable _cv;
  std::atomic_flag _latch = ATOMIC_FLAG_INIT;
  bool wlocked = false;
  uint32_t reader=0, w_waiter=0;

  CCContex* w_locker = nullptr;
  std::vector<CCContex*> r_locker;
  bool locked_me_share(CCContex* new_txn) {
    for (CCContex* reader : r_locker) {
      if (reader == new_txn) return true;
    }
    return false;
  }
  bool locked_me(CCContex* new_txn) {
    if (locked_me_share(new_txn)) return true;
    if (w_locker == new_txn) return true;
    return false;
  }
  bool should_abort(CCContex* new_txn) {
    if (_dead_lock == kNoWait) return true;
    if (w_locker != nullptr) {
      if (new_txn->timestamp > w_locker->timestamp) {
        // std::cout << "abort " << new_txn->txn_id << " due to conflict: wlocker is "<<w_locker->txn_id << "\n";
        return true;
      } else {
        return false;
      }
      return new_txn->txn_id > w_locker->txn_id;
    }
    for (CCContex* reader : r_locker) {
      if (new_txn->timestamp > reader->timestamp) {
        // std::cout << "abort " << new_txn->txn_id << " due to conflict: rlocker is "<<reader->txn_id << "\n";
        return true;
      }
      if (new_txn->txn_id > reader->txn_id) return true;
    }
    return false;
  }

  // std::shared_mutex _latch;
  /**
   * @return true lock success
   * @return false 
   */
  VFuture Wlock(CCContex* ctx) {
    while(_latch.test_and_set());
    if (w_locker == ctx) {
      _latch.clear();
      return folly::makeFuture();
    };
    
    if (wlocked == false && reader == 0) {
      // case 1, no one is holding the lock, directly obtain the lock
      w_locker = ctx;
      wlocked = true;
      locker_count.fetch_add(1);
      _latch.clear();
      return folly::makeFuture();
    }
    
    if (locked_me_share(ctx)) {
      // case 2, me is holding the share lock
      // need to upgrade the lock
      if (reader == 1) {
        if (r_locker[0] != ctx) throw FatalException("Something wrong with lockedme");
        if (_upgrade_waiters.size() != 0) throw FatalException("upgrater should be empty");
        wlocked = true;
        reader = 0;
        w_locker = ctx;
        r_locker.clear();
        _latch.clear();
        return folly::makeFuture();
      } else {
        if (should_abort(ctx)) {
          _latch.clear();
          throw CCFail("conflict");
        }
        w_waiter++;
        // fixme: check ts to see abort
        _upgrade_waiters.emplace(false, ctx);
        auto f = _upgrade_waiters.back()._p.getFuture();
        _latch.clear();
        return f;
      }
    } else {
      if (should_abort(ctx)) {
        _latch.clear();
        throw CCFail("conflict");
      }
      // need to wait
      w_waiter++;
      // fixme: check ts to see abort
      _waiters.emplace(false, ctx);
      auto f = _waiters.back()._p.getFuture();
      _latch.clear();
      return f;
    }
  }
  void Wunlock(CCContex* ctx) {
    while(_latch.test_and_set());
    if (locked_me(ctx) == false) throw FatalException("unlock without lock");
    if (_waiters.size() == 0) {
      wlocked = false;
      w_locker = nullptr;
      locker_count.fetch_sub(1);
      _latch.clear();
      return;
    }
    if (_waiters.front()._share == false) {
      QueuedReq* req = &_waiters.front();
      w_waiter--;
      w_locker = req->_ctx;
      req->_p.setValue();
      _waiters.pop();
      _latch.clear();
      return;
    } else {
      wlocked = false;
      w_locker = nullptr;
      locker_count.fetch_sub(1);
      if (reader != 0) throw FatalException("unlock writer, but reader is not 0?");
      while (true) {
        QueuedReq* req = &_waiters.front();
        if (req->_share == false) break;
        reader++;
        r_locker.push_back(req->_ctx);
        req->_p.setValue();
        // fixme: locker_count
        _waiters.pop();
        locker_count.fetch_add(1);
      }
      _latch.clear();
      return;
    }
  }
  VFuture Rlock(CCContex* ctx) {
    while(_latch.test_and_set());
    if (locked_me(ctx)) {
      _latch.clear();
      return folly::makeFuture();
    }

    if (wlocked == false && w_waiter == 0) {
      if (_waiters.size() != 0 || _upgrade_waiters.size() != 0)
        throw FatalException("no writer in count, but queue is not empty");
      reader++;
      r_locker.push_back(ctx);
      locker_count.fetch_add(1);
      _latch.clear();
      return folly::makeFuture();
    } else {
      if (should_abort(ctx)) {
        _latch.clear();
        throw CCFail("conflict");
      }
      // fixme: check ts to see abort
      _waiters.emplace(true, ctx);
      auto f = _waiters.back()._p.getFuture();
      _latch.clear();
      return f;
    }
  }
  void Runlock(CCContex* ctx) {
    while (_latch.test_and_set());
    if (locked_me(ctx) == false) throw FatalException("unlock without lock");
    reader--;
    locker_count.fetch_sub(1);
    
    for (auto iter = r_locker.begin(); iter != r_locker.end(); iter++) {
      if (*iter.base() == ctx) {
        r_locker.erase(iter);
        break;
      }
    }
    if (reader == 1 && _upgrade_waiters.front()._ctx == r_locker[0]) {
      // the only reader left is waiting for w lock; grant write lock
      QueuedReq * req = &_upgrade_waiters.front();
      if (req->_share == true) throw FatalException("reader is waiting for the same read?");
      wlocked = true;
      reader = 0;
      w_waiter--;
      w_locker = req->_ctx;
      r_locker.clear();
      req->_p.setValue();
      _upgrade_waiters.pop();
      locker_count.fetch_add(1);
      _latch.clear();
      return;
    }
    if (reader>0) {
      _latch.clear();
      return;
    }
    // reader == 0
    if (_upgrade_waiters.size() != 0) throw FatalException("No reader left, but still someone waiting for upgrade?");
    if (_waiters.size() == 0) {
      _latch.clear(); return;
    }
    QueuedReq* req = &_waiters.front();
    if (req->_share == true) {
      throw FatalException("release reader, no reader left, but still reader waiting");
    }
    wlocked = true;
    w_waiter--;
    w_locker = req->_ctx;
    req->_p.setValue();
    _waiters.pop();
    locker_count.fetch_add(1);
    _latch.clear();
    return;

    locker_count.fetch_sub(1);
  }
 public:
  VFuture Upgrade(CCContex* ctx) {
    while(_latch.test_and_set());
    if (w_locker == ctx) {
      throw FatalException("writer is upgrading");
    };

    if (!locked_me_share(ctx)) throw FatalException("Not a reader");

    // case 2, me is holding the share lock
    // need to upgrade the lock
    if (reader == 1) {
      if (_upgrade_waiters.size() != 0) throw FatalException("upgrater should be empty");
      wlocked = true;
      reader = 0;
      w_locker = ctx;
      r_locker.clear();
      _latch.clear();
      return folly::makeFuture();
    } else {
      w_waiter++;
      // fixme: check ts to see abort
      _upgrade_waiters.emplace(false, ctx);
      auto f = _upgrade_waiters.back()._p.getFuture();
      _latch.clear();
      return f;
    }
  }
  VFuture Lock(const bool share, CCContex* ctx) {
    if (share) return Rlock(ctx);
    return Wlock(ctx);
  }
  inline VFuture LockAsync(bool share, CCContex* ctx) {
    return Lock(share, ctx);
  }
  inline VFuture UpgradeAsync(CCContex* ctx) {
    return Upgrade(ctx);
  }
  void Unlock(const bool share, CCContex* ctx) {
    if (share)
      Runlock(ctx);
    else 
      Wunlock(ctx);
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
  std::vector<RWLOCK_T> _locks;

  const std::hash<labeled_id_t> _n_id_hasher = std::hash<labeled_id_t>();
  // uint64_t HashNodeIdx(const label_t n_l) {
  //   return n_l % _node_idx_locks.size();
  // }
 public: 
  uint64_t HashNodeProp(const labeled_id_t n_id) {
    return _n_id_hasher(n_id) % _node_p_lock_count + _n_idx_lock_count;
  }
  uint64_t HashAdjIdx(const label_t e_l, const labeled_id_t n_id, const dir_t dir) {
    return (_n_id_hasher(n_id) * (e_l + 1)
           * (dir ? 18446744073709551557ull : 18446744073709551533ull) )
           % _adj_idx_lock_count + _n_idx_lock_count + _node_p_lock_count;
  }
  uint64_t HashEdgeProp(const label_t e_l, const labeled_id_t id1, const labeled_id_t id2) {
    return (_n_id_hasher(id1) * _n_id_hasher(id2) * (e_l + 1)) 
           % _edge_p_lock_count + _n_idx_lock_count + _node_p_lock_count + _adj_idx_lock_count ;
  }
  LockManager(int n_idx_locks, int n_p_locks, int adj_idx_locks, int e_p_locks) :
      _n_idx_lock_count(n_idx_locks),
      _node_p_lock_count(n_p_locks),
      _adj_idx_lock_count(adj_idx_locks),
      _edge_p_lock_count(e_p_locks),
      // _node_idx_locks(n_idx_locks), 
      // _node_p_locks(n_p_locks), 
      // _adj_idx_locks(adj_idx_locks), _edge_p_locks(e_p_locks),
      _locks(n_idx_locks+ n_p_locks+adj_idx_locks + e_p_locks) {
    // 
    assert(n_idx_locks == 0);
  }
  // RWLOCK_T & NIdx(const label_t n_l) { return _node_idx_locks.at(HashNodeIdx(n_l)); }
  RWLOCK_T & NProp(const labeled_id_t n_id) { return _locks.at(HashNodeProp(n_id)); }
  RWLOCK_T & AdjIdx(const label_t e_l, const labeled_id_t n_id, const dir_t dir) {
    return _locks.at(HashAdjIdx(e_l, n_id, dir));
  }
  RWLOCK_T & EProp(const label_t e_l, const labeled_id_t id1, const labeled_id_t id2) {
    return _locks.at(HashEdgeProp(e_l, id1, id2));
  }
  inline RWLOCK_T & GetLock(const uint64_t hashed) {
    return _locks.at(hashed);
  }
};