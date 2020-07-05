#pragma once

#include "type.hpp"
#include "lock.hpp"
#include "cc_ctx.hpp"
#include <folly/futures/Future.h>

class CCManager {
 private:
  using VFuture=folly::Future<folly::Unit>;
  using VPromise=folly::Promise<folly::Unit>;

 public:
  virtual VFuture GrantAccessNProp(
    const node_id_t n_id, const bool share, CCContex* ctx);
  virtual VFuture GrantAccessAdjIdx(
    const label_t e_l, const node_id_t n_id, 
    const dir_t dir, const bool share, CCContex* ctx);
  virtual VFuture GrantAccessEProp(
    const label_t e_l, const node_id_t id1, 
    const node_id_t id2, const bool share, CCContex* ctx);
  virtual VFuture ReleaseAccessNProp(
    const node_id_t n_id, const bool share, CCContex* ctx);
  virtual VFuture ReleaseAccessAdjIdx(
    const label_t e_l, const node_id_t n_id, 
    const dir_t dir, const bool share, CCContex* ctx);
  virtual VFuture ReleaseAccessEProp(
    const label_t e_l, const node_id_t id1, 
    const node_id_t id2, const bool share, CCContex* ctx);
  virtual bool CommitCheck(CCContex* ctx);
  virtual void Commit(CCContex* ctx);
  virtual void Abort(CCContex* ctx);
};


class CCManager2PL {
 private:
  using VFuture=folly::Future<folly::Unit>;
  using VPromise=folly::Promise<folly::Unit>;
  LockManager* _locks;
 public:
  VFuture GrantAccessNProp(
      const node_id_t n_id, const bool share, CCContex* ctx) {
    // TODO
    // hash to the lock, then return its future
  }
  VFuture GrantAccessAdjIdx(
    const label_t e_l, const node_id_t n_id, 
    const dir_t dir, const bool share, CCContex* ctx) {
    // TODO
  }
  VFuture GrantAccessEProp(
      const label_t e_l, const node_id_t id1, 
      const node_id_t id2, const bool share, CCContex* ctx) {
    // TODO
  }
};