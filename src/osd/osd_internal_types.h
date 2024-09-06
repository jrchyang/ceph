// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OSD_INTERNAL_TYPES_H
#define CEPH_OSD_INTERNAL_TYPES_H

#include "osd_types.h"
#include "OpRequest.h"
#include "object_state.h"

/*
  * keep tabs on object modifications that are in flight.
  * we need to know the projected existence, size, snapset,
  * etc., because we don't send writes down to disk until after
  * replicas ack.
  */

// SnapSet 的内存版本，主要增加了引用计数机制，便于 SS 属性在 head 对象、克隆对象以及
// snapdir 对象之间共享
struct SnapSetContext {
  hobject_t oid;	// 对象关联的 snapdir 对象标识
  SnapSet snapset;	// SS 属性
  int ref;		// 引用计数，SnapSetContext 可能被同一个对象的多个相关对象
			// （head 对象、克隆对象、snapdir 对象）关联
  bool registered : 1;	// 是否已经将 SnapSetContext 加入缓存
  bool exists : 1;	// 指示 SnapSet 是否存在

  explicit SnapSetContext(const hobject_t& o) :
    oid(o), ref(0), registered(false), exists(true) { }
};
struct ObjectContext;
typedef std::shared_ptr<ObjectContext> ObjectContextRef;

/**
 * 对象上下文保存了对象的 OI 和 SS 属性，此外，内部实现了一个属性缓存（主要用于缓存用户自定义
 * 属性对）和读写互斥锁机制，用于对来自客户端的 op 进行保序
 *
 * op 操作对象之前，必须先获取对象上下文；在进行读写之前，则必须获得对象上下文的读锁（对应
 * op 仅包含读操作）或者写操作（对应 op 包含写操作）
 *
 * 原则上，op_shardedwq 的实现原理可用于对访问同一个 PG 的 op 进行保序，但是由于写是异步的
 * （纠删码存储池的读也是异步的），即写操作在执行过程如果遇到堵塞会让出 CPU，所以需要在对象
 * 上下文中额外引入一套读写锁互斥锁机制来对 op 进行保序
 */
struct ObjectContext {
  ObjectState obs;	// 对象状态
  SnapSetContext *ssc;  // may be null

  Context *destructor_callback;

public:

  // any entity in obs.oi.watchers MUST be in either watchers or unconnected_watchers.
  std::map<std::pair<uint64_t, entity_name_t>, WatchRef> watchers;

  // attr cache
  // 属性（指用户自定义属性）缓存
  std::map<std::string, ceph::buffer::list, std::less<>> attr_cache;

  /**
   * 读写锁，用于对来自客户端的 op 进行排队，以保证数据一致性
   * 共有三种类型：RWREAD、RWWRITE、RWEXCL
   * 与常见的读写锁实现逻辑不同，上述三种类型中，只有 RWEXCL 是真正的互斥锁，其他两种都可以
   *   被重复加锁（RWWRITE 也可以被强制重复加锁）
   * rwstate 内部维护了一个 op 等待队列，如果加锁失败，对应的 op 会进入等待队列进行等待，
   *   被唤醒后，按入队顺序以此重试以获取所请求类型的锁
   * 注意：除上述读写锁之外，为了防止 Ceph 内部诸如 Cache Tier、Recovery/Backfill 等
   *   机制产生的本地读写请求（这类请求不会创建 OpContext。事实上，如果对应的对象处于
   *   Recovery/Backfill 过程之中，相应的客户端请求会被阻塞）与客户端产生的（分布式）读写
   *   请求产生冲突，ObjectContext 还实现了另一套读写互斥锁，成为 ondisk_read/write_lock
   *   供 PG 访问本地数据时使用
   */
  RWState rwstate;
  std::list<OpRequestRef> waiters;  ///< ops waiting on state change
  bool get_read(OpRequestRef& op) {
    if (rwstate.get_read_lock()) {
      return true;
    } // else
      // Now we really need to bump up the ref-counter.
    waiters.emplace_back(op);
    rwstate.inc_waiters();
    return false;
  }
  bool get_write(OpRequestRef& op, bool greedy=false) {
    if (rwstate.get_write_lock(greedy)) {
      return true;
    } // else
    if (op) {
      waiters.emplace_back(op);
      rwstate.inc_waiters();
    }
    return false;
  }
  bool get_excl(OpRequestRef& op) {
    if (rwstate.get_excl_lock()) {
      return true;
    } // else
    if (op) {
      waiters.emplace_back(op);
      rwstate.inc_waiters();
    }
    return false;
  }
  void wake(std::list<OpRequestRef> *requeue) {
    rwstate.release_waiters();
    requeue->splice(requeue->end(), waiters);
  }
  void put_read(std::list<OpRequestRef> *requeue) {
    if (rwstate.put_read()) {
      wake(requeue);
    }
  }
  void put_write(std::list<OpRequestRef> *requeue) {
    if (rwstate.put_write()) {
      wake(requeue);
    }
  }
  void put_excl(std::list<OpRequestRef> *requeue) {
    if (rwstate.put_excl()) {
      wake(requeue);
    }
  }
  bool empty() const { return rwstate.empty(); }

  bool get_lock_type(OpRequestRef& op, RWState::State type) {
    switch (type) {
    case RWState::RWWRITE:
      return get_write(op);
    case RWState::RWREAD:
      return get_read(op);
    case RWState::RWEXCL:
      return get_excl(op);
    default:
      ceph_abort_msg("invalid lock type");
      return true;
    }
  }
  bool get_write_greedy(OpRequestRef& op) {
    return get_write(op, true);
  }
  bool get_snaptrimmer_write(bool mark_if_unsuccessful) {
    return rwstate.get_snaptrimmer_write(mark_if_unsuccessful);
  }
  bool get_recovery_read() {
    return rwstate.get_recovery_read();
  }
  bool try_get_read_lock() {
    return rwstate.get_read_lock();
  }
  void drop_recovery_read(std::list<OpRequestRef> *ls) {
    ceph_assert(rwstate.recovery_read_marker);
    put_read(ls);
    rwstate.recovery_read_marker = false;
  }
  void put_lock_type(
    RWState::State type,
    std::list<OpRequestRef> *to_wake,
    bool *requeue_recovery,
    bool *requeue_snaptrimmer) {
    switch (type) {
    case RWState::RWWRITE:
      put_write(to_wake);
      break;
    case RWState::RWREAD:
      put_read(to_wake);
      break;
    case RWState::RWEXCL:
      put_excl(to_wake);
      break;
    default:
      ceph_abort_msg("invalid lock type");
    }
    if (rwstate.empty() && rwstate.recovery_read_marker) {
      rwstate.recovery_read_marker = false;
      *requeue_recovery = true;
    }
    if (rwstate.empty() && rwstate.snaptrimmer_write_marker) {
      rwstate.snaptrimmer_write_marker = false;
      *requeue_snaptrimmer = true;
    }
  }
  bool is_request_pending() {
    return !rwstate.empty();
  }

  ObjectContext()
    : ssc(NULL),
      destructor_callback(0),
      blocked(false), requeue_scrub_on_unblock(false) {}

  ~ObjectContext() {
    ceph_assert(rwstate.empty());
    if (destructor_callback)
      destructor_callback->complete(0);
  }

  void start_block() {
    ceph_assert(!blocked);
    blocked = true;
  }
  void stop_block() {
    ceph_assert(blocked);
    blocked = false;
  }
  bool is_blocked() const {
    return blocked;
  }

  /// in-progress copyfrom ops for this object
  bool blocked:1;
  bool requeue_scrub_on_unblock:1;    // true if we need to requeue scrub on unblock

};

inline std::ostream& operator<<(std::ostream& out, const ObjectState& obs)
{
  out << obs.oi.soid;
  if (!obs.exists)
    out << "(dne)";
  return out;
}

inline std::ostream& operator<<(std::ostream& out, const ObjectContext& obc)
{
  return out << "obc(" << obc.obs << " " << obc.rwstate << ")";
}

class ObcLockManager {
  struct ObjectLockState {
    ObjectContextRef obc;
    RWState::State type;
    ObjectLockState(
      ObjectContextRef obc,
      RWState::State type)
      : obc(std::move(obc)), type(type) {}
  };
  std::map<hobject_t, ObjectLockState> locks;
public:
  ObcLockManager() = default;
  ObcLockManager(ObcLockManager &&) = default;
  ObcLockManager(const ObcLockManager &) = delete;
  ObcLockManager &operator=(ObcLockManager &&) = default;
  bool empty() const {
    return locks.empty();
  }
  bool get_lock_type(
    RWState::State type,
    const hobject_t &hoid,
    ObjectContextRef& obc,
    OpRequestRef& op) {
    ceph_assert(locks.find(hoid) == locks.end());
    if (obc->get_lock_type(op, type)) {
      locks.insert(std::make_pair(hoid, ObjectLockState(obc, type)));
      return true;
    } else {
      return false;
    }
  }
  /// Get write lock, ignore starvation
  bool take_write_lock(
    const hobject_t &hoid,
    ObjectContextRef obc) {
    ceph_assert(locks.find(hoid) == locks.end());
    if (obc->rwstate.take_write_lock()) {
      locks.insert(
	std::make_pair(
	  hoid, ObjectLockState(obc, RWState::RWWRITE)));
      return true;
    } else {
      return false;
    }
  }
  /// Get write lock for snap trim
  bool get_snaptrimmer_write(
    const hobject_t &hoid,
    ObjectContextRef obc,
    bool mark_if_unsuccessful) {
    ceph_assert(locks.find(hoid) == locks.end());
    if (obc->get_snaptrimmer_write(mark_if_unsuccessful)) {
      locks.insert(
	std::make_pair(
	  hoid, ObjectLockState(obc, RWState::RWWRITE)));
      return true;
    } else {
      return false;
    }
  }
  /// Get write lock greedy
  bool get_write_greedy(
    const hobject_t &hoid,
    ObjectContextRef obc,
    OpRequestRef op) {
    ceph_assert(locks.find(hoid) == locks.end());
    if (obc->get_write_greedy(op)) {
      locks.insert(
	std::make_pair(
	  hoid, ObjectLockState(obc, RWState::RWWRITE)));
      return true;
    } else {
      return false;
    }
  }

  /// try get read lock
  bool try_get_read_lock(
    const hobject_t &hoid,
    ObjectContextRef obc) {
    ceph_assert(locks.find(hoid) == locks.end());
    if (obc->try_get_read_lock()) {
      locks.insert(
	std::make_pair(
	  hoid,
	  ObjectLockState(obc, RWState::RWREAD)));
      return true;
    } else {
      return false;
    }
  }

  void put_locks(
    std::list<std::pair<ObjectContextRef, std::list<OpRequestRef> > > *to_requeue,
    bool *requeue_recovery,
    bool *requeue_snaptrimmer) {
    for (auto& p: locks) {
      std::list<OpRequestRef> _to_requeue;
      p.second.obc->put_lock_type(
	p.second.type,
	&_to_requeue,
	requeue_recovery,
	requeue_snaptrimmer);
      if (to_requeue) {
        // We can safely std::move here as the whole `locks` is going
        // to die just after the loop.
	to_requeue->emplace_back(std::move(p.second.obc),
				 std::move(_to_requeue));
      }
    }
    locks.clear();
  }
  ~ObcLockManager() {
    ceph_assert(locks.empty());
  }
};



#endif
