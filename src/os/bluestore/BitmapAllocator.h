// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_BITMAPFASTALLOCATOR_H
#define CEPH_OS_BLUESTORE_BITMAPFASTALLOCATOR_H

#include <mutex>

#include "Allocator.h"
#include "os/bluestore/bluestore_types.h"
#include "fastbmap_allocator_impl.h"
#include "include/mempool.h"
#include "common/debug.h"

class BitmapAllocator : public Allocator,
  public AllocatorLevel02<AllocatorLevel01Loose> {
  CephContext* cct;
public:
  BitmapAllocator(CephContext* _cct, int64_t capacity, int64_t alloc_unit,
		  std::string_view name);
  ~BitmapAllocator() override
  {
  }

  const char* get_type() const override
  {
    return "bitmap";
  }

  /**
   * 分配/释放空间
   * 分配的空间不一定是连续的，有可能是一些离散的段。
   * allocate() 接口可以同时指定 hint 参数，用于对下次开始分配的起始地址（例如可以时上一次
   * 成功分配后返回空间的结束地址）进行预测，以提升分配速率
   */
  int64_t allocate(
    uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
    int64_t hint, PExtentVector *extents) override;
  void release(
    const interval_set<uint64_t>& release_set) override;

  using Allocator::release;

  /**
   * 返回当前 BitmapAllocator 实例中的所有空闲空间的大小
   */
  uint64_t get_free() override
  {
    return get_available();
  }

  void dump() override;
  void foreach(
    std::function<void(uint64_t offset, uint64_t length)> notify) override
  {
    foreach_internal(notify);
  }
  double get_fragmentation() override
  {
    return get_fragmentation_internal();
  }

  /**
   * BlueStore 上电时，通过 FreelistManager 读取磁盘中空闲的段，然后调用本接口将
   * BitmapAllocator 中相应的段空间标记为空闲
   */
  void init_add_free(uint64_t offset, uint64_t length) override;
  /**
   * 将 BitmapAllocator 指定范围的空间（ [offset, offset+length] ）标记为已分配
   */
  void init_rm_free(uint64_t offset, uint64_t length) override;

  void shutdown() override;
};

#endif
