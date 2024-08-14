// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Bitmap based in-memory allocator implementation.
 * Author: Igor Fedotov, ifedotov@suse.com
 *
 */

#ifndef __FAST_BITMAP_ALLOCATOR_IMPL_H
#define __FAST_BITMAP_ALLOCATOR_IMPL_H
#include "include/intarith.h"

#include <vector>
#include <algorithm>
#include <mutex>

typedef uint64_t slot_t;

#ifdef NON_CEPH_BUILD
#include <assert.h>
struct interval_t
{
  uint64_t offset = 0;
  uint64_t length = 0;

  interval_t() {}
  interval_t(uint64_t o, uint64_t l) : offset(o), length(l) {}
  interval_t(const interval_t &ext) :
    offset(ext.offset), length(ext.length) {}
};
typedef std::vector<interval_t> interval_vector_t;
typedef std::vector<slot_t> slot_vector_t;
#else
#include "include/ceph_assert.h"
#include "common/likely.h"
#include "os/bluestore/bluestore_types.h"
#include "include/mempool.h"
#include "common/ceph_mutex.h"

typedef bluestore_interval_t<uint64_t, uint64_t> interval_t;
typedef PExtentVector interval_vector_t;

typedef mempool::bluestore_alloc::vector<slot_t> slot_vector_t;

#endif

// fitting into cache line on x86_64
/**
 * slot 的类型为 uint64_t 占 8 个字节
 * cpu cache line 为 64 字节
 * slotset 为 8 * slot = 64 字节，与 cpu cache line 对齐
 */
static const size_t slots_per_slotset = 8; // 8 slots per set
static const size_t slotset_bytes = sizeof(slot_t) * slots_per_slotset;
static const size_t bits_per_slot = sizeof(slot_t) * 8;
static const size_t bits_per_slotset = slotset_bytes * 8;
static const slot_t all_slot_set = 0xffffffffffffffff;
static const slot_t all_slot_clear = 0;

/**
 * 返回 bits_per_slot 表示未找到，即 slot_val 对应的所有位置均已分配
 */
inline size_t find_next_set_bit(slot_t slot_val, size_t start_pos)
{
#ifdef __GNUC__
  if (start_pos == 0) {
    start_pos = __builtin_ffsll(slot_val);
    return start_pos ? start_pos - 1 : bits_per_slot;
  }
#endif
  /**
   * mask is like : 0010 0000
   * slot_val & mask == 0 意味着查询位置的值为 0，即已分配，继续 while 循环
   */
  slot_t mask = slot_t(1) << start_pos;
  while (start_pos < bits_per_slot && !(slot_val & mask)) {
    mask <<= 1;
    ++start_pos;
  }
  return start_pos;
}


class AllocatorLevel
{
protected:

  virtual uint64_t _children_per_slot() const = 0;
  virtual uint64_t _level_granularity() const = 0;

public:
  static uint64_t l0_dives;
  static uint64_t l0_iterations;
  static uint64_t l0_inner_iterations;
  static uint64_t alloc_fragments;
  static uint64_t alloc_fragments_fast;
  static uint64_t l2_allocs;

  virtual ~AllocatorLevel()
  {}

  virtual void collect_stats(
    std::map<size_t, size_t>& bins_overall) = 0;

};

class AllocatorLevel01 : public AllocatorLevel
{
protected:
  slot_vector_t l0; // 所有 L0 层对应的 bit
  slot_vector_t l1; // 所有 L1 层对应的 bit
  uint64_t l0_granularity = 0; // space per entry，L0 层的粒度，一个 bit 表示的空间大小，即最小分配单元
  uint64_t l1_granularity = 0; // space per entry，L1 层的粒度，表示一个 entry 对应的 64 bytes (1 l0 slotset) * l0_granularity，与 cpu cache line 对齐

  size_t partial_l1_count = 0; // l1 部分空闲的个数
  size_t unalloc_l1_count = 0; // l0 未分配的 AU 的个数，即 l1 完全空间的个数

  double get_fragmentation() const {
    double res = 0.0;
    auto total = unalloc_l1_count + partial_l1_count;
    if (total) {
      res = double(partial_l1_count) / double(total);
    }
    return res;
  }

  uint64_t _level_granularity() const override
  {
    return l1_granularity;
  }

  inline bool _is_slot_fully_allocated(uint64_t idx) const {
    return l1[idx] == all_slot_clear;
  }
public:
  inline uint64_t get_min_alloc_size() const
  {
    return l0_granularity;
  }

};

template <class T>
class AllocatorLevel02;

class AllocatorLevel01Loose : public AllocatorLevel01
{
  enum {
    // 多少个 bit 管理 64 个 L0 bit
    L1_ENTRY_WIDTH = 2,
    // 二进制表示：0x11
    L1_ENTRY_MASK = (1 << L1_ENTRY_WIDTH) - 1,
    // 二进制表示：0x00，表示所管理的 L0 的 64 个 AU 已全部分配
    L1_ENTRY_FULL = 0x00,
    // 二进制表示：0x01，表示所管理的 L0 的 64 个 AU 已部分分配
    L1_ENTRY_PARTIAL = 0x01,
    // 二进制表示：0x10，当前代码中未使用
    L1_ENTRY_NOT_USED = 0x02,
    // 二进制表示：0x11，表示所管理的 L0 的 64 个 AU 全部未使用
    L1_ENTRY_FREE = 0x03,
    // 一个 uint64_t 表示多少个 L1 entry
    L1_ENTRIES_PER_SLOT = bits_per_slot / L1_ENTRY_WIDTH, //32
    // 一个 unit64_t 表示多少个 L0 entry
    L0_ENTRIES_PER_SLOT = bits_per_slot, // 64
  };
  uint64_t _children_per_slot() const override
  {
    return L1_ENTRIES_PER_SLOT;
  }

  interval_t _get_longest_from_l0(uint64_t pos0, uint64_t pos1,
    uint64_t min_length, interval_t* tail) const;

  inline void _fragment_and_emplace(uint64_t max_length, uint64_t offset,
    uint64_t len,
    interval_vector_t* res)
  {
    auto it = res->rbegin();
    if (max_length) {
      // 获取最后一个元素，判断是否连续，如果连续则直接修改最后一个元素的 length（不超过 max_length）
      if (it != res->rend() && it->offset + it->length == offset) {
	auto l = max_length - it->length;
	if (l >= len) {
	  it->length += len;
	  return;
	} else {
	  offset += l;
	  len -= l;
	  it->length += l;
	}
      }

      // 如果物理长度大于 max_length，则切片
      while (len > max_length) {
	res->emplace_back(offset, max_length);
	offset += max_length;
	len -= max_length;
      }
      res->emplace_back(offset, len);
      return;
    }

    // 如果没有指定 max_length 即不限制单个物理空间的长度
    // 则连续时直接修改最后一个 pe 的长度
    // 不连续时直接将 pe 加入到 vector 中
    if (it != res->rend() && it->offset + it->length == offset) {
      it->length += len;
    } else {
      res->emplace_back(offset, len);
    }
  }

  bool _allocate_l0(uint64_t length,
    uint64_t max_length,
    uint64_t l0_pos0, uint64_t l0_pos1,
    uint64_t* allocated,
    interval_vector_t* res)
  {
    uint64_t d0 = L0_ENTRIES_PER_SLOT;

    ++l0_dives;

    ceph_assert(l0_pos0 < l0_pos1);
    ceph_assert(length > *allocated);
    ceph_assert(0 == (l0_pos0 % (slots_per_slotset * d0)));
    ceph_assert(0 == (l0_pos1 % (slots_per_slotset * d0)));
    ceph_assert(((length - *allocated) % l0_granularity) == 0);

    uint64_t need_entries = (length - *allocated) / l0_granularity;

    // 以 slot 为单位遍历
    for (auto idx = l0_pos0 / d0; (idx < l0_pos1 / d0) && (length > *allocated);
      ++idx) {
      ++l0_iterations;
      slot_t& slot_val = l0[idx];
      auto base = idx * d0;
      if (slot_val == all_slot_clear) { // 如果该 slot 无空闲空间则跳过
        continue;
      } else if (slot_val == all_slot_set) { // 如果该 slot 全部空间则取 需要分配的大小和全部空间之间的较小值进行分配
        uint64_t to_alloc = std::min(need_entries, d0);
        *allocated += to_alloc * l0_granularity;
	++alloc_fragments;
        need_entries -= to_alloc;

	_fragment_and_emplace(max_length, base * l0_granularity,
          to_alloc * l0_granularity, res);

        if (to_alloc == d0) { // 全部已分配则将 slot 标记为全部已分配
          slot_val = all_slot_clear;
        } else { // 否则只标记已分配的位置
          _mark_alloc_l0(base, base + to_alloc);
        }
        continue;
      }

      // 查找第一个空闲的位置
      auto free_pos = find_next_set_bit(slot_val, 0);
      ceph_assert(free_pos < bits_per_slot);
      auto next_pos = free_pos + 1;
      // 循环结束的条件：1.遍历完当前 slot；2.找到足够的空间
      while (next_pos < bits_per_slot &&
        (next_pos - free_pos) < need_entries) {
	++l0_inner_iterations;

        // 按位与 == 0 说明下一个位置已分配，此时处理当前位置到下一个位置之间的空间
	// 然后从下一个位置开始继续查找空闲的位置
        if (0 == (slot_val & (slot_t(1) << next_pos))) {
          auto to_alloc = (next_pos - free_pos);
          *allocated += to_alloc * l0_granularity;
	  ++alloc_fragments;
          need_entries -= to_alloc;
	  _fragment_and_emplace(max_length, (base + free_pos) * l0_granularity,
	    to_alloc * l0_granularity, res);
          _mark_alloc_l0(base + free_pos, base + next_pos);
          free_pos = find_next_set_bit(slot_val, next_pos + 1);
          next_pos = free_pos + 1;
        } else {
          ++next_pos;
        }
      }
      // 走到这里说明从 free_pos 到结束位置全部空闲，但尚未分配
      // 取空间空间和需要空间的较小值，将其分配出来
      if (need_entries && free_pos < bits_per_slot) {
        auto to_alloc = std::min(need_entries, d0 - free_pos);
        *allocated += to_alloc * l0_granularity;
	++alloc_fragments;
	need_entries -= to_alloc;
	_fragment_and_emplace(max_length, (base + free_pos) * l0_granularity,
	  to_alloc * l0_granularity, res);
        _mark_alloc_l0(base + free_pos, base + free_pos + to_alloc);
      }
    }
    return _is_empty_l0(l0_pos0, l0_pos1);
  }

protected:

  friend class AllocatorLevel02<AllocatorLevel01Loose>;

  void _init(uint64_t capacity, uint64_t _alloc_unit, bool mark_as_free = true)
  {
    l0_granularity = _alloc_unit;
    // 512 bits ( 64 bytes ) at L0 mapped to L1 entry
    // 一个 00|01|11 对应的粒度
    l1_granularity = l0_granularity * bits_per_slotset;

    // capacity to have slot alignment at l1
    // l1 的 bit 管理也按照 cpu cache line 对齐
    // aligned_capacity 表示按照 l1 slotset 向上对齐之后表示的容量大小
    auto aligned_capacity =
      p2roundup((int64_t)capacity,
        int64_t(l1_granularity * slots_per_slotset * _children_per_slot()));
    // aligned_capacity / l1_granularity = l1_entrys, 表示需要多少个 l1_entrys
    // _children_per_slot() 表示一个 l1 slot 能表示多少 entry (多少个 l0 slotset)
    // l1_entrys / _children_per_slot() = 多少个 l1 slot 
    size_t slot_count =
      aligned_capacity / l1_granularity / _children_per_slot();
    // we use set bit(s) as a marker for (partially) free entry
    // 根据计算得到的 l1 slot 个数调整 l1 数组大小
    l1.resize(slot_count, mark_as_free ? all_slot_set : all_slot_clear);

    // l0 slot count
    size_t slot_count_l0 = aligned_capacity / _alloc_unit / bits_per_slot;
    // we use set bit(s) as a marker for (partially) free entry
    // 根据计算得到的 l0 slot 个数调整 l0 数组大小
    l0.resize(slot_count_l0, mark_as_free ? all_slot_set : all_slot_clear);

    partial_l1_count = unalloc_l1_count = 0;
    if (mark_as_free) {
      unalloc_l1_count = slot_count * _children_per_slot();
      // 将为了对齐多分配的 bit 标记为已使用
      // p2roundup((int64_t)capacity, (int64_t)l0_granularity) 调整 capacity 的容量，使其按照 l0 对齐
      // 通过对齐之后的容量 / l0 粒度，得出其最后一个 l0 粒度所对应的 bit
      auto l0_pos_no_use = p2roundup((int64_t)capacity, (int64_t)l0_granularity) / l0_granularity;
      // l0_pos_no_use 表示实际容量的所对应的 bit 索引 + 1，即要标记为已使用的起始位置
      // aligned_capacity / l0_granularity 使用对齐的容量 / l0 的粒度，表示最后一个 l0 粒度所对应的 bit 索引
      // 即多分配的 bit 的最后一个
      _mark_alloc_l1_l0(l0_pos_no_use, aligned_capacity / l0_granularity);
    }
  }

  struct search_ctx_t
  {
    size_t partial_count = 0;	// 部分未使用的 l1 entry 个数
    size_t free_count = 0;	// 全部未使用的 l1 entry 个数
    uint64_t free_l1_pos = 0;	// 可使用的 l1 entry 的起始索引

    uint64_t min_affordable_len = 0;	// 可提供的满足最小长度要求的最大区间的长度
    uint64_t min_affordable_offs = 0;	// 该区间的 offset
    uint64_t affordable_len = 0;	// 可提供的满足长度要求的最大区间的长度
    uint64_t affordable_offs = 0;	// 该区间的 offset

    bool fully_processed = false;	// 是否遍历完整个 l1 sloset

    void reset()
    {
      *this = search_ctx_t();
    }
  };
  enum {
    NO_STOP,
    STOP_ON_EMPTY,	// 找到全部未使用的 l1 entry 则停止
    STOP_ON_PARTIAL,
  };
  void _analyze_partials(uint64_t pos_start, uint64_t pos_end,
    uint64_t length, uint64_t min_length, int mode,
    search_ctx_t* ctx);

  void _mark_l1_on_l0(int64_t l0_pos, int64_t l0_pos_end);
  void _mark_alloc_l0(int64_t l0_pos_start, int64_t l0_pos_end);
  uint64_t _claim_free_to_left_l0(int64_t l0_pos_start);
  uint64_t _claim_free_to_right_l0(int64_t l0_pos_start);

  // 将 start ~ end 范围内的 l0 bit 标记为 0，表示已分配
  void _mark_alloc_l1_l0(int64_t l0_pos_start, int64_t l0_pos_end)
  {
    _mark_alloc_l0(l0_pos_start, l0_pos_end);
    // l0_pos_start 表示起始位置所在 slotset 的起始位置 
    l0_pos_start = p2align(l0_pos_start, int64_t(bits_per_slotset));
    // l0_pos_end 表示结尾位置所在 slotset 的结尾位置
    l0_pos_end = p2roundup(l0_pos_end, int64_t(bits_per_slotset));
    _mark_l1_on_l0(l0_pos_start, l0_pos_end);
  }

  // 将 start ~ end 范围内的 l0 bit 设置为 1，即标记为空闲
  void _mark_free_l0(int64_t l0_pos_start, int64_t l0_pos_end)
  {
    auto d0 = L0_ENTRIES_PER_SLOT;

    auto pos = l0_pos_start;
    slot_t bits = (slot_t)1 << (l0_pos_start % d0);
    slot_t* val_s = &l0[pos / d0];
    int64_t pos_e = std::min(l0_pos_end,
                             p2roundup<int64_t>(l0_pos_start + 1, d0));
    while (pos < pos_e) {
      *val_s |=  bits;
      bits <<= 1;
      pos++;
    }
    pos_e = std::min(l0_pos_end, p2align<int64_t>(l0_pos_end, d0));
    while (pos < pos_e) {
      *(++val_s) = all_slot_set;
      pos += d0;
    }
    bits = 1;
    ++val_s;
    while (pos < l0_pos_end) {
      *val_s |= bits;
      bits <<= 1;
      pos++;
    }
  }

  // 将 start ~ end 范围内的 l0 bit 标记为 0，表示空闲
  void _mark_free_l1_l0(int64_t l0_pos_start, int64_t l0_pos_end)
  {
    _mark_free_l0(l0_pos_start, l0_pos_end);
    l0_pos_start = p2align(l0_pos_start, int64_t(bits_per_slotset));
    l0_pos_end = p2roundup(l0_pos_end, int64_t(bits_per_slotset));
    _mark_l1_on_l0(l0_pos_start, l0_pos_end);
  }

  /**
   * 判断 l0 start ~ end 范围内是否有未分配的空间
   * 没有返回 true
   */
  bool _is_empty_l0(uint64_t l0_pos, uint64_t l0_pos_end)
  {
    bool no_free = true;
    // d 表示一个 slotset 表示的字节数
    uint64_t d = slots_per_slotset * L0_ENTRIES_PER_SLOT;
    ceph_assert(0 == (l0_pos % d));
    ceph_assert(0 == (l0_pos_end % d));

    auto idx = l0_pos / L0_ENTRIES_PER_SLOT;
    auto idx_end = l0_pos_end / L0_ENTRIES_PER_SLOT;
    while (idx < idx_end && no_free) {
      no_free = l0[idx] == all_slot_clear;
      ++idx;
    }
    return no_free;
  }
  // 判断 l1 start ~ end 范围内是否已全部分配
  bool _is_empty_l1(uint64_t l1_pos, uint64_t l1_pos_end)
  {
    bool no_free = true;
    uint64_t d = slots_per_slotset * _children_per_slot();
    ceph_assert(0 == (l1_pos % d));
    ceph_assert(0 == (l1_pos_end % d));

    auto idx = l1_pos / L1_ENTRIES_PER_SLOT;
    auto idx_end = l1_pos_end / L1_ENTRIES_PER_SLOT;
    while (idx < idx_end && no_free) {
      no_free = _is_slot_fully_allocated(idx);
      ++idx;
    }
    return no_free;
  }

  interval_t _allocate_l1_contiguous(uint64_t length,
    uint64_t min_length, uint64_t max_length,
    uint64_t pos_start, uint64_t pos_end);

  bool _allocate_l1(uint64_t length,
    uint64_t min_length, uint64_t max_length,
    uint64_t l1_pos_start, uint64_t l1_pos_end,
    uint64_t* allocated,
    interval_vector_t* res);

  // 根据 offset 和 length 标记 l0 l1 相关位置为已分配
  uint64_t _mark_alloc_l1(uint64_t offset, uint64_t length)
  {
    uint64_t l0_pos_start = offset / l0_granularity;
    uint64_t l0_pos_end = p2roundup(offset + length, l0_granularity) / l0_granularity;
    _mark_alloc_l1_l0(l0_pos_start, l0_pos_end);
    return l0_granularity * (l0_pos_end - l0_pos_start);
  }

  // 根据 offset 和 length 标记 l0 l1 相关位置为空闲
  uint64_t _free_l1(uint64_t offs, uint64_t len)
  {
    uint64_t l0_pos_start = offs / l0_granularity;
    uint64_t l0_pos_end = p2roundup(offs + len, l0_granularity) / l0_granularity;
    _mark_free_l1_l0(l0_pos_start, l0_pos_end);
    return l0_granularity * (l0_pos_end - l0_pos_start);
  }

  // 分配 offs 左侧所有连续的空闲空间
  uint64_t claim_free_to_left_l1(uint64_t offs)
  {
    uint64_t l0_pos_end = offs / l0_granularity;
    uint64_t l0_pos_start = _claim_free_to_left_l0(l0_pos_end);
    // l0_pos_start 小于 l0_pos_end 说明在 offs 左侧找到了连续空闲的位置
    // 获取过程中已经标记了 l0，这里再根据起始和结束位置标记 l1，并返回分配的空间大小
    if (l0_pos_start < l0_pos_end) {
      _mark_l1_on_l0(
        p2align(l0_pos_start, uint64_t(bits_per_slotset)),
        p2roundup(l0_pos_end, uint64_t(bits_per_slotset)));
      return l0_granularity * (l0_pos_end - l0_pos_start);
    }
    // 走到这里说明在 offs 左侧找到连续空间，返回 0 表示未分配
    return 0;
  }

  // 分配 offs 右侧所有连续的空闲空间
  uint64_t claim_free_to_right_l1(uint64_t offs)
  {
    uint64_t l0_pos_start = offs / l0_granularity;
    uint64_t l0_pos_end = _claim_free_to_right_l0(l0_pos_start);

    if (l0_pos_start < l0_pos_end) {
      _mark_l1_on_l0(
        p2align(l0_pos_start, uint64_t(bits_per_slotset)),
        p2roundup(l0_pos_end, uint64_t(bits_per_slotset)));
      return l0_granularity * (l0_pos_end - l0_pos_start);
    }
    return 0;
  }


public:
  uint64_t debug_get_allocated(uint64_t pos0 = 0, uint64_t pos1 = 0)
  {
    if (pos1 == 0) {
      pos1 = l1.size() * L1_ENTRIES_PER_SLOT;
    }
    auto avail = debug_get_free(pos0, pos1);
    return (pos1 - pos0) * l1_granularity - avail;
  }

  uint64_t debug_get_free(uint64_t l1_pos0 = 0, uint64_t l1_pos1 = 0)
  {
    ceph_assert(0 == (l1_pos0 % L1_ENTRIES_PER_SLOT));
    ceph_assert(0 == (l1_pos1 % L1_ENTRIES_PER_SLOT));

    auto idx0 = l1_pos0 * slots_per_slotset;
    auto idx1 = l1_pos1 * slots_per_slotset;

    if (idx1 == 0) {
      idx1 = l0.size();
    }

    uint64_t res = 0;
    for (uint64_t i = idx0; i < idx1; ++i) {
      auto v = l0[i];
      if (v == all_slot_set) {
        res += L0_ENTRIES_PER_SLOT;
      } else if (v != all_slot_clear) {
        size_t cnt = 0;
#ifdef __GNUC__
        cnt = __builtin_popcountll(v);
#else
        // Kernighan's Alg to count set bits
        while (v) {
          v &= (v - 1);
          cnt++;
        }
#endif
        res += cnt;
      }
    }
    return res * l0_granularity;
  }
  void collect_stats(
    std::map<size_t, size_t>& bins_overall) override;

  static inline ssize_t count_0s(slot_t slot_val, size_t start_pos);
  static inline ssize_t count_1s(slot_t slot_val, size_t start_pos);
  void foreach_internal(std::function<void(uint64_t offset, uint64_t length)> notify);
};


class AllocatorLevel01Compact : public AllocatorLevel01
{
  uint64_t _children_per_slot() const override
  {
    return 8;
  }
public:
  void collect_stats(
    std::map<size_t, size_t>& bins_overall) override
  {
    // not implemented
  }
};

template <class L1>
class AllocatorLevel02 : public AllocatorLevel
{
public:
  uint64_t debug_get_free(uint64_t pos0 = 0, uint64_t pos1 = 0)
  {
    std::lock_guard l(lock);
    return l1.debug_get_free(pos0 * l1._children_per_slot() * bits_per_slot,
      pos1 * l1._children_per_slot() * bits_per_slot);
  }
  uint64_t debug_get_allocated(uint64_t pos0 = 0, uint64_t pos1 = 0)
  {
    std::lock_guard l(lock);
    return l1.debug_get_allocated(pos0 * l1._children_per_slot() * bits_per_slot,
      pos1 * l1._children_per_slot() * bits_per_slot);
  }

  uint64_t get_available()
  {
    std::lock_guard l(lock);
    return available;
  }
  inline uint64_t get_min_alloc_size() const
  {
    return l1.get_min_alloc_size();
  }
  void collect_stats(
    std::map<size_t, size_t>& bins_overall) override {

      std::lock_guard l(lock);
      l1.collect_stats(bins_overall);
  }

  // 分配 offset 左侧所有连续空间
  uint64_t claim_free_to_left(uint64_t offset) {
    std::lock_guard l(lock);
    auto allocated = l1.claim_free_to_left_l1(offset);
    ceph_assert(available >= allocated);
    available -= allocated;

    uint64_t l2_pos = (offset - allocated) / l2_granularity;
    uint64_t l2_pos_end =
      p2roundup(int64_t(offset), int64_t(l2_granularity)) / l2_granularity;
    _mark_l2_on_l1(l2_pos, l2_pos_end);
    return allocated;
  }

  // 分配 offset 右侧所有连续空间
  uint64_t claim_free_to_right(uint64_t offset) {
    std::lock_guard l(lock);
    auto allocated = l1.claim_free_to_right_l1(offset);
    ceph_assert(available >= allocated);
    available -= allocated;

    uint64_t l2_pos = (offset) / l2_granularity;
    int64_t end = offset + allocated;
    uint64_t l2_pos_end = p2roundup(end, int64_t(l2_granularity)) / l2_granularity;
    _mark_l2_on_l1(l2_pos, l2_pos_end);
    return allocated;
  }

  void foreach_internal(
    std::function<void(uint64_t offset, uint64_t length)> notify)
  {
    size_t alloc_size = get_min_alloc_size();
    auto multiply_by_alloc_size = [alloc_size, notify](size_t off, size_t len) {
      notify(off * alloc_size, len * alloc_size);
    };
    std::lock_guard l(lock);
    l1.foreach_internal(multiply_by_alloc_size);
  }
  double get_fragmentation_internal() {
    std::lock_guard l(lock);
    return l1.get_fragmentation();
  }

protected:
  ceph::mutex lock = ceph::make_mutex("AllocatorLevel02::lock");
  L1 l1;
  slot_vector_t l2;
  /**
   * 一个 bit 代表的磁盘空间大小，对应一个 l1 级别的 slotset 能表示的磁盘空间大小
   * = l1 的粒度 * 一个 slot 有多少个 l1 entry * 一个 slotset 有多少个 slot
   */
  uint64_t l2_granularity = 0; // space per entry
  uint64_t available = 0;
  uint64_t last_pos = 0;

  enum {
    L1_ENTRIES_PER_SLOT = bits_per_slot, // 64
  };

  uint64_t _children_per_slot() const override
  {
    return L1_ENTRIES_PER_SLOT;
  }
  uint64_t _level_granularity() const override
  {
    return l2_granularity;
  }

  void _init(uint64_t capacity, uint64_t _alloc_unit, bool mark_as_free = true)
  {
    ceph_assert(isp2(_alloc_unit));
    // 初始化 l1
    l1._init(capacity, _alloc_unit, mark_as_free);

    l2_granularity =
      l1._level_granularity() * l1._children_per_slot() * slots_per_slotset;

    // capacity to have slot alignment at l2
    // 实际容量按照 l2 粒度 及 slotset 向上取整
    auto aligned_capacity =
      p2roundup((int64_t)capacity, (int64_t)l2_granularity * L1_ENTRIES_PER_SLOT);
    // 需要多少个 uint64_t 来表示
    size_t elem_count = aligned_capacity / l2_granularity / L1_ENTRIES_PER_SLOT;
    // we use set bit(s) as a marker for (partially) free entry
    // 设置数组大小
    l2.resize(elem_count, mark_as_free ? all_slot_set : all_slot_clear);

    if (mark_as_free) {
      // capacity to have slotset alignment at l1
      // 将超分配部分标记为已使用
      auto l2_pos_no_use =
	p2roundup((int64_t)capacity, (int64_t)l2_granularity) / l2_granularity;
      _mark_l2_allocated(l2_pos_no_use, aligned_capacity / l2_granularity);
      available = p2align(capacity, _alloc_unit);
    } else {
      available = 0;
    }
  }

  // 将 pos ~ pos_end 索引范围内的 l2 标记为已使用
  void _mark_l2_allocated(int64_t l2_pos, int64_t l2_pos_end)
  {
    auto d = L1_ENTRIES_PER_SLOT;
    ceph_assert(0 <= l2_pos_end);
    ceph_assert((int64_t)l2.size() >= (l2_pos_end / d));

    while (l2_pos < l2_pos_end) {
      l2[l2_pos / d] &= ~(slot_t(1) << (l2_pos % d));
      ++l2_pos;
    }
  }

  // 将 pos ~ pos_end 索引范围内的 l2 标记为空闲
  void _mark_l2_free(int64_t l2_pos, int64_t l2_pos_end)
  {
    auto d = L1_ENTRIES_PER_SLOT;
    ceph_assert(0 <= l2_pos_end);
    ceph_assert((int64_t)l2.size() >= (l2_pos_end / d));

    while (l2_pos < l2_pos_end) {
        l2[l2_pos / d] |= (slot_t(1) << (l2_pos % d));
        ++l2_pos;
    }
  }

  // 根据 l1 的状态标记 l2
  // 只有一个 slotset 的 l1 全部被分配会被标记为 0
  // 否则标记为 1
  void _mark_l2_on_l1(int64_t l2_pos, int64_t l2_pos_end)
  {
    auto d = L1_ENTRIES_PER_SLOT;
    ceph_assert(0 <= l2_pos_end);
    ceph_assert((int64_t)l2.size() >= (l2_pos_end / d));

    auto idx = l2_pos * slots_per_slotset;
    auto idx_end = l2_pos_end * slots_per_slotset;
    bool all_allocated = true;
    while (idx < idx_end) {
      if (!l1._is_slot_fully_allocated(idx)) {
        all_allocated = false;
        idx = p2roundup(int64_t(++idx), int64_t(slots_per_slotset));
      }
      else {
        ++idx;
      }
      if ((idx % slots_per_slotset) == 0) {
        if (all_allocated) {
          l2[l2_pos / d] &= ~(slot_t(1) << (l2_pos % d));
        }
        else {
          l2[l2_pos / d] |= (slot_t(1) << (l2_pos % d));
        }
        all_allocated = true;
        ++l2_pos;
      }
    }
  }

  void _allocate_l2(uint64_t length,
    uint64_t min_length,
    uint64_t max_length,
    uint64_t hint,
    
    uint64_t* allocated,
    interval_vector_t* res)
  {
    uint64_t prev_allocated = *allocated;
    uint64_t d = L1_ENTRIES_PER_SLOT;
    ceph_assert(min_length <= l2_granularity);
    ceph_assert(max_length == 0 || max_length >= min_length);
    ceph_assert(max_length == 0 || (max_length % min_length) == 0);
    ceph_assert(length >= min_length);
    ceph_assert((length % min_length) == 0);

    // 最大分配 2G
    uint64_t cap = 1ull << 31;
    if (max_length == 0 || max_length >= cap) {
      max_length = cap;
    }

    // 一个 slotset 有多少个 l1 entry
    uint64_t l1_w = slots_per_slotset * l1._children_per_slot();

    std::lock_guard l(lock);

    // 可用空间小于最小分配单位，则直接退出
    if (available < min_length) {
      return;
    }
    if (hint != 0) {
      // 提示位置对应的 bit 所在 slot 的起始 bit
      last_pos = (hint / (d * l2_granularity)) < l2.size() ? p2align(hint / l2_granularity, d) : 0;
    }
    auto l2_pos = last_pos;
    auto last_pos0 = last_pos;
    auto pos = last_pos / d;	// 起始位置所在 slot 的索引
    auto pos_end = l2.size();
    // outer loop below is intended to optimize the performance by
    // avoiding 'modulo' operations inside the internal loop.
    // Looks like they have negative impact on the performance
    // 两次循环，第一次从 hint 位置开始分配到磁盘末尾，第二次从磁盘开头开始分配提示位置
    for (auto i = 0; i < 2; ++i) {
      // length > *allocated 表示未分配够
      // pos < pos_end 表示未遍历到设定的结束位置
      // 以 slot 为单位递增
      for(; length > *allocated && pos < pos_end; ++pos) {
	// 得到起始位置所在 l2 slot 的值，用于判断其所对应的一个 slotset 的 l1 是否空闲
	slot_t& slot_val = l2[pos];
	size_t free_pos = 0;
	bool all_set = false;
	if (slot_val == all_slot_clear) { // 对应的位置已全部分配
	  l2_pos += d;
	  last_pos = l2_pos;
	  continue;
	} else if (slot_val == all_slot_set) { // 对应的位置全部空闲
	  free_pos = 0;
	  all_set = true;
	} else { // 部分使用，超找第一个等于 1 的位置（即表示空闲的位置）
	  free_pos = find_next_set_bit(slot_val, 0);
	  ceph_assert(free_pos < bits_per_slot);
	}
	do {
	  ceph_assert(length > *allocated);
	  // 调用 l1 的接口分配空间
	  // l1 起始和结束位置只跨越一个 l2 bit，即一个 l1 slotset
	  bool empty = l1._allocate_l1(length,
	    min_length,
	    max_length,
	    (l2_pos + free_pos) * l1_w,
	    (l2_pos + free_pos + 1) * l1_w,
	    allocated,
	    res);
	  if (empty) {
	    slot_val &= ~(slot_t(1) << free_pos);
	  }
	  // 如果已分配完或者当前 slot 已全部遍历完，则退出
	  // 已分配完会再次退出外层循环，已遍历完则继续下一个 slot
	  if (length <= *allocated || slot_val == all_slot_clear) {
	    break;
	  }
	  ++free_pos;
	  // 如果是部分空闲则查找下一个空闲的 bit
	  if (!all_set) {
	    free_pos = find_next_set_bit(slot_val, free_pos);
	  }
	} while (free_pos < bits_per_slot);
	last_pos = l2_pos;
	l2_pos += d;
      }
      l2_pos = 0;
      pos = 0;
      pos_end = last_pos0 / d;
    }

    ++l2_allocs;
    auto allocated_here = *allocated - prev_allocated;
    ceph_assert(available >= allocated_here);
    available -= allocated_here;
  }

#ifndef NON_CEPH_BUILD
  // to provide compatibility with BlueStore's allocator interface
  void _free_l2(const interval_set<uint64_t> & rr)
  {
    uint64_t released = 0;
    std::lock_guard l(lock);
    for (auto r : rr) {
      released += l1._free_l1(r.first, r.second);
      uint64_t l2_pos = r.first / l2_granularity;
      uint64_t l2_pos_end = p2roundup(int64_t(r.first + r.second), int64_t(l2_granularity)) / l2_granularity;

      _mark_l2_free(l2_pos, l2_pos_end);
    }
    available += released;
  }
#endif

  template <typename T>
  void _free_l2(const T& rr)
  {
    uint64_t released = 0;
    std::lock_guard l(lock);
    for (auto r : rr) {
      released += l1._free_l1(r.offset, r.length);
      uint64_t l2_pos = r.offset / l2_granularity;
      uint64_t l2_pos_end = p2roundup(int64_t(r.offset + r.length), int64_t(l2_granularity)) / l2_granularity;

      _mark_l2_free(l2_pos, l2_pos_end);
    }
    available += released;
  }

  void _mark_allocated(uint64_t o, uint64_t len)
  {
    uint64_t l2_pos = o / l2_granularity;
    uint64_t l2_pos_end = p2roundup(int64_t(o + len), int64_t(l2_granularity)) / l2_granularity;

    std::lock_guard l(lock);
    auto allocated = l1._mark_alloc_l1(o, len);
    ceph_assert(available >= allocated);
    available -= allocated;
    _mark_l2_on_l1(l2_pos, l2_pos_end);
  }

  void _mark_free(uint64_t o, uint64_t len)
  {
    // 算出起始位置所在 l2 粒度的索引
    uint64_t l2_pos = o / l2_granularity;
    // 算出结束位置所在 l2 粒度的索引
    uint64_t l2_pos_end = p2roundup(int64_t(o + len), int64_t(l2_granularity)) / l2_granularity;

    std::lock_guard l(lock);
    // 标记 l1 l0 相对应位置为 free 并累加空闲空间
    available += l1._free_l1(o, len);
    // 标记 l2
    _mark_l2_free(l2_pos, l2_pos_end);
  }
  void _shutdown()
  {
    last_pos = 0;
  }
};

#endif
