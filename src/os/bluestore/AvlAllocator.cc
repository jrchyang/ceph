// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "AvlAllocator.h"

#include <limits>

#include "common/config_proxy.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef  dout_prefix
#define dout_prefix *_dout << "AvlAllocator "

MEMPOOL_DEFINE_OBJECT_FACTORY(range_seg_t, range_seg_t, bluestore_alloc);

namespace {
  // a light-weight "range_seg_t", which only used as the key when searching in
  // range_tree and range_size_tree
  struct range_t {
    uint64_t start;
    uint64_t end;
  };
}

/*
 * This is a helper function that can be used by the allocator to find
 * a suitable block to allocate. This will search the specified AVL
 * tree looking for a block that matches the specified criteria.
 */
uint64_t AvlAllocator::_pick_block_after(uint64_t *cursor,
					 uint64_t size,
					 uint64_t align)
{
  const auto compare = range_tree.key_comp();
  uint32_t search_count = 0;
  uint64_t search_bytes = 0;
  // 找到第一个 起始位置 不小于 size 的节点
  // 起始位置小于 size 要求的那么肯定长度不够，相当于跳过这一部分
  auto rs_start = range_tree.lower_bound(range_t{*cursor, size}, compare);
  for (auto rs = rs_start; rs != range_tree.end(); ++rs) {
    uint64_t offset = p2roundup(rs->start, align);
    *cursor = offset + size;
    // 如果找到的节点大小满足分配要求，则直接返回该节点
    if (offset + size <= rs->end) {
      return offset;
    }
    // 搜索次数达到上限，退出后采用最佳拟合
    if (max_search_count > 0 && ++search_count > max_search_count) {
      return -1ULL;
    }
    // 搜索容量达到上限，退出后采用最佳拟合
    if (search_bytes = rs->start - rs_start->start;
	max_search_bytes > 0 && search_bytes > max_search_bytes) {
      return -1ULL;
    }
  }

  // 说明没有找到，直接退出采用最佳拟合
  if (*cursor == 0) {
    // If we already started from beginning, don't bother with searching from beginning
    return -1ULL;
  }

  // If we reached end, start from beginning till cursor.
  // 虽然找到了，但是节点长度不符合要求，从起始位置继续找
  for (auto rs = range_tree.begin(); rs != rs_start; ++rs) {
    uint64_t offset = p2roundup(rs->start, align);
    *cursor = offset + size;
    // 长度满足要求直接返回
    if (offset + size <= rs->end) {
      return offset;
    }
    if (max_search_count > 0 && ++search_count > max_search_count) {
      return -1ULL;
    }
    if (max_search_bytes > 0 && search_bytes + rs->start > max_search_bytes) {
      return -1ULL;
    }
  }
  return -1ULL;
}

uint64_t AvlAllocator::_pick_block_fits(uint64_t size,
					uint64_t align)
{
  // instead of searching from cursor, just pick the smallest range which fits
  // the needs
  const auto compare = range_size_tree.key_comp();
  // 找到长度不小于 size 的节点
  auto rs_start = range_size_tree.lower_bound(range_t{0, size}, compare);
  for (auto rs = rs_start; rs != range_size_tree.end(); ++rs) {
    uint64_t offset = p2roundup(rs->start, align);
    // 节点大小满足要求则直接返回
    if (offset + size <= rs->end) {
      return offset;
    }
  }
  return -1ULL;
}

void AvlAllocator::_add_to_tree(uint64_t start, uint64_t size)
{
  ceph_assert(size != 0);

  // seg 使用 offset~end 来表示
  uint64_t end = start + size;

  // upper_bound 查找第一个大于 start 大于要插入元素 end 的节点
  auto rs_after = range_tree.upper_bound(range_t{start, end},
					 range_tree.key_comp());

  /* Make sure we don't overlap with either of our neighbors */
  auto rs_before = range_tree.end();
  // 如果不是第一个节点，那么还需要找到其前一个节点
  if (rs_after != range_tree.begin()) {
    rs_before = std::prev(rs_after);
  }

  // 判断是否可以进行合并
  // 如果有前一个节点，那么判断前一个节点的结束位置是否等于新节点的起始位置，如果相等，则可以与前一个元素合并
  // 如果有后一个节点，那么判断后一个节点的起始位置是否等于新节点的结束位置，如果相等，则可以与后一个元素合并
  bool merge_before = (rs_before != range_tree.end() && rs_before->end == start);
  bool merge_after = (rs_after != range_tree.end() && rs_after->start == end);

  if (merge_before && merge_after) { // 前 + 新 + 后 合并到一起
    // 先将两个节点从 size tree 中删除
    _range_size_tree_rm(*rs_before);
    _range_size_tree_rm(*rs_after);
    // 让后一个节点的 start 等于前一个节点的 start
    rs_after->start = rs_before->start;
    // 前边调整了 after 节点，这里直接删除 before 节点
    range_tree.erase_and_dispose(rs_before, dispose_rs{});
    // offset 树已经完成处理，这里只需要处理 size 树
    // 将调整后的 after 节点尝试插入到 size tree 中，如果不能插入则添加到派生类指定的分配器中
    _range_size_tree_try_insert(*rs_after);
  } else if (merge_before) { // 前 + 新 合并到一起
    // 将 before 节点从 size 树中删除
    _range_size_tree_rm(*rs_before);
    // 调整 before 节点的结束位置
    rs_before->end = end;
    // offset 树已经完成处理，这里只需要处理 size 树
    // 将调整后的 before 节点尝试插入到 size tree 中，如果不能插入则添加到派生类指定的分配器中
    _range_size_tree_try_insert(*rs_before);
  } else if (merge_after) { // 新 + 后 合并到一起
    // 将 after 节点从 size 树中删除
    _range_size_tree_rm(*rs_after);
    // 调整 after 节点的起始位置
    rs_after->start = start;
    // offset 树已经完成处理，这里只需要处理 size 树
    // 将调整后的 after 节点尝试插入到 size tree 中，如果不能插入则添加到派生类指定的分配器中
    _range_size_tree_try_insert(*rs_after);
  } else { // 都不能合并，直接尝试插入
    // 插入成功则成功，失败则插入到派生类指定的分配器中
    // 这里指定插入位置，after 之前
    _try_insert_range(start, end, &rs_after);
  }
}

void AvlAllocator::_process_range_removal(uint64_t start, uint64_t end,
  AvlAllocator::range_tree_t::iterator& rs)
{
  bool left_over = (rs->start != start);	// 左边是否有剩余
  bool right_over = (rs->end != end);		// 右边是否有剩余

  // 不管有没有剩余，节点的 size 肯定变化，先将其从 size 树中删除
  _range_size_tree_rm(*rs);

  if (left_over && right_over) { // 左右都有剩余
    auto old_right_end = rs->end;
    auto insert_pos = rs;
    ceph_assert(insert_pos != range_tree.end());
    // 新节点插入的位置，即插入到下一个节点的前边
    ++insert_pos;
    // 调整原来的节点，这样只插入一个新的就行
    rs->end = start;

    // Insert tail first to be sure insert_pos hasn't been disposed.
    // This woulnd't dispose rs though since it's out of range_size_tree.
    // Don't care about a small chance of 'not-the-best-choice-for-removal' case
    // which might happen if rs has the lowest size.
    // end ~ old_ned 组成一个新的节点插入
    _try_insert_range(end, old_right_end, &insert_pos);
    // 插入调整长度后的旧节点
    _range_size_tree_try_insert(*rs);

  } else if (left_over) { // 仅左边有剩余
    // 调整原节点的结束位置
    rs->end = start;
    // 插入调整后的节点到 size 树中
    _range_size_tree_try_insert(*rs);
  } else if (right_over) { // 右侧有剩余
    // 调整原节点的起始位置
    rs->start = end;
    // 插入调整后的节点到 size 树中
    _range_size_tree_try_insert(*rs);
  } else { // 完整覆盖指定节点
    // 直接将其从 offset 树中删除（函数一开始已经处理了 size 树）
    range_tree.erase_and_dispose(rs, dispose_rs{});
  }
}

void AvlAllocator::_remove_from_tree(uint64_t start, uint64_t size)
{
  uint64_t end = start + size;

  ceph_assert(size != 0);
  ceph_assert(size <= num_free);

  auto rs = range_tree.find(range_t{start, end}, range_tree.key_comp());
  /* Make sure we completely overlap with someone */
  // 没找到异常退出
  ceph_assert(rs != range_tree.end());
  // 找到的节点不能前后包含要删除的部分则异常退出
  ceph_assert(rs->start <= start);
  ceph_assert(rs->end >= end);

  // 将指定的区间从找到的节点中移除
  _process_range_removal(start, end, rs);
}

void AvlAllocator::_try_remove_from_tree(uint64_t start, uint64_t size,
  std::function<void(uint64_t, uint64_t, bool)> cb)
{
  uint64_t end = start + size;

  ceph_assert(size != 0);

  auto rs = range_tree.find(range_t{ start, end },
    range_tree.key_comp());

  if (rs == range_tree.end() || rs->start >= end) {
    cb(start, size, false);
    return;
  }

  do {

    auto next_rs = rs;
    ++next_rs;

    if (start < rs->start) {
      cb(start, rs->start - start, false);
      start = rs->start;
    }
    auto range_end = std::min(rs->end, end);
    _process_range_removal(start, range_end, rs);
    cb(start, range_end - start, true);
    start = range_end;

    rs = next_rs;
  } while (rs != range_tree.end() && rs->start < end && start < end);
  if (start < end) {
    cb(start, end - start, false);
  }
}

/**
 * want - 需要分配的空间大小
 * unit - 最小分配单元
 * max_alloc_size - 最大分配大小
 * hint - 用于提示分配的起始位置，当前未使用
 * extents - 用于保存分配的物理空间
 */
int64_t AvlAllocator::_allocate(
  uint64_t want,
  uint64_t unit,
  uint64_t max_alloc_size,
  int64_t  hint, // unused, for now!
  PExtentVector* extents)
{
  uint64_t allocated = 0;
  // 循环直到分配完
  while (allocated < want) {
    uint64_t offset, length;
    int r = _allocate(std::min(max_alloc_size, want - allocated),
      unit, &offset, &length);
    if (r < 0) {
      // Allocation failed.
      break;
    }
    // 将分配的空间插入到数组中
    extents->emplace_back(offset, length);
    allocated += length;
  }
  return allocated ? allocated : -ENOSPC;
}

/**
 * size : 要分配的大小
 * unit : 最小分配单元
 * offset : 保存分配的磁盘偏移
 * length : 保存分配的长度
 */
int AvlAllocator::_allocate(
  uint64_t size,
  uint64_t unit,
  uint64_t *offset,
  uint64_t *length)
{
  // p 指向 range_size_tree 的最后一个元素
  // max_size 表示最大连续空间
  uint64_t max_size = 0;
  if (auto p = range_size_tree.rbegin(); p != range_size_tree.rend()) {
    max_size = p->end - p->start;
  }

  // 如果最大连续空间小于需要的空间则强制使用 best-fit 策略分配
  bool force_range_size_alloc = false;
  if (max_size < size) {
    if (max_size < unit) {
      return -ENOSPC;
    }
    size = p2align(max_size, unit);
    ceph_assert(size > 0);
    force_range_size_alloc = true;
  }

  const int free_pct = num_free * 100 / device_size;  // 计算空闲率
  uint64_t start = 0;
  // If we're running low on space, find a range by size by looking up in the size
  // sorted tree (best-fit), instead of searching in the area pointed by cursor
  // 满足一下条件时采用最佳拟合分配策略
  //   1. 最大连续空间小于要求分配的空间
  //   2. 最大连续空间小于设置的阈值
  //   3. 剩余空间百分比小于设置的阈值
  if (force_range_size_alloc ||
      max_size < range_size_alloc_threshold ||
      free_pct < range_size_alloc_free_pct) {
    start = -1ULL;
  } else {
    /*
     * Find the largest power of 2 block size that evenly divides the
     * requested size. This is used to try to allocate blocks with similar
     * alignment from the same area (i.e. same cursor bucket) but it does
     * not guarantee that other allocations sizes may exist in the same
     * region.
     */
    // 未触发最佳拟合分配策略，采用首次拟合
    // 
    uint64_t align = size & -size;
    ceph_assert(align != 0);
    // cbits(align) 计算出除去前导零之后剩下的有多少位
    // lbas 初始化为 0，所以第一次 cursor 的值也为 0
    uint64_t* cursor = &lbas[cbits(align) - 1];
    start = _pick_block_after(cursor, size, unit);
    dout(20) << __func__ << " first fit=" << start << " size=" << size << dendl;
  }
  if (start == -1ULL) { // 首次拟合失败，采用最佳拟合
    do {
      start = _pick_block_fits(size, unit);
      dout(20) << __func__ << " best fit=" << start << " size=" << size << dendl;
      if (start != uint64_t(-1ULL)) { // 找到则退出循环
        break;
      }
      // try to collect smaller extents as we could fail to retrieve
      // that large block due to misaligned extents
      // 最佳拟合失败，则减少 size 再次尝试（使用较小空间来分配）
      // 一个大块没准可以用两个小块来分配
      size = p2align(size >> 1, unit);
    } while (size >= unit);
  }
  // 没找到返回空间不足的错误
  if (start == -1ULL) {
    return -ENOSPC;
  }

  _remove_from_tree(start, size);

  *offset = start;
  *length = size;
  return 0;
}

void AvlAllocator::_release(const interval_set<uint64_t>& release_set)
{
  for (auto p = release_set.begin(); p != release_set.end(); ++p) {
    const auto offset = p.get_start();
    const auto length = p.get_len();
    ceph_assert(offset + length <= uint64_t(device_size));
    ldout(cct, 10) << __func__ << std::hex
      << " offset 0x" << offset
      << " length 0x" << length
      << std::dec << dendl;
    _add_to_tree(offset, length);
  }
}

void AvlAllocator::_release(const PExtentVector& release_set) {
  for (auto& e : release_set) {
    ldout(cct, 10) << __func__ << std::hex
      << " offset 0x" << e.offset
      << " length 0x" << e.length
      << std::dec << dendl;
    _add_to_tree(e.offset, e.length);
  }
}

void AvlAllocator::_shutdown()
{
  range_size_tree.clear();
  range_tree.clear_and_dispose(dispose_rs{});
}

AvlAllocator::AvlAllocator(CephContext* cct,
                           int64_t device_size,
                           int64_t block_size,
                           uint64_t max_mem,
                           std::string_view name) :
  Allocator(name, device_size, block_size), // 成员变量赋值，无其他特殊操作
  range_size_alloc_threshold(
    cct->_conf.get_val<uint64_t>("bluestore_avl_alloc_bf_threshold")), // 默认值 128K
  range_size_alloc_free_pct(
    cct->_conf.get_val<uint64_t>("bluestore_avl_alloc_bf_free_pct")), // 默认值 4
  max_search_count(
    cct->_conf.get_val<uint64_t>("bluestore_avl_alloc_ff_max_search_count")), // 默认值 100
  max_search_bytes(
    cct->_conf.get_val<Option::size_t>("bluestore_avl_alloc_ff_max_search_bytes")), // 默认值 16M
  range_count_cap(max_mem / sizeof(range_seg_t)), // max_mem 默认值 64M
  cct(cct)
{}

AvlAllocator::AvlAllocator(CephContext* cct,
			   int64_t device_size,
			   int64_t block_size,
			   std::string_view name) :
  AvlAllocator(cct, device_size, block_size, 0 /* max_mem */, name)
{}

AvlAllocator::~AvlAllocator()
{
  shutdown();
}

// 调整 max_alloc_size 并调用 _allocate 进行实际分配
int64_t AvlAllocator::allocate(
  uint64_t want,
  uint64_t unit,
  uint64_t max_alloc_size,
  int64_t  hint, // unused, for now!
  PExtentVector* extents)
{
  ldout(cct, 10) << __func__ << std::hex
                 << " want 0x" << want
                 << " unit 0x" << unit
                 << " max_alloc_size 0x" << max_alloc_size
                 << " hint 0x" << hint
                 << std::dec << dendl;
  ceph_assert(isp2(unit));
  ceph_assert(want % unit == 0);

  if (max_alloc_size == 0) {
    max_alloc_size = want;
  }
  if (constexpr auto cap = std::numeric_limits<decltype(bluestore_pextent_t::length)>::max(); // 获取类型的最大值
      max_alloc_size >= cap) {
    // 如果 max 超过 pe 长度变量能表示的最大值则调整 max
    max_alloc_size = p2align(uint64_t(cap), (uint64_t)block_size);
  }
  std::lock_guard l(lock);
  return _allocate(want, unit, max_alloc_size, hint, extents);
}

void AvlAllocator::release(const interval_set<uint64_t>& release_set) {
  std::lock_guard l(lock);
  _release(release_set);
}

uint64_t AvlAllocator::get_free()
{
  std::lock_guard l(lock);
  return num_free;
}

uint64_t AvlAllocator::get_size()
{
  std::lock_guard l(lock);
  return range_size_tree.size();
}

double AvlAllocator::get_fragmentation()
{
  std::lock_guard l(lock);
  return _get_fragmentation();
}

void AvlAllocator::dump()
{
  std::lock_guard l(lock);
  _dump();
}

void AvlAllocator::_dump() const
{
  ldout(cct, 0) << __func__ << " range_tree: " << dendl;
  for (auto& rs : range_tree) {
    ldout(cct, 0) << std::hex
      << "0x" << rs.start << "~" << rs.end
      << std::dec
      << dendl;
  }
  ldout(cct, 0) << __func__ << " range_size_tree: " << dendl;
  for (auto& rs : range_size_tree) {
    ldout(cct, 0) << std::hex
      << "0x" << rs.start << "~" << rs.end
      << std::dec
      << dendl;
  }
}

// 遍历节点并执行一些操作
void AvlAllocator::foreach(
  std::function<void(uint64_t offset, uint64_t length)> notify)
{
  std::lock_guard l(lock);
  _foreach(notify);
}

void AvlAllocator::_foreach(
  std::function<void(uint64_t offset, uint64_t length)> notify) const
{
  for (auto& rs : range_tree) {
    notify(rs.start, rs.end - rs.start);
  }
}

// 将 offset~length 区间的空间加入到 avl 树中
void AvlAllocator::init_add_free(uint64_t offset, uint64_t length)
{
  ldout(cct, 10) << __func__ << std::hex
                 << " offset 0x" << offset
                 << " length 0x" << length
                 << std::dec << dendl;
  if (!length)
    return;
  std::lock_guard l(lock);
  ceph_assert(offset + length <= uint64_t(device_size));
  _add_to_tree(offset, length);
}

// 将 offset~length 区间的空间从 avl 树中删除
void AvlAllocator::init_rm_free(uint64_t offset, uint64_t length)
{
  ldout(cct, 10) << __func__ << std::hex
                 << " offset 0x" << offset
                 << " length 0x" << length
                 << std::dec << dendl;
  if (!length)
    return;
  std::lock_guard l(lock);
  ceph_assert(offset + length <= uint64_t(device_size));
  _remove_from_tree(offset, length);
}

void AvlAllocator::shutdown()
{
  std::lock_guard l(lock);
  _shutdown();
}
