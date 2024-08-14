// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Bitmap based in-memory allocator implementation.
 * Author: Igor Fedotov, ifedotov@suse.com
 *
 */

#include "fastbmap_allocator_impl.h"

uint64_t AllocatorLevel::l0_dives = 0;
uint64_t AllocatorLevel::l0_iterations = 0;
uint64_t AllocatorLevel::l0_inner_iterations = 0;
uint64_t AllocatorLevel::alloc_fragments = 0;
uint64_t AllocatorLevel::alloc_fragments_fast = 0;
uint64_t AllocatorLevel::l2_allocs = 0;

inline interval_t _align2units(uint64_t offset, uint64_t len, uint64_t min_length)
{
  interval_t res;
  if (len >= min_length) {
    res.offset = p2roundup(offset, min_length);
    auto delta_off = res.offset - offset;
    if (len > delta_off) {
      res.length = len - delta_off;
      res.length = p2align<uint64_t>(res.length, min_length);
      if (res.length) {
	return res;
      }
    }
  }
  return interval_t();
}

interval_t AllocatorLevel01Loose::_get_longest_from_l0(uint64_t pos0,
  uint64_t pos1, uint64_t min_length, interval_t* tail) const
{
  interval_t res;
  if (pos0 >= pos1) {
    return res;
  }
  auto pos = pos0;

  interval_t res_candidate;
  if (tail->length != 0) {
    ceph_assert((tail->offset % l0_granularity) == 0);
    ceph_assert((tail->length % l0_granularity) == 0);
    res_candidate.offset = tail->offset / l0_granularity;
    res_candidate.length = tail->length / l0_granularity;
  }
  *tail = interval_t();

  auto d = bits_per_slot;
  slot_t bits = l0[pos / d];
  bits >>= pos % d;
  bool end_loop = false;
  auto min_granules = min_length / l0_granularity;

  do {
    if ((pos % d) == 0) {
      bits = l0[pos / d];
      if (pos1 - pos >= d) {
        switch(bits) {
	  case all_slot_set:
	    // slot is totally free
	    if (!res_candidate.length) {
	      res_candidate.offset = pos;
	    }
	    res_candidate.length += d;
	    pos += d;
	    end_loop = pos >= pos1;
	    if (end_loop) {
	      *tail = res_candidate;
	      res_candidate = _align2units(res_candidate.offset,
		res_candidate.length, min_granules);
	      if(res.length < res_candidate.length) {
		res = res_candidate;
	      }
	    }
	    continue;
	  case all_slot_clear:
	    // slot is totally allocated
	    res_candidate = _align2units(res_candidate.offset,
	      res_candidate.length, min_granules);
	    if (res.length < res_candidate.length) {
	      res = res_candidate;
	    }
	    res_candidate = interval_t();
	    pos += d;
	    end_loop = pos >= pos1;
	    continue;
	}
      }
    } //if ((pos % d) == 0)

    end_loop = ++pos >= pos1;
    if (bits & 1) {
      // item is free
      if (!res_candidate.length) {
	res_candidate.offset = pos - 1;
      }
      ++res_candidate.length;
      if (end_loop) {
	*tail = res_candidate;
	res_candidate = _align2units(res_candidate.offset,
	  res_candidate.length, min_granules);
	if (res.length < res_candidate.length) {
	  res = res_candidate;
	}
      }
    } else {
      res_candidate = _align2units(res_candidate.offset,
	res_candidate.length, min_granules);
      if (res.length < res_candidate.length) {
	res = res_candidate;
      }
      res_candidate = interval_t();
    }
    bits >>= 1;
  } while (!end_loop);
  res.offset *= l0_granularity;
  res.length *= l0_granularity;
  tail->offset *= l0_granularity;
  tail->length *= l0_granularity;
  return res;
}

void AllocatorLevel01Loose::_analyze_partials(uint64_t pos_start,
  uint64_t pos_end, uint64_t length, uint64_t min_length, int mode,
  search_ctx_t* ctx)
{
  auto d = L1_ENTRIES_PER_SLOT;
  ceph_assert((pos_start % d) == 0);
  ceph_assert((pos_end % d) == 0);

  uint64_t l0_w = slots_per_slotset * L0_ENTRIES_PER_SLOT;

  uint64_t l1_pos = pos_start;
  const interval_t empty_tail;
  interval_t prev_tail;

  uint64_t next_free_l1_pos = 0;
  // 遍历 start~end 区间内的 l1 slot
  for (auto pos = pos_start / d; pos < pos_end / d; ++pos) {
    slot_t slot_val = l1[pos];
    // FIXME minor: code below can be optimized to check slot_val against
    // all_slot_set(_clear) value
    // 遍历所有 l1 entry
    for (auto c = 0; c < d; c++) {
      switch (slot_val & L1_ENTRY_MASK) {
      case L1_ENTRY_FREE: // 该 l1 entry 对应的 l0 slotset 全部未使用
        prev_tail  = empty_tail;
        if (!ctx->free_count) { // 找到的第一个 全部未使用的 l1 entry
          ctx->free_l1_pos = l1_pos; // 记录它的起始位置
        } else if (l1_pos != next_free_l1_pos) { // 不相等说明遍历过程中出现了非全部未使用的 l1 entry
	  auto o = ctx->free_l1_pos * l1_granularity;
	  auto l = ctx->free_count * l1_granularity;
          // check if already found extent fits min_length after alignment
	  // 检查是否这段连续的空间是否满足最小空间要求，如果不满足，则重置统计信息，继续查找
	  if (_align2units(o, l, min_length).length >= min_length) {
	    break;
	  }
	  // if not - proceed with the next one
          ctx->free_l1_pos = l1_pos;
          ctx->free_count = 0;
	}
	next_free_l1_pos = l1_pos + 1;
        ++ctx->free_count;
        if (mode == STOP_ON_EMPTY) {
          return;
        }
        break;
      case L1_ENTRY_FULL:
        prev_tail = empty_tail;
        break;
      case L1_ENTRY_PARTIAL:
	interval_t longest;
        ++ctx->partial_count;

        longest = _get_longest_from_l0(l1_pos * l0_w, (l1_pos + 1) * l0_w, min_length, &prev_tail);

	/**
	 * 两个 if 表达式中的 != 0 && < 已存在的值，想要取得的效果就是在满足要求的前提下使用最小的
	 */
        if (longest.length >= length) {
          if ((ctx->affordable_len == 0) ||
              ((ctx->affordable_len != 0) &&
                (longest.length < ctx->affordable_len))) {
            ctx->affordable_len = longest.length;
	    ctx->affordable_offs = longest.offset;
          }
        }
        if (longest.length >= min_length &&
	    (ctx->min_affordable_len == 0 ||
	      (longest.length < ctx->min_affordable_len))) {

          ctx->min_affordable_len = p2align<uint64_t>(longest.length, min_length);
	  ctx->min_affordable_offs = longest.offset;
        }
        if (mode == STOP_ON_PARTIAL) {
          return;
        }
        break;
      }
      slot_val >>= L1_ENTRY_WIDTH;
      ++l1_pos;
    }
  }
  ctx->fully_processed = true;
}

// 根据 l0 的状态更新 l1 entry 的状态
void AllocatorLevel01Loose::_mark_l1_on_l0(int64_t l0_pos, int64_t l0_pos_end)
{
  if (l0_pos == l0_pos_end) {
    return;
  }
  auto d0 = bits_per_slotset;
  uint64_t l1_w = L1_ENTRIES_PER_SLOT;
  // this should be aligned with slotset boundaries
  ceph_assert(0 == (l0_pos % d0));
  ceph_assert(0 == (l0_pos_end % d0));

  // idx 表示 l0 起始位置对应 l0 slot 的索引
  int64_t idx = l0_pos / bits_per_slot;
  // idx_end 表示 l0 结束位置对应的 l0 slot 的索引
  int64_t idx_end = l0_pos_end / bits_per_slot;
  slot_t mask_to_apply = L1_ENTRY_NOT_USED; // 初始状态，正常不会是这种状态

  auto l1_pos = l0_pos / d0;

  // 循环处理所有的 l0 slot
  while (idx < idx_end) {
    // l0 slot 被标记为全部已使用
    if (l0[idx] == all_slot_clear) {
      // if not all prev slots are allocated then no need to check the
      // current slot set, it's partial
      ++idx;
      // 如果此时既不是 初始状态 也不是 全部使用，则表示之前某个 slot 判断为 部分使用，则标记为部分使用，且跳过整个 slotset
      if (mask_to_apply == L1_ENTRY_NOT_USED) {
	mask_to_apply = L1_ENTRY_FULL;
      } else if (mask_to_apply != L1_ENTRY_FULL) {
	idx = p2roundup(idx, int64_t(slots_per_slotset));
        mask_to_apply = L1_ENTRY_PARTIAL;
      }
    } else if (l0[idx] == all_slot_set) {
      // if not all prev slots are free then no need to check the
      // current slot set, it's partial
      ++idx;
      // 如果此时既不是 初始状态 也不是 全部空闲，则表示之前某个 slot 判断为 部分使用，则标记为部分使用，且跳过整个 slotset
      if (mask_to_apply == L1_ENTRY_NOT_USED) {
	mask_to_apply = L1_ENTRY_FREE;
      } else if (mask_to_apply != L1_ENTRY_FREE) {
	idx = p2roundup(idx, int64_t(slots_per_slotset));
        mask_to_apply = L1_ENTRY_PARTIAL;
      }
    } else {
      // no need to check the current slot set, it's partial
      mask_to_apply = L1_ENTRY_PARTIAL;
      ++idx;
      idx = p2roundup(idx, int64_t(slots_per_slotset));
    }
    // 遍历完一个 slotset
    if ((idx % slots_per_slotset) == 0) {
      ceph_assert(mask_to_apply != L1_ENTRY_NOT_USED);
      // 得到 l1_pos 所在 slot 的 bit 偏移
      uint64_t shift = (l1_pos % l1_w) * L1_ENTRY_WIDTH;
      // 得到 l1_pos 所在 slot 的值（uint64_t）
      slot_t& slot_val = l1[l1_pos / l1_w];
      auto mask = slot_t(L1_ENTRY_MASK) << shift;

      // old_mask 原位置上的值
      slot_t old_mask = (slot_val & mask) >> shift;
      // 先减去所对应状态的统计信息
      switch(old_mask) {
      case L1_ENTRY_FREE:
	unalloc_l1_count--;
	break;
      case L1_ENTRY_PARTIAL:
	partial_l1_count--;
	break;
      }
      // 按位与 ~mask 表示先将原先的值设置为 00
      slot_val &= ~mask;
      // 更新为新的状态
      slot_val |= slot_t(mask_to_apply) << shift;
      // 重新更新统计
      switch(mask_to_apply) {
      case L1_ENTRY_FREE:
	unalloc_l1_count++;
	break;
      case L1_ENTRY_PARTIAL:
	partial_l1_count++;
	break;
      }
      // 复位为初始状态，继续遍历
      mask_to_apply = L1_ENTRY_NOT_USED;
      ++l1_pos;
    }
  }
}

void AllocatorLevel01Loose::_mark_alloc_l0(int64_t l0_pos_start,
  int64_t l0_pos_end)
{
  auto d0 = L0_ENTRIES_PER_SLOT;

  int64_t pos = l0_pos_start;
  // l0_pos_start % d0 表示 l0 start 在一个 l0 slot 内的偏移
  // 1<< 用于执行后续的位操作来标记为已使用
  slot_t bits = (slot_t)1 << (l0_pos_start % d0);
  // pos / d0 表示该 AU 对应的 bit 在哪个 slot 内，即数组的索引
  slot_t* val_s = l0.data() + (pos / d0);
  
  // 以下是分批次处理
  // 第一批处理 start ~ start_slot 内的 bit
  int64_t pos_e = std::min(l0_pos_end, p2roundup<int64_t>(l0_pos_start + 1, d0));
  while (pos < pos_e) {
    (*val_s) &= ~bits;
    bits <<= 1;
    pos++;
  }
  // 第二批处理 start ~ end 中间完成的 slot 内的 bit
  pos_e = std::min(l0_pos_end, p2align<int64_t>(l0_pos_end, d0));
  while (pos < pos_e) {
    *(++val_s) = all_slot_clear;
    pos += d0;
  }
  bits = 1;
  ++val_s;
  // 处理 end_slot_start ~ end 之间的 bit
  while (pos < l0_pos_end) {
    (*val_s) &= ~bits;
    bits <<= 1;
    pos++;
  }
}

interval_t AllocatorLevel01Loose::_allocate_l1_contiguous(uint64_t length,
  uint64_t min_length, uint64_t max_length,
  uint64_t pos_start, uint64_t pos_end)
{
  interval_t res = { 0, 0 };
  uint64_t l0_w = slots_per_slotset * L0_ENTRIES_PER_SLOT;

  if (unlikely(length <= l0_granularity)) {
    search_ctx_t ctx;
    // length 小于最小分配单元的情况下，部分未分配的 l1 entry 应该就可以满足要求
    _analyze_partials(pos_start, pos_end, l0_granularity, l0_granularity,
      STOP_ON_PARTIAL, &ctx);

    // check partially free slot sets first (including neighboring),
    // full length match required.
    // 即 start~end 范围内有连续的空间满足 length 要求
    if (ctx.affordable_len) {
      // allocate as specified
      ceph_assert(ctx.affordable_len >= length);
      auto pos = ctx.affordable_offs / l0_granularity;
      _mark_alloc_l1_l0(pos, pos + 1);
      res = interval_t(ctx.affordable_offs, length);
      return res;
    }

    // allocate from free slot sets
    // 如果不满足 length，但是有空闲的 l0 slotset，则分配之
    if (ctx.free_count) {
      auto l = std::min(length, ctx.free_count * l1_granularity);
      ceph_assert((l % l0_granularity) == 0);
      auto pos_end = ctx.free_l1_pos * l0_w + l / l0_granularity;

      _mark_alloc_l1_l0(ctx.free_l1_pos * l0_w, pos_end);
      res = interval_t(ctx.free_l1_pos * l1_granularity, l);
      return res;
    }
  } else if (unlikely(length == l1_granularity)) {
    search_ctx_t ctx;
    // length 等于 l1 粒度的时候，超找完全未分配的 l1 entry 即可满足要求
    _analyze_partials(pos_start, pos_end, length, min_length, STOP_ON_EMPTY, &ctx);

    // allocate using contiguous extent found at l1 if any
    // 找到完全空闲的 l1 entry，分配之
    if (ctx.free_count) {

      auto l = std::min(length, ctx.free_count * l1_granularity);
      ceph_assert((l % l0_granularity) == 0);
      auto pos_end = ctx.free_l1_pos * l0_w + l / l0_granularity;

      _mark_alloc_l1_l0(ctx.free_l1_pos * l0_w, pos_end);
      res = interval_t(ctx.free_l1_pos * l1_granularity, l);

      return res;
    }

    // we can terminate earlier on free entry only
    ceph_assert(ctx.fully_processed);

    // check partially free slot sets first (including neighboring),
    // full length match required.
    // _analyze_partials 内部有判断，可提供的长度大于 length 才会赋值
    // 这里有值及说明该段长度满足要求
    if (ctx.affordable_len) {
      ceph_assert(ctx.affordable_len >= length);
      ceph_assert((length % l0_granularity) == 0);
      auto pos_start = ctx.affordable_offs / l0_granularity;
      auto pos_end = (ctx.affordable_offs + length) / l0_granularity;
      _mark_alloc_l1_l0(pos_start, pos_end);
      res = interval_t(ctx.affordable_offs, length);
      return res;
    }
    // _analyze_partials 内部有判断，可提供的长度大于 min_length 才会赋值
    // 这里有值及说明该段长度满足要求，但这里应该是没能分配全部需要的空间
    if (ctx.min_affordable_len) {
      auto pos_start = ctx.min_affordable_offs / l0_granularity;
      auto pos_end = (ctx.min_affordable_offs + ctx.min_affordable_len) / l0_granularity;
      _mark_alloc_l1_l0(pos_start, pos_end);
      return interval_t(ctx.min_affordable_offs, ctx.min_affordable_len);
    }
  } else { // 最小分配单位大于 l1 粒度，此时不
    search_ctx_t ctx;
    _analyze_partials(pos_start, pos_end, length, min_length, NO_STOP, &ctx);
    ceph_assert(ctx.fully_processed);
    // check partially free slot sets first (including neighboring),
    // full length match required.
    // 优先使用 部分未分配 的区间
    if (ctx.affordable_len) {
      ceph_assert(ctx.affordable_len >= length);
      ceph_assert((length % l0_granularity) == 0);
      auto pos_start = ctx.affordable_offs / l0_granularity;
      auto pos_end = (ctx.affordable_offs + length) / l0_granularity;
      _mark_alloc_l1_l0(pos_start, pos_end);
      res = interval_t(ctx.affordable_offs, length);
      return res;
    }
    // allocate using contiguous extent found at l1 if affordable
    // align allocated extent with min_length
    // 再次使用 完全未使用 的区间
    if (ctx.free_count) {
      auto o = ctx.free_l1_pos * l1_granularity;
      auto l = ctx.free_count * l1_granularity;
      interval_t aligned_extent = _align2units(o, l, min_length);
      if (aligned_extent.length > 0) {
	aligned_extent.length = std::min(length,
	  uint64_t(aligned_extent.length));
	ceph_assert((aligned_extent.offset % l0_granularity) == 0);
	ceph_assert((aligned_extent.length % l0_granularity) == 0);

	auto pos_start = aligned_extent.offset / l0_granularity;
	auto pos_end = (aligned_extent.offset + aligned_extent.length) / l0_granularity;

	_mark_alloc_l1_l0(pos_start, pos_end);
	return aligned_extent;
      }
    }
    // 最次使用能使用的空间
    if (ctx.min_affordable_len) {
      auto pos_start = ctx.min_affordable_offs / l0_granularity;
      auto pos_end = (ctx.min_affordable_offs + ctx.min_affordable_len) / l0_granularity;
      _mark_alloc_l1_l0(pos_start, pos_end);
      return interval_t(ctx.min_affordable_offs, ctx.min_affordable_len);
    }
  }
  return res;
}

bool AllocatorLevel01Loose::_allocate_l1(uint64_t length,
  uint64_t min_length, uint64_t max_length,
  uint64_t l1_pos_start, uint64_t l1_pos_end,
  uint64_t* allocated,
  interval_vector_t* res)
{
  uint64_t d0 = L0_ENTRIES_PER_SLOT;
  uint64_t d1 = L1_ENTRIES_PER_SLOT;

  ceph_assert(0 == (l1_pos_start % (slots_per_slotset * d1)));
  ceph_assert(0 == (l1_pos_end % (slots_per_slotset * d1)));
  if (min_length != l0_granularity) {
    // probably not the most effecient way but
    // don't care much about that at the moment
    bool has_space = true;
    // 分配完或者已经没有空闲空间
    while (length > *allocated && has_space) {
      // 再多次循环中，length - *allocated 会发生变化，即可能多次分配多个小区间来满足要求
      interval_t i =
        _allocate_l1_contiguous(length - *allocated, min_length, max_length,
	  l1_pos_start, l1_pos_end);
      if (i.length == 0) {
        has_space = false; // 没有空闲空间了，退出 while 循环
      } else {
	_fragment_and_emplace(max_length, i.offset, i.length, res);
        *allocated += i.length;
      }
    }
  } else { // 最小分配单位和 l0 粒度相同
    uint64_t l0_w = slots_per_slotset * d0;

    // 根据起始位置和结束位置遍历 l1
    for (auto idx = l1_pos_start / d1;
      idx < l1_pos_end / d1 && length > *allocated;
      ++idx) {
      slot_t& slot_val = l1[idx];
      if (slot_val == all_slot_clear) { // 当前 slot 对应的 l1 entry 所对应的空间已全部分配完毕
        continue;
      } else if (slot_val == all_slot_set) { // 当前 slot 对应的 l1 entry 全部未使用
        // 取全部空间和待分配空间的较小值
        uint64_t to_alloc = std::min(length - *allocated,
          l1_granularity * d1);
        *allocated += to_alloc;
        ++alloc_fragments_fast;
	_fragment_and_emplace(max_length, idx * d1 * l1_granularity, to_alloc,
	  res);
	// 将已分配的物理空间标记为已使用
        _mark_alloc_l1_l0(idx * d1 * bits_per_slotset,
	  idx * d1 * bits_per_slotset + to_alloc / l0_granularity);
        continue;
      }
      // 以下处理当前 slot 部分空闲的场景
      // 找到第一个表示对应位置未使用的 bit
      auto free_pos = find_next_set_bit(slot_val, 0);
      ceph_assert(free_pos < bits_per_slot);
      do {
        ceph_assert(length > *allocated);

        bool empty;
	// 一个 l1 entry 一个 l1 entry 的去找
	// 给定一个 l0 slotset 的起始位置和结束位置，查看这段区间内的空闲空间
	// emptry 为真表示起始和结束位置之间还有空闲空间
        empty = _allocate_l0(length, max_length,
	  (idx * d1 + free_pos / L1_ENTRY_WIDTH) * l0_w,
          (idx * d1 + free_pos / L1_ENTRY_WIDTH + 1) * l0_w,
          allocated,
          res);

	auto mask = slot_t(L1_ENTRY_MASK) << free_pos;
	// 更新统计信息
	slot_t old_mask = (slot_val & mask) >> free_pos;
	switch(old_mask) {
	case L1_ENTRY_FREE:
	  unalloc_l1_count--;
	  break;
	case L1_ENTRY_PARTIAL:
	  partial_l1_count--;
	  break;
	}
        slot_val &= ~mask;
	// 如果没有空闲空间了则标记为全部已分配
	// 如果有则标记为部分分配
        if (empty) {
          // the next line is no op with the current L1_ENTRY_FULL but left
          // as-is for the sake of uniformity and to avoid potential errors
          // in future
          slot_val |= slot_t(L1_ENTRY_FULL) << free_pos;
        } else {
          slot_val |= slot_t(L1_ENTRY_PARTIAL) << free_pos;
	  partial_l1_count++;
        }
	// 分配已完成或当前 slot 已无空闲空间则退出循环
        if (length <= *allocated || slot_val == all_slot_clear) {
          break;
        }
	// 否则查找下一个表示空闲的 bit
	free_pos = find_next_set_bit(slot_val, free_pos + L1_ENTRY_WIDTH);
      } while (free_pos < bits_per_slot);
    }
  }
  return _is_empty_l1(l1_pos_start, l1_pos_end);
}

void AllocatorLevel01Loose::collect_stats(
  std::map<size_t, size_t>& bins_overall)
{
  size_t free_seq_cnt = 0;
  for (auto slot : l0) {
    if (slot == all_slot_set) {
      free_seq_cnt += L0_ENTRIES_PER_SLOT;
    } else if(slot != all_slot_clear) {
      size_t pos = 0;
      do {
	auto pos1 = find_next_set_bit(slot, pos);
	if (pos1 == pos) {
	  free_seq_cnt++;
	  pos = pos1 + 1;
	} else {
	  if (free_seq_cnt) {
	    bins_overall[cbits(free_seq_cnt) - 1]++;
	    free_seq_cnt = 0;
	  }
	  if (pos1 < bits_per_slot) {
	    free_seq_cnt = 1;
	  }
          pos = pos1 + 1;
	}
      } while (pos < bits_per_slot);
    } else if (free_seq_cnt) {
      bins_overall[cbits(free_seq_cnt) - 1]++;
      free_seq_cnt = 0;
    }
  }
  if (free_seq_cnt) {
    bins_overall[cbits(free_seq_cnt) - 1]++;
  }
}

inline ssize_t AllocatorLevel01Loose::count_0s(slot_t slot_val, size_t start_pos)
  {
  #ifdef __GNUC__
    size_t pos = __builtin_ffsll(slot_val >> start_pos);
    if (pos == 0)
      return sizeof(slot_t)*8 - start_pos;
    return pos - 1;
  #else
    size_t pos = start_pos;
    slot_t mask = slot_t(1) << pos;
    while (pos < bits_per_slot && (slot_val & mask) == 0) {
      mask <<= 1;
      pos++;
    }
    return pos - start_pos;
  #endif
  }

 inline ssize_t AllocatorLevel01Loose::count_1s(slot_t slot_val, size_t start_pos)
 {
   return count_0s(~slot_val, start_pos);
 }
void AllocatorLevel01Loose::foreach_internal(
    std::function<void(uint64_t offset, uint64_t length)> notify)
{
  size_t len = 0;
  size_t off = 0;
  for (size_t i = 0; i < l1.size(); i++)
  {
    for (size_t j = 0; j < L1_ENTRIES_PER_SLOT * L1_ENTRY_WIDTH; j += L1_ENTRY_WIDTH)
    {
      size_t w = (l1[i] >> j) & L1_ENTRY_MASK;
      switch (w) {
        case L1_ENTRY_FULL:
          if (len > 0) {
            notify(off, len);
            len = 0;
          }
          break;
        case L1_ENTRY_FREE:
          if (len == 0)
            off = ( ( bits_per_slot * i + j ) / L1_ENTRY_WIDTH ) * slots_per_slotset * bits_per_slot;
          len += bits_per_slotset;
          break;
        case L1_ENTRY_PARTIAL:
          size_t pos = ( ( bits_per_slot * i + j ) / L1_ENTRY_WIDTH ) * slots_per_slotset;
          for (size_t t = 0; t < slots_per_slotset; t++) {
            size_t p = 0;
            slot_t allocation_pattern = l0[pos + t];
            while (p < bits_per_slot) {
              if (len == 0) {
                //continue to skip allocated space, meaning bits set to 0
                ssize_t alloc_count = count_0s(allocation_pattern, p);
                p += alloc_count;
                //now we are switched to expecting free space
                if (p < bits_per_slot) {
                  //now @p are 1s
                  ssize_t free_count = count_1s(allocation_pattern, p);
                  assert(free_count > 0);
                  len = free_count;
                  off = (pos + t) * bits_per_slot + p;
                  p += free_count;
                }
              } else {
                //continue free region
                ssize_t free_count = count_1s(allocation_pattern, p);
                if (free_count == 0) {
                  notify(off, len);
                  len = 0;
                } else {
                  p += free_count;
                  len += free_count;
                }
              }
            }
          }
          break;
      }
    }
  }
  if (len > 0)
    notify(off, len);
}

/**
 * 在 l0 层，从 l0_pos_start 起，向前获取连续的空闲空间，将空闲空间标记为已分配
 * 并返回起始位置
 */
uint64_t AllocatorLevel01Loose::_claim_free_to_left_l0(int64_t l0_pos_start)
{
  int64_t d0 = L0_ENTRIES_PER_SLOT;

  /**
   * l0_pos_start 表示 offset 所对应的 AU 在 l0 中的索引
   * l0_pos_start - 1 即前一个 AU 在 l0 中的索引
   */
  int64_t pos = l0_pos_start - 1;
  // 前一个 AU 在 slot 内对应的 bit
  slot_t bits = (slot_t)1 << (pos % d0);
  // 前一个 AU 所在 slot 的索引
  int64_t idx = pos / d0;
  // 获取前一个 AU 所在 slot 的 uint64_t 的指针
  slot_t* val_s = l0.data() + idx;
  // pos_e ~ pos 覆盖一个 slot 的前部分
  int64_t pos_e = p2align<int64_t>(pos, d0);

  // 将 pos_e ~ pos 连续空闲的位置标记为已分配
  while (pos >= pos_e) {
    // 等于 0 说明 pos 对应的 bit 等于 0，即已分配
    // 说明从 l0_pos_start 之前没有空闲空间
    if (0 == ((*val_s) & bits))
      return pos + 1;
    (*val_s) &= ~bits;
    bits >>= 1;
    --pos;
  }

  // 将整个空闲的 slot 标记为已分配
  --idx;
  val_s = l0.data() + idx;
  while (idx >= 0 && (*val_s) == all_slot_set) {
    *val_s = all_slot_clear;
    --idx;
    pos -= d0;
    val_s = l0.data() + idx;
  }

  // 如果没有遍历到 l0 的起始位置，且 pos 所在的 slot 属于部分分配的状态
  // 倒序遍历连续的空闲的位置
  if (idx >= 0 &&
      (*val_s) != all_slot_set && (*val_s) != all_slot_clear) {
    int64_t pos_e = p2align<int64_t>(pos, d0);
    slot_t bits = (slot_t)1 << (pos % d0);
    while (pos >= pos_e) {
      if (0 == ((*val_s) & bits))
        return pos + 1;
      (*val_s) &= ~bits;
      bits >>= 1;
      --pos;
    }
  }
  return pos + 1;
}

/**
 * 在 l0 层，从 l0_pos_start 起，向后获取连续的空闲空间，将空闲空间标记为已分配
 * 并返回结束位置
 */
uint64_t AllocatorLevel01Loose::_claim_free_to_right_l0(int64_t l0_pos_start)
{
  auto d0 = L0_ENTRIES_PER_SLOT;

  int64_t pos = l0_pos_start;
  slot_t bits = (slot_t)1 << (pos % d0);
  size_t idx = pos / d0;
  if (idx >= l0.size()) {
    return pos;
  }
  slot_t* val_s = l0.data() + idx;

  int64_t pos_e = p2roundup<int64_t>(pos + 1, d0);

  while (pos < pos_e) {
    if (0 == ((*val_s) & bits))
      return pos;
    (*val_s) &= ~bits;
    bits <<= 1;
    ++pos;
  }
  ++idx;
  val_s = l0.data() + idx;
  while (idx < l0.size() && (*val_s) == all_slot_set) {
    *val_s = all_slot_clear;
    ++idx;
    pos += d0;
    val_s = l0.data() + idx;
  }

  if (idx < l0.size() &&
      (*val_s) != all_slot_set && (*val_s) != all_slot_clear) {
    int64_t pos_e = p2roundup<int64_t>(pos + 1, d0);
    slot_t bits = (slot_t)1 << (pos % d0);
    while (pos < pos_e) {
      if (0 == ((*val_s) & bits))
        return pos;
      (*val_s) &= ~bits;
      bits <<= 1;
      ++pos;
    }
  }
  return pos;
}
