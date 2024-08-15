// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <mutex>
#include <boost/intrusive/avl_set.hpp>

#include "Allocator.h"
#include "os/bluestore/bluestore_types.h"
#include "include/mempool.h"

/**
 * 两棵 avl 树
 * 第一棵树以 seg 的起始位置排序，起始位置更大的排在后边
 * 第二棵树以 seg 的长度排序，长度更大的排在后边
 */
struct range_seg_t {
  MEMPOOL_CLASS_HELPERS();  ///< memory monitoring
  uint64_t start;   ///< starting offset of this segment
  uint64_t end;	    ///< ending offset (non-inclusive)

  range_seg_t(uint64_t start, uint64_t end)
    : start{start},
      end{end}
  {}
  // Tree is sorted by offset, greater offsets at the end of the tree.
  // 以起始位置进行排序的的比价器
  struct before_t {
    template<typename KeyLeft, typename KeyRight>
    bool operator()(const KeyLeft& lhs, const KeyRight& rhs) const {
      return lhs.end <= rhs.start;
    }
  };
  // 以起始位置进行排序的树的钩子
  boost::intrusive::avl_set_member_hook<> offset_hook;

  // Tree is sorted by size, larger sizes at the end of the tree.
  // 以长度进行排序的的比价器
  struct shorter_t {
    template<typename KeyType>
    bool operator()(const range_seg_t& lhs, const KeyType& rhs) const {
      auto lhs_size = lhs.end - lhs.start;
      auto rhs_size = rhs.end - rhs.start;
      if (lhs_size < rhs_size) {
	return true;
      } else if (lhs_size > rhs_size) {
	return false;
      } else {
	return lhs.start < rhs.start;
      }
    }
  };
  inline uint64_t length() const {
    return end - start;
  }
  // 以长度进行排序的树的钩子
  boost::intrusive::avl_set_member_hook<> size_hook;
};

class AvlAllocator : public Allocator {
  // 处理 range_seg，即删除之前的内存
  struct dispose_rs {
    void operator()(range_seg_t* p)
    {
      delete p;
    }
  };

protected:
  /*
  * ctor intended for the usage from descendant class(es) which
  * provides handling for spilled over entries
  * (when entry count >= max_entries)
  */
  AvlAllocator(CephContext* cct, int64_t device_size, int64_t block_size,
    uint64_t max_mem,
    std::string_view name);

public:
  AvlAllocator(CephContext* cct, int64_t device_size, int64_t block_size,
	       std::string_view name);
  ~AvlAllocator();
  const char* get_type() const override
  {
    return "avl";
  }
  int64_t allocate(
    uint64_t want,
    uint64_t unit,
    uint64_t max_alloc_size,
    int64_t  hint,
    PExtentVector *extents) override;
  void release(const interval_set<uint64_t>& release_set) override;
  uint64_t get_free() override;
  uint64_t get_size();
  double get_fragmentation() override;

  void dump() override;
  void foreach(
    std::function<void(uint64_t offset, uint64_t length)> notify) override;
  void init_add_free(uint64_t offset, uint64_t length) override;
  void init_rm_free(uint64_t offset, uint64_t length) override;
  void shutdown() override;

private:
  // pick a range by search from cursor forward
  uint64_t _pick_block_after(
    uint64_t *cursor,
    uint64_t size,
    uint64_t align);
  // pick a range with exactly the same size or larger
  uint64_t _pick_block_fits(
    uint64_t size,
    uint64_t align);
  int _allocate(
    uint64_t size,
    uint64_t unit,
    uint64_t *offset,
    uint64_t *length);

  using range_tree_t = 
    boost::intrusive::avl_set<
      range_seg_t,
      boost::intrusive::compare<range_seg_t::before_t>,
      boost::intrusive::member_hook<
	range_seg_t,
	boost::intrusive::avl_set_member_hook<>,
	&range_seg_t::offset_hook>>;
  // 以 offset 排序的 avl 树
  range_tree_t range_tree;    ///< main range tree
  /*
   * The range_size_tree should always contain the
   * same number of segments as the range_tree.
   * The only difference is that the range_size_tree
   * is ordered by segment sizes.
   */
  using range_size_tree_t =
    boost::intrusive::avl_multiset<
      range_seg_t,
      boost::intrusive::compare<range_seg_t::shorter_t>,
      boost::intrusive::member_hook<
	range_seg_t,
	boost::intrusive::avl_set_member_hook<>,
	&range_seg_t::size_hook>,
      boost::intrusive::constant_time_size<true>>;
  // 以 长度 排序的 avl 树
  range_size_tree_t range_size_tree;

  uint64_t num_free = 0;     ///< total bytes in freelist

  /*
   * This value defines the number of elements in the ms_lbas array.
   * The value of 64 was chosen as it covers all power of 2 buckets
   * up to UINT64_MAX.
   * This is the equivalent of highest-bit of UINT64_MAX.
   */
  static constexpr unsigned MAX_LBAS = 64;
  uint64_t lbas[MAX_LBAS] = {0};

  /*
   * Minimum size which forces the dynamic allocator to change
   * it's allocation strategy.  Once the allocator cannot satisfy
   * an allocation of this size then it switches to using more
   * aggressive strategy (i.e search by size rather than offset).
   *
   * 当剩余空间的最大块小于该值时分配策略从 first-fit 切换到 best-fit
   */
  uint64_t range_size_alloc_threshold = 0;
  /*
   * The minimum free space, in percent, which must be available
   * in allocator to continue allocations in a first-fit fashion.
   * Once the allocator's free space drops below this level we dynamically
   * switch to using best-fit allocations.
   *
   * 当总体剩余空间百分比小于该值时分配策略从 first-fit 切换到 best-fit
   */
  int range_size_alloc_free_pct = 0;
  /*
   * Maximum number of segments to check in the first-fit mode, without this
   * limit, fragmented device can see lots of iterations and _block_picker()
   * becomes the performance limiting factor on high-performance storage.
   *
   * first-fit 分配策略下的最大搜索次数，超过该值时切换到 best-fit
   */
  const uint32_t max_search_count;
  /*
   * Maximum distance to search forward from the last offset, without this
   * limit, fragmented device can see lots of iterations and _block_picker()
   * becomes the performance limiting factor on high-performance storage.
   *
   * first-fit 分配策略下的最大搜索字节数，超过该值时切换到 best-fit
   */
  const uint32_t max_search_bytes;
  /*
   * Max amount of range entries allowed. 0 - unlimited
   * val tree 的最大节点个数
   */
  uint64_t range_count_cap = 0;

  // 删除一个节点
  void _range_size_tree_rm(range_seg_t& r) {
    ceph_assert(num_free >= r.length());
    num_free -= r.length();	// 调整剩余空间统计
    range_size_tree.erase(r);	// 删除树的节点
  }
  void _range_size_tree_try_insert(range_seg_t& r) {
    if (_try_insert_range(r.start, r.end)) { // 为真表示可以插入
      range_size_tree.insert(r); // 插入到 size 树中
      num_free += r.length();    // 调整统计信息
    } else {
      // 为假说明不能插入，在 if 中的函数中已经将其加入到派生类分配器中
      // 在调用者中，该节点还在树上，这里还需要将其移除
      range_tree.erase_and_dispose(r, dispose_rs{});
    }
  }
  // 返回值为 true 表示可以插入到 avl 中，为 false 表示不能插入且已完成溢出处理。
  // insert_pos 为 nullptr 时表示在调用者中完成两棵树的插入
  bool _try_insert_range(uint64_t start,
                         uint64_t end,
                        range_tree_t::iterator* insert_pos = nullptr) {
    // !range_count_cap 为真说明没有指定 avl tree 最大节点个数，可以直接插入
    // range_size_tree.size() < range_count_cap 说明节点个数没有达到上限，可以直接插入
    bool res = !range_count_cap || range_size_tree.size() < range_count_cap;
    bool remove_lowest = false;
    // res == false 说明不能直接插入
    if (!res) {
      // 比较要插入节点的长度和树中最小节点的长度，如果要插入的长度大，则移除最小节点
      // 移除最小节点后，res 设置为 true 表明可以插入
      if (end - start > _lowest_size_available()) {
        remove_lowest = true;
        res = true;
      }
    }

    // 走到这里 !res 为真说明 avl tree 节点数达到上限并且小于最小节点的长度
    if (!res) {
      // 只有设置了最大节点个数才会走到这里，那么必须有派生类实现了该函数
      // 目前是 HybridAllocator，在该分配器中将这部分区间添加到 bitmap 分配器中管理
      _spillover_range(start, end);
    } else {
      // NB:  we should do insertion before the following removal
      // to avoid potential iterator disposal insertion might depend on.
      // 如果指定了插入位置则根据插入位置插入
      if (insert_pos) {
	// 生成一个新的节点
        auto new_rs = new range_seg_t{ start, end };
	// 在指定节点之前插入到 offset 树中
        range_tree.insert_before(*insert_pos, *new_rs);
	// 插入到 size 树中
        range_size_tree.insert(*new_rs);
	// 调整统计信息
        num_free += new_rs->length();
      }
      // 这里处理需要移除最小节点的场景
      if (remove_lowest) {
	// 获取最小节点
        auto r = range_size_tree.begin();
	// 将其从 size 树中删除
        _range_size_tree_rm(*r);
	// 将其插入到派生类分配器指定的分配器中
        _spillover_range(r->start, r->end);
	// 将最小节点从 offset 树中删除
        range_tree.erase_and_dispose(*r, dispose_rs{});
      }
    }
    return res;
  }
  virtual void _spillover_range(uint64_t start, uint64_t end) {
    // this should be overriden when range count cap is present,
    // i.e. (range_count_cap > 0)
    ceph_assert(false);
  }
protected:
  // called when extent to be released/marked free
  virtual void _add_to_tree(uint64_t start, uint64_t size);

protected:
  CephContext* cct;
  std::mutex lock;

  double _get_fragmentation() const {
    auto free_blocks = p2align(num_free, (uint64_t)block_size) / block_size;
    if (free_blocks <= 1) {
      return .0;
    }
    return (static_cast<double>(range_tree.size() - 1) / (free_blocks - 1));
  }
  void _dump() const;
  void _foreach(std::function<void(uint64_t offset, uint64_t length)>) const;

  uint64_t _lowest_size_available() {
    auto rs = range_size_tree.begin();
    return rs != range_size_tree.end() ? rs->length() : 0;
  }

  int64_t _allocate(
    uint64_t want,
    uint64_t unit,
    uint64_t max_alloc_size,
    int64_t  hint,
    PExtentVector *extents);

  void _release(const interval_set<uint64_t>& release_set);
  void _release(const PExtentVector&  release_set);
  void _shutdown();

  void _process_range_removal(uint64_t start, uint64_t end, range_tree_t::iterator& rs);
  void _remove_from_tree(uint64_t start, uint64_t size);
  void _try_remove_from_tree(uint64_t start, uint64_t size,
    std::function<void(uint64_t offset, uint64_t length, bool found)> cb);

  uint64_t _get_free() const {
    return num_free;
  }
};
