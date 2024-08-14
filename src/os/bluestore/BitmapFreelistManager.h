// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_BITMAPFREELISTMANAGER_H
#define CEPH_OS_BLUESTORE_BITMAPFREELISTMANAGER_H

#include "FreelistManager.h"

#include <string>
#include <mutex>

#include "common/ceph_mutex.h"
#include "include/buffer.h"
#include "kv/KeyValueDB.h"

class BitmapFreelistManager : public FreelistManager {
  // 在 rocksdb 中 key 的前缀，meta 为 B，bitmap 为 b
  std::string meta_prefix, bitmap_prefix;
  // merge 操作，实际上就是按位异或
  std::shared_ptr<KeyValueDB::MergeOperator> merge_op;
  ceph::mutex lock = ceph::make_mutex("BitmapFreelistManager::lock");

  uint64_t size;            ///< size of device (bytes)
  uint64_t bytes_per_block; ///< bytes per block (bdev_block_size)
  uint64_t blocks_per_key;  ///< blocks (bits) per key/value pair, default is 128
  uint64_t bytes_per_key;   ///< bytes per key/value pair, == bytes_per_block * blocks_per_key
  uint64_t blocks;          ///< size of device (blocks, size rounded up)

  // something like 0xFFFF0000
  uint64_t block_mask;  ///< mask to convert byte offset to block offset, block offset 取余
  uint64_t key_mask;    ///< mask to convert offset to key offset, key offset 取整

  ceph::buffer::list all_set_bl; ///< 单个 key 对应的 value 的 buffer

 // 遍历 rocksdb key 相关的成员
  KeyValueDB::Iterator enumerate_p;
  uint64_t enumerate_offset; ///< logical offset; position
  ceph::buffer::list enumerate_bl;   ///< current key at enumerate_offset
  int enumerate_bl_pos;      ///< bit position in enumerate_bl

  uint64_t _get_offset(uint64_t key_off, int bit) {
    return key_off + bit * bytes_per_block;
  }

  void _init_misc();

  void _xor(
    uint64_t offset, uint64_t length,
    KeyValueDB::Transaction txn);

  int _read_cfg(
    std::function<int(const std::string&, std::string*)> cfg_reader);

  int _expand(uint64_t new_size, KeyValueDB* db);

  uint64_t size_2_block_count(uint64_t target_size) const;

  int read_size_meta_from_db(KeyValueDB* kvdb, uint64_t* res);
  void _sync(KeyValueDB* kvdb, bool read_only);

  void _load_from_db(KeyValueDB* kvdb);

public:
  BitmapFreelistManager(CephContext* cct, std::string meta_prefix,
			std::string bitmap_prefix);

  static void setup_merge_operator(KeyValueDB *db, std::string prefix);

  /**
   * 通过 BlueStore 的 mkfs 接口创建一个 BitmapFreelistManager 实例
   *
   * 因为 BitmapFreelistManager 中的一些关键参数如：块大小、每个段包含的块数目等都是可配置的，
   * 所以需要通过 create() 固化到 kvDB 中。后续重新上电时，这些参数将直接从 kvDB 读取，
   * 防止因为配置变化而导致 BitmapFreelistManager 无法正常工作
   */
  int create(uint64_t size, uint64_t granularity,
	     uint64_t zone_size, uint64_t first_sequential_zone,
	     KeyValueDB::Transaction txn) override;

  /**
   * 初始化 BitmapFreelistManager 。上电时调用，用于从 kvDB 中加载块大小、每个段包含的
   * 块数目等可配置参数
   */
  int init(KeyValueDB *kvdb, bool db_in_read_only,
    std::function<int(const std::string&, std::string*)> cfg_reader) override;

  void shutdown() override;
  void sync(KeyValueDB* kvdb) override;

  void dump(KeyValueDB *kvdb) override;

  /**
   * 上电时，BlueStore 通过这两个接口遍历 BitmapFreelistManager 中所有空闲字段，
   * 并将其从 Allocator 中同步移除，从而还原得到上一次上电时 Allocator 对应的内存结构
   */
  void enumerate_reset() override;
  bool enumerate_next(KeyValueDB *kvdb, uint64_t *offset, uint64_t *length) override;

  /**
   * 从 BitmapFreelistManager 中分配指定范围（ [offset, offset_len] ）空间
   */
  void allocate(
    uint64_t offset, uint64_t length,
    KeyValueDB::Transaction txn) override;
  /**
   * 从 BitmapFreelistManager 中释放制定范围（ [offset, offset_len] ）空间
   */
  void release(
    uint64_t offset, uint64_t length,
    KeyValueDB::Transaction txn) override;

  inline uint64_t get_size() const override {
    return size;
  }
  inline uint64_t get_alloc_units() const override {
    return size / bytes_per_block;
  }
  inline uint64_t get_alloc_size() const override {
    return bytes_per_block;
  }
  void get_meta(uint64_t target_size,
    std::vector<std::pair<std::string, std::string>>*) const override;
};

#endif
