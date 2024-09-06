#ifndef __CEPH_SNAP_TYPES_H
#define __CEPH_SNAP_TYPES_H

#include "include/types.h"
#include "include/fs_types.h"

namespace ceph {
class Formatter;
}

struct SnapRealmInfo {
  mutable ceph_mds_snap_realm h;
  std::vector<snapid_t> my_snaps;
  std::vector<snapid_t> prior_parent_snaps;  // before parent_since

  SnapRealmInfo() {
    // FIPS zeroization audit 20191115: this memset is not security related.
    memset(&h, 0, sizeof(h));
  }
  SnapRealmInfo(inodeno_t ino_, snapid_t created_, snapid_t seq_, snapid_t current_parent_since_) {
    // FIPS zeroization audit 20191115: this memset is not security related.
    memset(&h, 0, sizeof(h));
    h.ino = ino_;
    h.created = created_;
    h.seq = seq_;
    h.parent_since = current_parent_since_;
  }
  
  inodeno_t ino() const { return inodeno_t(h.ino); }
  inodeno_t parent() const { return inodeno_t(h.parent); }
  snapid_t seq() const { return snapid_t(h.seq); }
  snapid_t parent_since() const { return snapid_t(h.parent_since); }
  snapid_t created() const { return snapid_t(h.created); }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<SnapRealmInfo*>& o);
};
WRITE_CLASS_ENCODER(SnapRealmInfo)


// 如果是前端应用自定义快照模式（例如 RBD，可以针对每个 image 执行快照操作），那么由前端应用
// 下发的请求（op）会携带 SnapContext，指示当前的快照信息；如果是存储池快照模式，那么 PGPool
// 中的 SnapContext 会指示当前存储池的快照信息（PG 每次更新 OSDMap 的同时会同步更新 PGPool）
struct SnapContext {
  snapid_t seq;			// 最新快照序列号
  std::vector<snapid_t> snaps;	// 当前所有快照序列号（包含 seq 并且降序排列，亦即总有 snaps[0] == seq）

  SnapContext() {}
  SnapContext(snapid_t s, const std::vector<snapid_t>& v) : seq(s), snaps(v) {}    

  bool is_valid() const;

  void clear() {
    seq = 0;
    snaps.clear();
  }
  bool empty() const { return seq == 0; }

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    encode(seq, bl);
    encode(snaps, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    decode(seq, bl);
    decode(snaps, bl);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<SnapContext*>& o);
};
WRITE_CLASS_ENCODER(SnapContext)

inline std::ostream& operator<<(std::ostream& out, const SnapContext& snapc) {
  return out << snapc.seq << "=" << snapc.snaps;
}

//}

#endif
