#ifndef VERSION_H_
#define VERSION_H_

#include <atomic>
#include <cstdint>

/// 版本号结构体（论文3.6节：数据层版本化锁，引用1-93）
/// 核心：读写操作通过版本号判断一致性，避免ABA问题
struct Version {
  std::atomic<uint32_t> read_version;  // 读版本（偶数：稳定，奇数：写入中）
  std::atomic<uint32_t> write_version; // 写版本（与读版本同步）

  Version() : read_version(0), write_version(0) {}

  /// 读锁：获取稳定版本号
  uint32_t ReadLock() const {
    uint32_t v;
    do {
      v = read_version.load(std::memory_order_acquire);
    } while (v % 2 != 0);  // 等待写入完成（奇数表示写入中）
    return v;
  }

  /// 写锁：标记写入开始
  void WriteLock() {
    uint32_t v = read_version.fetch_add(1, std::memory_order_acq_rel);
    write_version.store(v + 1, std::memory_order_release);
  }

  /// 写解锁：标记写入结束
  void WriteUnlock() {
    read_version.fetch_add(1, std::memory_order_acq_rel);
  }

  /// 检查版本一致性（读操作后验证）
  bool IsConsistent(uint32_t start_version) const {
    return read_version.load(std::memory_order_acquire) == start_version;
  }
};

#endif  // VERSION_H_
