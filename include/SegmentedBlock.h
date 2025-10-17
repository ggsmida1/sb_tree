// SegmentedBlock.h
#pragma once
#include <atomic>
#include <cstdint>
#include <cstddef>
#include <vector>
#include <thread>

class PerThreadDataBlock;

// 每个 slot 保存 owner（线程 id hash）与 ptr（该线程的 PerThreadDataBlock*）
struct SegSlot
{
  std::atomic<uint64_t> owner;
  std::atomic<PerThreadDataBlock *> ptr;
  SegSlot() : owner(0), ptr(nullptr) {}
};

// SegmentedBlock：每线程拥有独立 PerThreadDataBlock，无需锁即可写入。
// 新增：window_id_ 表示该 segmented block 所属逻辑窗口（time window）
class SegmentedBlock
{
public:
  explicit SegmentedBlock(size_t capacity = 128, uint64_t window_id = 0);
  ~SegmentedBlock();

  SegmentedBlock(const SegmentedBlock &) = delete;
  SegmentedBlock &operator=(const SegmentedBlock &) = delete;

  // 获取当前线程的 PerThreadDataBlock（如不存在则自动创建并安装）
  PerThreadDataBlock *get_or_install_block_for_current_thread();

  // 遍历所有注册过的 per-thread buffer
  template <typename F>
  void for_each_registered_ptr(F &&f) const
  {
    for (size_t i = 0; i < capacity_; ++i)
    {
      PerThreadDataBlock *p = slots_[i].ptr.load(std::memory_order_acquire);
      if (p)
        f(p);
    }
  }

  // 标记当前 SegmentedBlock 为“正在转换”状态（第一次调用返回 true）
  bool try_mark_converting();

  // *** MODIFIED: 新增：读状态判断接口（非阻塞）
  bool is_converting() const
  {
    const uint64_t CONVERT_BIT = (1ull << 63);
    return (version_.load(std::memory_order_acquire) & CONVERT_BIT) != 0;
  }

  size_t capacity() const { return capacity_; }

  // 窗口 id 访问
  uint64_t window_id() const { return window_id_; }

private:
  std::vector<SegSlot> slots_;
  size_t capacity_;
  std::atomic<uint64_t> version_{0};
  uint64_t window_id_;
};
