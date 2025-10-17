// SegmentedBlock.cpp
#include "SegmentedBlock.h"
#include "PerThreadDataBlock.h"
#include <functional>
#include <cassert>
#include <thread>

// thread-local hashed id for slot claiming
static uint64_t thread_hash_id()
{
  static thread_local uint64_t thash = 0;
  if (thash == 0)
  {
    std::hash<std::thread::id> h;
    thash = (uint64_t)h(std::this_thread::get_id());
    if (thash == 0)
      thash = 1;
  }
  return thash;
}

SegmentedBlock::SegmentedBlock(size_t capacity, uint64_t window_id)
    : slots_(capacity), capacity_(capacity), version_(0), window_id_(window_id)
{
  // slots_(capacity) will default-construct capacity SegSlot
}

SegmentedBlock::~SegmentedBlock()
{
  // Not deleting PerThreadDataBlock here: ownership is transferred to converter
}

// 获取当前线程的 PerThreadDataBlock（不存在则创建并安装）
PerThreadDataBlock *SegmentedBlock::get_or_install_block_for_current_thread()
{
  uint64_t my = thread_hash_id();

  // 1) try find existing owner slot
  for (size_t i = 0; i < capacity_; ++i)
  {
    uint64_t owner = slots_[i].owner.load(std::memory_order_acquire);
    if (owner == my)
    {
      PerThreadDataBlock *p = slots_[i].ptr.load(std::memory_order_acquire);
      if (p)
        return p;
      // lazy allocate
      PerThreadDataBlock *b = new PerThreadDataBlock();
      PerThreadDataBlock *expected = nullptr;
      if (slots_[i].ptr.compare_exchange_strong(expected, b, std::memory_order_acq_rel))
        return b;
      delete b;
      return expected;
    }
  }

  // 2) claim empty slot
  for (size_t i = 0; i < capacity_; ++i)
  {
    uint64_t expected_owner = 0;
    if (slots_[i].owner.compare_exchange_strong(expected_owner, my, std::memory_order_acq_rel))
    {
      PerThreadDataBlock *b = new PerThreadDataBlock();
      slots_[i].ptr.store(b, std::memory_order_release);
      return b;
    }
  }

  // 3) no slot available
  return nullptr;
}

// 标记转换（避免重复）
// *** MODIFIED: 注释强调该函数为 "first-wins" CAS 标记，后续调用返回 false。
bool SegmentedBlock::try_mark_converting()
{
  const uint64_t CONVERT_BIT = (1ull << 63);
  uint64_t cur = version_.load(std::memory_order_acquire);
  if (cur & CONVERT_BIT)
    return false;
  uint64_t desired = cur | CONVERT_BIT;
  return version_.compare_exchange_strong(cur, desired, std::memory_order_acq_rel);
}

// （可选增强）判断转换条件：至少半数 slot 满或存在延迟键
// 说明：由于当前类未持有每线程块的满载状态与键范围，此函数应由上层 SBTree
// 在收集统计后做判断；此处保留现状，实现位于 SBTree::insert 中。
