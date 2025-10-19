#include "segmented_block.h"
#include "block_allocator.h"
#include <algorithm>
#include <thread>
#include <chrono>

// -----------------------------------------------------------------------------
// SegmentedBlock 实现（论文3.3节，引用1-71、1-105）
// -----------------------------------------------------------------------------
SegmentedBlock::SegmentedBlock(size_t max_threads, BlockAllocator* allocator)
    : max_threads_(max_threads),
      allocator_(allocator),
      min_key_(kInvalidKey),
      max_key_(0) {
  per_thread_blocks_ = std::make_unique<std::atomic<PerThreadDataBlock*>[]>(max_threads_);
  for (size_t i = 0; i < max_threads_; ++i) {
    per_thread_blocks_[i].store(nullptr, std::memory_order_relaxed);
  }
}

SegmentedBlock::~SegmentedBlock() {
  for (size_t i = 0; i < max_threads_; ++i) {
    PerThreadDataBlock* ptr = per_thread_blocks_[i].load(std::memory_order_acquire);
    if (ptr) {
      delete ptr;
      per_thread_blocks_[i].store(nullptr, std::memory_order_release);
    }
  }
}

PerThreadDataBlock* SegmentedBlock::AllocatePerThreadBlock(size_t thread_id) {
  if (thread_id >= max_threads_) {
    return nullptr;
  }
  
  PerThreadDataBlock* existing = per_thread_blocks_[thread_id].load(std::memory_order_acquire);
  if (existing) return existing;
  
  PerThreadDataBlock* created = new PerThreadDataBlock(allocator_);
  if (per_thread_blocks_[thread_id].compare_exchange_strong(existing, created, std::memory_order_acq_rel, std::memory_order_acquire)) {
    return created;
  }
  // 其他线程已放入
  delete created;
  return existing;
}

PerThreadDataBlock* SegmentedBlock::LoadPerThreadBlock(size_t thread_id) const {
  if (thread_id >= max_threads_) return nullptr;
  return per_thread_blocks_[thread_id].load(std::memory_order_acquire);
}

PerThreadDataBlock* SegmentedBlock::StealPerThreadBlock(size_t thread_id) {
  if (thread_id >= max_threads_) return nullptr;
  return per_thread_blocks_[thread_id].exchange(nullptr, std::memory_order_acq_rel);
}

void SegmentedBlock::UpdateKeyRange(uint64_t key) {
  // 原子更新最小键
  uint64_t current_min = min_key_.load(std::memory_order_acquire);
  while (current_min == kInvalidKey || key < current_min) {
    if (min_key_.compare_exchange_weak(current_min, key, 
                                       std::memory_order_acq_rel, 
                                       std::memory_order_acquire)) {
      break;
    }
  }
  
  // 原子更新最大键
  uint64_t current_max = max_key_.load(std::memory_order_acquire);
  while (key > current_max) {
    if (max_key_.compare_exchange_weak(current_max, key, 
                                       std::memory_order_acq_rel, 
                                       std::memory_order_acquire)) {
      break;
    }
  }
}

bool SegmentedBlock::NeedConversion(uint64_t /*current_max_key*/) const {
  // 论文：当任何一个每线程数据块满时就应触发转换
  for (size_t i = 0; i < max_threads_; ++i) {
    PerThreadDataBlock* pt_block = per_thread_blocks_[i].load(std::memory_order_acquire);
    if (pt_block && pt_block->IsFull()) {
      return true;
    }
  }

  // 可选：存在明显延迟数据也触发（最小键落在当前分段块范围左侧）
  const uint64_t seg_min = min_key_.load(std::memory_order_acquire);
  for (size_t i = 0; i < max_threads_; ++i) {
    PerThreadDataBlock* pt_block = per_thread_blocks_[i].load(std::memory_order_acquire);
    if (!pt_block) continue;
    const uint64_t pt_min = pt_block->GetMinKey();
    if (pt_min != kInvalidKey && seg_min != kInvalidKey && pt_min < seg_min) {
      return true;
    }
  }

  return false;
}

void SegmentedBlock::BeginWrite() {
  active_writers_.fetch_add(1, std::memory_order_acq_rel);
}

void SegmentedBlock::EndWrite() {
  active_writers_.fetch_sub(1, std::memory_order_acq_rel);
}

void SegmentedBlock::WaitForQuiescent() const {
  // 等待没有活跃写者
  while (active_writers_.load(std::memory_order_acquire) != 0) {
    std::this_thread::yield();
  }
}

void SegmentedBlock::MarkConversionTriggered() {
  const uint64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
  last_conversion_ns_.store(now, std::memory_order_release);
}
