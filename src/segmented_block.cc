#include "segmented_block.h"
#include "block_allocator.h"
#include <algorithm>
#include <thread>
#include <chrono>
#include <iostream>

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
    std::cerr << "ERROR: thread_id " << thread_id << " >= max_threads " << max_threads_ << std::endl;
    return nullptr;
  }
  
  // 首先检查是否已经存在
  PerThreadDataBlock* existing = per_thread_blocks_[thread_id].load(std::memory_order_acquire);
  if (existing) {
    return existing;
  }
  
  // 创建新的PerThreadBlock
  PerThreadDataBlock* created = new PerThreadDataBlock(allocator_);
  
  // 尝试原子设置
  PerThreadDataBlock* expected = nullptr;
  if (per_thread_blocks_[thread_id].compare_exchange_strong(expected, created, std::memory_order_acq_rel, std::memory_order_acquire)) {
    return created;
  }
  
  // 其他线程已经设置了，删除我们创建的
  delete created;
  return expected;
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
  // 论文设计：当任何一个每线程数据块满时就应触发转换
  // 优化：基于实际使用的slot数量，而非固定80个
  
  // 统计实际使用的slot数量
  size_t active_slots = 0;
  bool has_full_block = false;
  
  for (size_t i = 0; i < max_threads_; ++i) {
    PerThreadDataBlock* pt_block = per_thread_blocks_[i].load(std::memory_order_acquire);
    if (pt_block) {
      active_slots++;
      if (pt_block->IsFull()) {
        has_full_block = true;
        break;
      }
    }
  }
  
  // 如果没有任何活跃slot，不需要转换
  if (active_slots == 0) {
    return false;
  }
  
  // 如果有块满了，立即转换
  if (has_full_block) {
    return true;
  }
  
  // 优化：如果活跃slot较少但数据量较大，也触发转换
  // 避免少数线程的数据长期不转换
  if (active_slots <= 4 && active_slots > 0) {
    // 计算总数据量
    size_t total_data = 0;
    for (size_t i = 0; i < max_threads_; ++i) {
      PerThreadDataBlock* pt_block = per_thread_blocks_[i].load(std::memory_order_acquire);
      if (pt_block) {
        total_data += pt_block->GetAllKv().size();
      }
    }
    
    // 如果总数据量超过阈值，触发转换
    constexpr size_t kConversionThreshold = 2048;  // 2K条记录
    if (total_data >= kConversionThreshold) {
      return true;
    }
  }

  return false;
}

bool SegmentedBlock::BeginWrite() {
  // 版本锁机制：检查当前分段块是否已被标记转换
  if (conversion_triggered_.load(std::memory_order_acquire)) {
    return false; // 旧分段块已标记转换，拒绝新写入
  }
  active_writers_.fetch_add(1, std::memory_order_acq_rel);
  return true; // 成功获取写锁
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
  // 修复P0-2：设置转换触发标记，禁止新写入
  conversion_triggered_.store(true, std::memory_order_release);
  
  const uint64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
  last_conversion_ns_.store(now, std::memory_order_release);
}

size_t SegmentedBlock::AllocateSlot() {
  // 修复：使用更安全的线程ID分配方式
  // 使用线程局部存储确保线程ID在有效范围内
  static std::atomic<size_t> slot_counter{0};
  thread_local size_t tls_slot = SIZE_MAX;
  
  if (tls_slot == SIZE_MAX) {
    // 首次分配：使用原子计数器分配slot
    tls_slot = slot_counter.fetch_add(1, std::memory_order_relaxed) % max_threads_;
  }
  
  // 验证slot在有效范围内
  if (tls_slot >= max_threads_) {
    std::cerr << "ERROR: Invalid slot " << tls_slot << " >= max_threads " << max_threads_ << std::endl;
    tls_slot = 0; // 回退到slot 0
  }
  
  return tls_slot;
}
