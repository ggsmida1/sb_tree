#include "data_block.h"
#include "block_allocator.h"
#include <algorithm>
#include <thread>

// -----------------------------------------------------------------------------
// DataBlock 实现（论文3.3节、4.4节，引用1-73、1-120）
// -----------------------------------------------------------------------------
DataBlock::DataBlock(BlockAllocator* allocator)
    : search_table_(kDataBlockCapacity, kNAryBucketSize),
      size_(0),
      allocator_(allocator) {
  keys_.reserve(kDataBlockCapacity);
  values_.reserve(kDataBlockCapacity);
}

int DataBlock::Insert(uint64_t key, uint64_t value, std::unique_ptr<DataBlock>* split_block) {
  version_.WriteLock();  // 写锁：标记写入开始（引用1-93）
  // 使用RAII确保解锁
  struct WriteUnlocker {
    Version& version_;
    WriteUnlocker(Version& v) : version_(v) {}
    ~WriteUnlocker() { version_.WriteUnlock(); }
  } unlocker(version_);

  if (IsFull()) {
    // 块满：尝试分裂（仅延迟数据插入时触发，引用1-120）
    std::lock_guard<std::mutex> lock(split_mutex_);
    if (IsFull()) {
      *split_block = Split();
      // 判断key应插入当前块还是新块
      if (key <= keys_.back()) {
        // 插入当前块（前半部分）
        auto it = std::lower_bound(keys_.begin(), keys_.end(), key);
        size_t pos = std::distance(keys_.begin(), it);
        keys_.insert(it, key);
        values_.insert(values_.begin() + pos, value);
        size_++;
        search_table_.Update(keys_, size_);
        return 0;
      } else {
        // 插入新块（后半部分）
        if ((*split_block)->Insert(key, value, nullptr) != 0) {
          return -1;  // 新块也满（理论上不会发生）
        }
        return 1;
      }
    }
  }

  // 正常插入：保持键有序（引用1-73）
  auto it = std::lower_bound(keys_.begin(), keys_.end(), key);
  size_t pos = std::distance(keys_.begin(), it);
  keys_.insert(it, key);
  values_.insert(values_.begin() + pos, value);
  size_++;
  search_table_.Update(keys_, size_);  // 实时更新N元表
  return 0;
}

std::unique_ptr<DataBlock> DataBlock::Split() {
  auto new_block = std::make_unique<DataBlock>(allocator_);
  const size_t split_pos = size_ / 2;

  // 拆分键值对（前半部分保留，后半部分移到新块）
  new_block->keys_.assign(keys_.begin() + split_pos, keys_.end());
  new_block->values_.assign(values_.begin() + split_pos, values_.end());
  new_block->size_ = size_ - split_pos;
  keys_.erase(keys_.begin() + split_pos, keys_.end());
  values_.erase(values_.begin() + split_pos, values_.end());
  size_ = split_pos;

  // 更新两个块的N元表
  search_table_.Update(keys_, size_);
  new_block->search_table_.Update(new_block->keys_, new_block->size_);

  // 链接新块（当前块的next指向新块，新块继承当前块的next）
  new_block->SetNextBlock(std::move(next_block_));
  next_block_ = std::move(new_block);
  return std::move(next_block_);  // 返回新块（所有权转移）
}

const uint64_t* DataBlock::Lookup(uint64_t key) const {
  const uint32_t start_version = version_.ReadLock();  // 读锁：获取稳定版本
  
  // 验证版本一致性（避免读取过程中数据被修改，引用1-93）
  if (!version_.IsConsistent(start_version)) {
    // 版本不一致，返回nullptr（上层需重试）
    return nullptr;
  }

  if (size_ == 0 || key < keys_[0] || key > keys_.back()) {
    return nullptr;
  }

  // N元表定位桶，再线性搜索（引用1-88）
  const size_t bucket_idx = search_table_.FindBucketIndex(key, size_);
  const size_t bucket_start = bucket_idx * search_table_.GetBucketSize();
  const size_t bucket_end = std::min(
      bucket_start + search_table_.GetBucketSize(), size_);

  for (size_t i = bucket_start; i < bucket_end; ++i) {
    if (keys_[i] == key) {
      return &values_[i];
    } else if (keys_[i] > key) {
      break;
    }
  }
  return nullptr;
}

size_t DataBlock::Scan(uint64_t start_key, size_t count, 
                       std::vector<KeyValuePair>* result) const {
  if (size_ == 0 || count == 0 || result == nullptr) {
    return 0;
  }

  const uint32_t start_version = version_.ReadLock();
  
  if (!version_.IsConsistent(start_version)) {
    return 0;  // 版本不一致，返回0（上层需重试）
  }

  // 找到起始位置（引用1-128）
  auto it = std::lower_bound(keys_.begin(), keys_.end(), start_key);
  const size_t start_pos = std::distance(keys_.begin(), it);
  const size_t remaining = size_ - start_pos;
  const size_t take = std::min(remaining, count);

  // 填充结果
  for (size_t i = 0; i < take; ++i) {
    result->emplace_back(KeyValuePair{keys_[start_pos + i], values_[start_pos + i]});
  }

  // 跨块扫描（引用1-128）
  if (take < count && next_block_ != nullptr) {
    return take + next_block_->Scan(keys_.back() + 1, count - take, result);
  }

  return take;
}
