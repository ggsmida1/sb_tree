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
  // 使用写互斥锁保护并发写操作
  std::unique_lock<std::mutex> write_lock(write_mutex_);
  
  version_.WriteLock();  // 写锁：标记写入开始（引用1-93）
  // 使用RAII确保解锁
  struct WriteUnlocker {
    Version& version_;
    WriteUnlocker(Version& v) : version_(v) {}
    ~WriteUnlocker() { version_.WriteUnlock(); }
  } unlocker(version_);

  if (IsFull()) {
    // 块满：执行分裂（仅延迟数据插入时触发，引用1-120）
    if (!split_block) return -1;

    // 分裂：当前块保留前半，新块返回给调用者
    auto new_block = Split();
    if (!new_block) return -1;

    // 将当前插入路由到合适的块
    if (size_ > 0 && key <= keys_.back()) {
      // 插入当前块
      auto it = std::lower_bound(keys_.begin(), keys_.end(), key);
      size_t pos = std::distance(keys_.begin(), it);
      keys_.insert(it, key);
      values_.insert(values_.begin() + pos, value);
      size_++;
      // 降低N元表更新频率：仅在桶边界或桶数量变化时更新
      const size_t old_buckets = search_table_.GetNumBuckets(size_ - 1);
      const size_t new_buckets = search_table_.GetNumBuckets(size_);
      const bool bucket_boundary = (pos % search_table_.GetBucketSize()) == 0 || pos == 0;
      if (bucket_boundary || new_buckets != old_buckets) {
        search_table_.Update(keys_, size_);
      }
    } else {
      // 插入新块
      auto& nb = new_block;
      auto it = std::lower_bound(nb->keys_.begin(), nb->keys_.end(), key);
      size_t pos = std::distance(nb->keys_.begin(), it);
      nb->keys_.insert(it, key);
      nb->values_.insert(nb->values_.begin() + pos, value);
      nb->size_++;
      const size_t old_buckets = nb->search_table_.GetNumBuckets(nb->size_ - 1);
      const size_t new_buckets = nb->search_table_.GetNumBuckets(nb->size_);
      const bool bucket_boundary = (pos % nb->search_table_.GetBucketSize()) == 0 || pos == 0;
      if (bucket_boundary || new_buckets != old_buckets) {
        nb->search_table_.Update(nb->keys_, nb->size_);
      }
    }

    // 将新块所有权交给调用者
    *split_block = std::move(new_block);
    return 1;
  }

  // 正常插入：保持键有序（引用1-73）
  auto it = std::lower_bound(keys_.begin(), keys_.end(), key);
  const size_t pos = std::distance(keys_.begin(), it);
  const size_t old_size = size_;
  keys_.insert(it, key);
  values_.insert(values_.begin() + pos, value);
  size_ = old_size + 1;
  // 降低N元表更新频率：在桶边界或桶数量变化时更新
  const size_t old_buckets = search_table_.GetNumBuckets(old_size);
  const size_t new_buckets = search_table_.GetNumBuckets(size_);
  const bool bucket_boundary = (pos % search_table_.GetBucketSize()) == 0 || pos == 0;
  if (bucket_boundary || new_buckets != old_buckets) {
    search_table_.Update(keys_, size_);
  }
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

  // 注意：不在此处改动 next_block_，由调用者决定如何挂链
  return new_block;
}

void DataBlock::BulkFill(const std::vector<KeyValuePair>& kv, size_t start_idx, size_t end_idx) {
  if (start_idx >= end_idx) return;
  std::unique_lock<std::mutex> write_lock(write_mutex_);
  version_.WriteLock();
  struct WriteUnlocker {
    Version& version_;
    WriteUnlocker(Version& v) : version_(v) {}
    ~WriteUnlocker() { version_.WriteUnlock(); }
  } unlocker(version_);

  const size_t count = end_idx - start_idx;
  keys_.reserve(size_ + count);
  values_.reserve(size_ + count);
  for (size_t i = start_idx; i < end_idx; ++i) {
    keys_.push_back(kv[i].key);
    values_.push_back(kv[i].value);
  }
  size_ += count;
  search_table_.Update(keys_, size_);
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
