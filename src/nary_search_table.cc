#include "nary_search_table.h"
#include <algorithm>

// -----------------------------------------------------------------------------
// NArySearchTable 实现（论文3.4节，引用1-87、1-88）
// -----------------------------------------------------------------------------
NArySearchTable::NArySearchTable(size_t data_block_cap, size_t bucket_size)
    : bucket_size_(bucket_size),
      max_buckets_((data_block_cap + bucket_size - 1) / bucket_size) {
  bucket_min_keys_.reserve(max_buckets_);
}

void NArySearchTable::Update(const std::vector<uint64_t>& keys, size_t current_size) {
  bucket_min_keys_.clear();
  const size_t num_buckets = GetNumBuckets(current_size);
  for (size_t i = 0; i < num_buckets; ++i) {
    const size_t bucket_start = i * bucket_size_;
    if (bucket_start >= current_size) break;
    bucket_min_keys_.push_back(keys[bucket_start]);
  }
}

size_t NArySearchTable::FindBucketIndex(uint64_t key, size_t current_size) const {
  if (current_size == 0 || bucket_min_keys_.empty()) return 0;
  const size_t num_buckets = GetNumBuckets(current_size);
  // 二分查找桶（bucket_min_keys_已排序，引用1-88）
  auto it = std::upper_bound(bucket_min_keys_.begin(), bucket_min_keys_.end(), key);
  if (it == bucket_min_keys_.begin()) return 0;
  size_t idx = std::distance(bucket_min_keys_.begin(), it) - 1;
  return std::min(idx, num_buckets - 1);  // 防止越界
}
