#ifndef NARY_SEARCH_TABLE_H_
#define NARY_SEARCH_TABLE_H_

#include <vector>
#include <cstdint>
#include <algorithm>

/// N元搜索表（论文3.4节，引用1-87、1-88）
/// 优化：数据块拆分后实时更新，支持非满数据块的高效查找
class NArySearchTable {
 public:
  NArySearchTable(size_t data_block_cap, size_t bucket_size);

  /// 动态更新N元表（数据块插入时同步更新，而非仅满时构建）
  /// @param keys 数据块当前键数组（已排序）
  /// @param current_size 数据块当前元素数量
  void Update(const std::vector<uint64_t>& keys, size_t current_size);

  /// 查找目标键所在桶索引（论文3.4节：先查N元表再线性搜索）
  size_t FindBucketIndex(uint64_t key, size_t current_size) const;

  size_t GetBucketSize() const { return bucket_size_; }
  size_t GetNumBuckets(size_t current_size) const { 
    return (current_size + bucket_size_ - 1) / bucket_size_; 
  }

 private:
  std::vector<uint64_t> bucket_min_keys_;  // 每个桶的最小键
  const size_t bucket_size_;               // 桶大小（固定）
  const size_t max_buckets_;               // 最大桶数（数据块满时）
};

#endif  // NARY_SEARCH_TABLE_H_
