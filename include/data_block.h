#ifndef DATA_BLOCK_H_
#define DATA_BLOCK_H_

#include <vector>
#include <memory>
#include <mutex>
#include <cstdint>
#include "version.h"
#include "nary_search_table.h"
#include "constants.h"

// 前向声明
class BlockAllocator;

// 键值对结构体（论文4.1节：每线程数据块存储格式）
struct KeyValuePair {
  uint64_t key;    // 8字节键：6字节时间戳 + 2字节传感器ID（引用1-139）
  uint64_t value;  // 8字节值
};

/// 数据块（论文3.3节、4.4节，引用1-73、1-120）
/// 扩展：支持延迟数据插入、块分裂、版本化同步
class DataBlock {
 public:
  explicit DataBlock(BlockAllocator* allocator);

  /// 插入键值对（支持延迟数据：保持键有序，块满则返回需分裂）
  /// @param key 键（可能小于当前块最大键）
  /// @param value 值
  /// @param[out] split_block 分裂出的新块（若分裂）
  /// @return 0：成功，1：需分裂，-1：失败
  int Insert(uint64_t key, uint64_t value, std::unique_ptr<DataBlock>* split_block);

  /// 查找键（带版本一致性检查，引用1-93）
  const uint64_t* Lookup(uint64_t key) const;

  /// 扫描操作（论文4.5节，引用1-128）
  /// 扩展：支持跨块扫描时的版本一致性
  size_t Scan(uint64_t start_key, size_t count, 
              std::vector<KeyValuePair>* result) const;

  // 基础访问接口
  uint64_t GetMinKey() const { return (size_ > 0) ? keys_[0] : kInvalidKey; }
  uint64_t GetMaxKey() const { return (size_ > 0) ? keys_.back() : kInvalidKey; }
  std::unique_ptr<DataBlock>& GetNextBlock() { return next_block_; }
  void SetNextBlock(std::unique_ptr<DataBlock> next) { next_block_ = std::move(next); }
  bool IsFull() const { return size_ >= kDataBlockCapacity; }
  size_t GetSize() const { return size_; }
  Version& GetVersion() { return version_; }

 private:
  /// 分裂数据块（论文4.4节：延迟数据导致块满时分裂）
  /// @return 分裂出的新块（存储后半部分键值对）
  std::unique_ptr<DataBlock> Split();

  std::vector<uint64_t> keys_;               // 键数组（始终有序，引用1-73）
  std::vector<uint64_t> values_;           // 值数组（与键一一对应，引用1-73）
  NArySearchTable search_table_;            // N元搜索表（引用1-87）
  std::unique_ptr<DataBlock> next_block_;   // 下一个数据块（链表，引用1-65）
  size_t size_;                             // 当前元素数量
  BlockAllocator* allocator_;               // 块分配器（引用1-89）
  Version version_;                         // 版本锁（引用1-93）
  mutable std::mutex write_mutex_;          // 写互斥锁（保护并发写操作）
  mutable std::mutex split_mutex_;          // 分裂互斥锁（避免并发分裂）
};

#endif  // DATA_BLOCK_H_
