#ifndef SEARCH_NODE_H_
#define SEARCH_NODE_H_

#include <vector>
#include <memory>
#include <mutex>
#include <cstdint>
#include "data_block.h"
#include "constants.h"

// 前向声明
class BlockAllocator;

/// 搜索层节点（论文3.2节，引用1-67、1-119）
/// 扩展：支持节点分裂、ROWEX同步（读无锁，写单线程）
class SearchNode {
 public:
  enum NodeType { kLeafNode, kInternalNode };

  SearchNode(NodeType type, size_t capacity, BlockAllocator* allocator);

  /// 插入数据块（叶子节点，引用1-119）
  bool InsertDataBlock(uint64_t min_key, DataBlock* data_block);

  /// 插入子节点（内部节点，引用1-67）
  bool InsertChild(uint64_t max_key, std::unique_ptr<SearchNode> child);

  /// 分裂节点（论文3.2节：节点满时自底向上分裂）
  /// @param[out] new_node 分裂出的新节点
  /// @param[out] split_key 分裂键（父节点需插入）
  /// @return 分裂成功返回true
  bool Split(std::unique_ptr<SearchNode>* new_node, uint64_t* split_key);

  /// 查找数据块（叶子节点，读无锁，引用1-92）
  DataBlock* FindDataBlock(uint64_t key) const;

  /// 查找子节点（内部节点，读无锁，引用1-92）
  SearchNode* FindChild(uint64_t key) const;

  // 基础访问接口
  NodeType GetType() const { return type_; }
  const std::vector<uint64_t>& GetKeys() const { return keys_; }
  const std::vector<DataBlock*>& GetDataBlocks() const { return data_blocks_; }
  const std::vector<std::unique_ptr<SearchNode>>& GetChildren() const { return children_; }
  bool IsFull() const { return size_ >= capacity_; }
  uint64_t GetMaxKey() const { return (size_ > 0) ? keys_.back() : kInvalidKey; }

 private:
  const NodeType type_;                            // 节点类型
  const size_t capacity_;                          // 最大容量
  size_t size_;                                    // 当前元素数量
  std::vector<uint64_t> keys_;                     // 键列表（叶子：min_key；内部：max_key）
  std::vector<DataBlock*> data_blocks_;            // 叶子节点：数据块指针
  std::vector<std::unique_ptr<SearchNode>> children_;// 内部节点：子节点
  BlockAllocator* allocator_;                      // 块分配器
  mutable std::mutex write_mutex_;                 // 写锁（ROWEX：仅写线程持有，引用1-92）
};

#endif  // SEARCH_NODE_H_
