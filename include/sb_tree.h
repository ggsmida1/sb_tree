#ifndef SB_TREE_H_
#define SB_TREE_H_

#include <memory>
#include <mutex>
#include <atomic>
#include <cstdint>
#include <vector>
#include "data_block.h"
#include "search_node.h"
#include "block_allocator.h"
#include "segmented_block.h"
#include "segmented_block_converter.h"
#include "constants.h"

/// SB-Tree 主类（论文3.1节、4节，引用1-23、1-61）
/// 扩展：完整支持延迟数据、ROWEX同步、异步转换
class SBTree {
 public:
  SBTree();
  ~SBTree();

  bool Insert(uint64_t key, uint64_t value);
  const uint64_t* Lookup(uint64_t key) const;
  size_t Scan(uint64_t start_key, size_t count, 
              std::vector<KeyValuePair>* result) const;

  /// 供转换线程调用：更新搜索层（论文4.3节，引用1-119）
  void UpdateSearchLayerWithDataBlocks(std::vector<std::unique_ptr<DataBlock>> data_blocks);

  BlockAllocator* GetAllocator() { return &allocator_; }
  uint64_t GetCurrentMaxKey() const { return current_max_key_.load(std::memory_order_acquire); }

  /// 在当前分段块中查找键值（用于查找未转换的新数据）
  bool LookupInSegmentedBlock(uint64_t key, uint64_t* out_value) const;

 private:
  /// 判断是否为延迟数据（论文4.4节：key < 当前分段块最小键 或 key < 当前全局最大键）
  bool IsDelayedData(uint64_t key) const;

  /// 延迟数据插入逻辑（论文4.4节：遍历搜索层+数据层找目标块）
  bool InsertDelayedData(uint64_t key, uint64_t value);

  /// 搜索层插入数据块（支持节点分裂，自底向上更新，引用1-67）
  bool InsertDataBlockToSearchLayer(DataBlock* data_block);

  /// 搜索层递归插入数据块（支持内部节点和叶子节点）
  bool InsertDataBlockToSearchLayerRecursive(SearchNode* node, uint64_t key, DataBlock* data_block);

  /// 收集搜索层所有叶子节点中的数据块（用于延迟数据插入）
  void CollectAllDataBlocks(SearchNode* node, std::vector<DataBlock*>* result) const;

  /// 搜索层节点分裂处理（递归更新父节点，引用1-67）
  bool HandleSearchNodeSplit(std::unique_ptr<SearchNode>* parent_node, 
                             size_t child_idx, 
                             std::unique_ptr<SearchNode> new_child, 
                             uint64_t split_key);

  // 核心组件
  std::unique_ptr<SearchNode> root_;                          // 搜索层根节点（引用1-67）
  std::atomic<SegmentedBlock*> current_segmented_block_;      // 当前活跃分段块（原子指针，引用P0）
  BlockAllocator allocator_;                                  // 块分配器（引用1-89）
  SegmentedBlockConverter converter_;                         // 分段块转换器（引用1-107）
  std::atomic<uint64_t> current_max_key_;                     // 当前全局最大键（原子更新）
  mutable std::mutex segmented_block_mutex_;                  // 分段块切换锁（不再用于fast-path）
  mutable std::mutex search_layer_write_mutex_;               // 搜索层写锁（ROWEX：单写线程，引用1-92）
};

#endif  // SB_TREE_H_
