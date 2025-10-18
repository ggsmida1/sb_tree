#include "sb_tree.h"
#include "block_allocator.h"
#include "segmented_block.h"
#include "segmented_block_converter.h"
#include "search_node.h"
#include "data_block.h"
#include <thread>
#include <functional>
#include <algorithm>

// 线程局部存储初始化（如果需要的话）

// -----------------------------------------------------------------------------
// SBTree 实现（论文3.1节、4节，引用1-23、1-61）
// -----------------------------------------------------------------------------
SBTree::SBTree()
    : allocator_(kBlockSize),
      converter_(this, &allocator_),
      current_max_key_(0) {
  // 初始化搜索层根节点（叶子节点，引用1-67）
  root_ = std::make_unique<SearchNode>(SearchNode::kLeafNode, kSearchNodeCapacity, &allocator_);
  // 初始化第一个分段块（引用1-65）
  current_segmented_block_ = std::make_unique<SegmentedBlock>(kSegmentedBlockMaxThreads, &allocator_);
}

SBTree::~SBTree() {
  // 组件自动析构（converter_会停止转换线程）
}

bool SBTree::IsDelayedData(uint64_t key) const {
  // 延迟数据判断（论文4.4节）：
  // 1. key < 当前分段块的最小键；2. key < 当前全局最大键（排除新数据）
  const uint64_t seg_min = current_segmented_block_->GetMinKey();
  const uint64_t global_max = current_max_key_.load(std::memory_order_acquire);
  // 简化判断：只有当key明显小于当前最大键时才认为是延迟数据
  return (global_max > 0 && key < global_max - 1000);  // 给一个缓冲区间
}

bool SBTree::Insert(uint64_t key, uint64_t value) {
  // 更新全局最大键（原子操作，引用1-103）
  uint64_t old_max = current_max_key_.load(std::memory_order_acquire);
  while (key > old_max && !current_max_key_.compare_exchange_weak(
      old_max, key, std::memory_order_acq_rel, std::memory_order_acquire)) {
    // 自旋等待直到更新成功
  }

  // 1. 判断是否为延迟数据（引用1-120）
  if (IsDelayedData(key)) {
    return InsertDelayedData(key, value);
  }

  // 2. 非延迟数据：通过shortcut插入分段块（引用1-69）
  std::unique_lock<std::mutex> seg_lock(segmented_block_mutex_, std::try_to_lock);
  if (!seg_lock.owns_lock()) {
    // 分段块切换中，重试（短期阻塞）
    std::this_thread::yield();
    return Insert(key, value);
  }

  // 获取当前线程的PerThreadBlock（引用1-71）
  const size_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id()) % 
                          kSegmentedBlockMaxThreads;
  PerThreadDataBlock* pt_block = current_segmented_block_->AllocatePerThreadBlock(thread_id);
  if (!pt_block) {
    // 线程已分配块，直接获取
    const auto& pt_blocks = current_segmented_block_->GetAllPerThreadBlocks();
    if (thread_id < pt_blocks.size() && pt_blocks[thread_id]) {
      pt_block = pt_blocks[thread_id].get();
    }
    if (!pt_block) {
      return false;
    }
  }

  // 3. 插入PerThreadBlock（引用1-103）
  if (pt_block->Insert(key, value)) {
    // 4. 检查分段块是否需要转换（引用1-105）
    if (current_segmented_block_->NeedConversion(current_max_key_)) {
      // 创建新分段块替换当前（引用1-107）
      auto new_seg_block = std::make_unique<SegmentedBlock>(kSegmentedBlockMaxThreads, &allocator_);
      auto old_seg_block = std::move(current_segmented_block_);
      current_segmented_block_ = std::move(new_seg_block);
      // 提交转换任务（异步，引用1-107）
      converter_.SubmitConversionTask(std::move(old_seg_block));
    }
    return true;
  }

  // 5. PerThreadBlock满：提交转换任务并重试（引用1-105）
  auto new_seg_block = std::make_unique<SegmentedBlock>(kSegmentedBlockMaxThreads, &allocator_);
  auto old_seg_block = std::move(current_segmented_block_);
  current_segmented_block_ = std::move(new_seg_block);
  converter_.SubmitConversionTask(std::move(old_seg_block));
  return Insert(key, value);  // 重试插入新分段块
}

bool SBTree::InsertDelayedData(uint64_t key, uint64_t value) {
  // 1. 遍历搜索层找目标数据块（引用1-120）
  SearchNode* current_node = root_.get();
  while (current_node->GetType() != SearchNode::kLeafNode) {
    current_node = current_node->FindChild(key);
    if (!current_node) {
      break;  // 搜索层未找到，遍历数据层
    }
  }

  // 2. 遍历数据层找目标块（引用1-120）
  DataBlock* target_block = nullptr;
  if (current_node) {
    target_block = current_node->FindDataBlock(key);
  }
  if (!target_block) {
    // 搜索层未找到，从数据层头部开始遍历（链表）
    // 注：实际需维护数据层头部指针，此处简化为从搜索层根节点叶子块开始
    if (root_->GetType() == SearchNode::kLeafNode) {
      const auto& data_blocks = root_->GetDataBlocks();
      if (!data_blocks.empty()) {
        DataBlock* curr = data_blocks[0];
        while (curr) {
          if (key >= curr->GetMinKey() && key <= curr->GetMaxKey()) {
            target_block = curr;
            break;
          }
          curr = curr->GetNextBlock().get();
        }
      }
    }
  }
  if (!target_block) {
    return false;  // 未找到目标块（数据不存在）
  }

  // 3. 插入延迟数据（可能触发块分裂，引用1-120）
  std::unique_ptr<DataBlock> split_block;
  const int insert_res = target_block->Insert(key, value, &split_block);
  if (insert_res == -1) {
    return false;
  }

  // 4. 若分裂，将新块插入搜索层（引用1-120）
  if (split_block && insert_res == 1) {
    return InsertDataBlockToSearchLayer(split_block.release());
  }
  return true;
}

bool SBTree::InsertDataBlockToSearchLayer(DataBlock* data_block) {
  if (!data_block) {
    return false;
  }
  std::lock_guard<std::mutex> lock(search_layer_write_mutex_);  // ROWEX：单写线程（引用1-92）

  // 简化实现：直接插入到根节点（如果是叶子节点）
  if (root_->GetType() == SearchNode::kLeafNode) {
    if (root_->InsertDataBlock(data_block->GetMinKey(), data_block)) {
      return true;
    }
    
    // 根节点满，分裂
    std::unique_ptr<SearchNode> new_node;
    uint64_t split_key;
    if (!root_->Split(&new_node, &split_key)) {
      return false;
    }
    
    // 创建新根节点
    auto new_root = std::make_unique<SearchNode>(SearchNode::kInternalNode, kSearchNodeCapacity, &allocator_);
    new_root->InsertChild(root_->GetMaxKey(), std::move(root_));
    new_root->InsertChild(new_node->GetMaxKey(), std::move(new_node));
    root_ = std::move(new_root);
    
    // 重新尝试插入
    return root_->InsertDataBlock(data_block->GetMinKey(), data_block);
  }
  
  // 内部节点：简化处理，直接插入到最右侧
  // 这里需要更复杂的实现，暂时返回false
  return false;
}

bool SBTree::HandleSearchNodeSplit(std::unique_ptr<SearchNode>* parent_node, 
                                   size_t child_idx, 
                                   std::unique_ptr<SearchNode> new_child, 
                                   uint64_t split_key) {
  if (!parent_node || !(*parent_node) || !new_child) {
    return false;
  }

  // 插入新子节点到父节点（引用1-67）
  if ((*parent_node)->InsertChild(new_child->GetMaxKey(), std::move(new_child))) {
    return true;
  }

  // 父节点满，触发分裂（递归处理）
  std::unique_ptr<SearchNode> grandchild_node;
  uint64_t grand_split_key;
  if (!(*parent_node)->Split(&grandchild_node, &grand_split_key)) {
    return false;
  }
  return HandleSearchNodeSplit(parent_node, child_idx, std::move(grandchild_node), grand_split_key);
}

void SBTree::UpdateSearchLayerWithDataBlocks(std::vector<std::unique_ptr<DataBlock>> data_blocks) {
  if (data_blocks.empty()) {
    return;
  }
  // 插入第一个数据块（后续数据块通过链表关联，无需重复插入）
  InsertDataBlockToSearchLayer(data_blocks[0].release());
}

const uint64_t* SBTree::Lookup(uint64_t key) const {
  // 1. 遍历搜索层找数据块（引用1-124）
  SearchNode* current_node = root_.get();
  while (current_node->GetType() != SearchNode::kLeafNode) {
    current_node = current_node->FindChild(key);
    if (!current_node) {
      return nullptr;
    }
  }

  // 2. 数据块内查找（引用1-124）
  DataBlock* data_block = current_node->FindDataBlock(key);
  if (!data_block) {
    // 搜索层未找到，遍历数据层（引用1-119）
    if (root_->GetType() == SearchNode::kLeafNode) {
      const auto& data_blocks = root_->GetDataBlocks();
      for (DataBlock* db : data_blocks) {
        if (db->GetMinKey() <= key && key <= db->GetMaxKey()) {
          data_block = db;
          break;
        }
      }
    }
    if (!data_block) {
      return nullptr;
    }
  }

  // 3. 带版本一致性检查的查找（引用1-93）
  const uint64_t* value = nullptr;
  const uint32_t max_retries = 3;  // 最大重试次数
  for (uint32_t i = 0; i < max_retries; ++i) {
    value = data_block->Lookup(key);
    if (value) {
      break;
    }
    std::this_thread::yield();  // 版本不一致，重试
  }
  return value;
}

size_t SBTree::Scan(uint64_t start_key, size_t count, 
                    std::vector<KeyValuePair>* result) const {
  if (count == 0 || !result) {
    return 0;
  }

  // 1. 找到起始数据块（引用1-128）
  SearchNode* current_node = root_.get();
  while (current_node->GetType() != SearchNode::kLeafNode) {
    current_node = current_node->FindChild(start_key);
    if (!current_node) {
      break;
    }
  }

  DataBlock* start_block = nullptr;
  if (current_node) {
    start_block = current_node->FindDataBlock(start_key);
  }
  if (!start_block) {
    // 搜索层未找到，遍历数据层（引用1-119）
    if (root_->GetType() == SearchNode::kLeafNode) {
      const auto& data_blocks = root_->GetDataBlocks();
      for (DataBlock* db : data_blocks) {
        if (db->GetMaxKey() >= start_key) {
          start_block = db;
          break;
        }
      }
    }
    if (!start_block) {
      return 0;
    }
  }

  // 2. 带版本一致性检查的扫描（引用1-93、1-128）
  size_t total_scanned = 0;
  const uint32_t max_retries = 3;
  for (uint32_t i = 0; i < max_retries; ++i) {
    result->clear();
    total_scanned = start_block->Scan(start_key, count, result);
    if (total_scanned > 0 || result->empty()) {
      break;
    }
    std::this_thread::yield();  // 版本不一致，重试
  }

  return total_scanned;
}
