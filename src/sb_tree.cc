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
  current_segmented_block_.store(new SegmentedBlock(kSegmentedBlockMaxThreads, &allocator_), std::memory_order_release);
}

SBTree::~SBTree() {
  // 组件自动析构（converter_会停止转换线程）
  SegmentedBlock* seg = current_segmented_block_.load(std::memory_order_acquire);
  if (seg) {
    delete seg;
  }
}

bool SBTree::IsDelayedData(uint64_t key) const {
  // 延迟数据判断（论文4.4节）：
  // 1. key < 当前分段块的最小键；2. key明显小于当前全局最大键
  SegmentedBlock* seg = current_segmented_block_.load(std::memory_order_acquire);
  const uint64_t seg_min = seg ? seg->GetMinKey() : kInvalidKey;
  const uint64_t global_max = current_max_key_.load(std::memory_order_acquire);
  
  // 如果分段块已有数据且key小于最小键，则为延迟数据
  if (seg_min != kInvalidKey && key < seg_min) {
    return true;
  }
  
  // 如果key明显小于全局最大键，则为延迟数据（使用分段块最小键作为参考）
  if (global_max > 0 && seg_min != kInvalidKey) {
    // 使用分段块最小键而非硬编码的1000
    const uint64_t threshold = (global_max > seg_min) ? seg_min : (global_max - 1000);
    return key < threshold;
  }
  
  return false;
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
  // 使用循环替代递归重试，避免栈溢出
  while (true) {
    // 每轮重新读取当前分段块（原子加载，确保看到最新的分段块）
    SegmentedBlock* seg = current_segmented_block_.load(std::memory_order_acquire);
    if (!seg) {
      std::this_thread::yield();
      continue;
    }

    // 获取当前线程的PerThreadBlock（引用1-71）
    const size_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id()) % 
                            kSegmentedBlockMaxThreads;
    seg->BeginWrite();
    PerThreadDataBlock* pt_block = seg->AllocatePerThreadBlock(thread_id);
    if (!pt_block) return false;
    // 再次确认seg仍为当前分段块且slot未被替换
    if (seg != current_segmented_block_.load(std::memory_order_acquire)) {
      seg->EndWrite();
      std::this_thread::yield();
      continue;
    }
    if (seg->LoadPerThreadBlock(thread_id) != pt_block) {
      seg->EndWrite();
      std::this_thread::yield();
      continue;
    }

    // 3. 插入PerThreadBlock（引用1-103）
    if (pt_block->Insert(key, value)) {
      // 更新分段块的键范围
      seg->UpdateKeyRange(key);
      
      // 4. 检查分段块是否需要转换（引用1-105）
      if (seg->NeedConversion(current_max_key_.load(std::memory_order_acquire))) {
        // 创建新分段块并原子替换（引用P0）
        seg->EndWrite();
        SegmentedBlock* new_seg = new SegmentedBlock(kSegmentedBlockMaxThreads, &allocator_);
        SegmentedBlock* old_seg = current_segmented_block_.exchange(new_seg, std::memory_order_acq_rel);
        // 提交转换任务（异步，引用1-107）
        converter_.SubmitConversionTask(std::unique_ptr<SegmentedBlock>(old_seg));
      }
      else {
        seg->EndWrite();
      }
      return true;
    }

    // 5. PerThreadBlock满：提交转换任务并在下次循环中重试（引用1-105）
    seg->EndWrite();
    {
      SegmentedBlock* new_seg = new SegmentedBlock(kSegmentedBlockMaxThreads, &allocator_);
      SegmentedBlock* old_seg = current_segmented_block_.exchange(new_seg, std::memory_order_acq_rel);
      converter_.SubmitConversionTask(std::unique_ptr<SegmentedBlock>(old_seg));
    }
    // 继续循环，重试插入新分段块
  }
}

bool SBTree::InsertDelayedData(uint64_t key, uint64_t value) {
  // 1. 遍历搜索层找目标数据块（引用1-120）
  SearchNode* current_node = root_.get();
  while (current_node && current_node->GetType() != SearchNode::kLeafNode) {
    current_node = current_node->FindChild(key);
  }

  // 2. 在搜索层叶子节点中查找目标数据块
  DataBlock* target_block = nullptr;
  if (current_node) {
    target_block = current_node->FindDataBlock(key);
  }
  
  // 3. 如果搜索层未找到，遍历数据层链表（支持多级搜索层）
  if (!target_block) {
    // 收集所有叶子节点中的数据块
    std::vector<DataBlock*> all_data_blocks;
    CollectAllDataBlocks(root_.get(), &all_data_blocks);
    
    // 遍历数据块链表
    for (DataBlock* db : all_data_blocks) {
      DataBlock* curr = db;
      while (curr) {
        if (key >= curr->GetMinKey() && key <= curr->GetMaxKey()) {
          target_block = curr;
          break;
        }
        curr = curr->GetNextBlock().get();
      }
      if (target_block) {
        break;
      }
    }
  }
  
  if (!target_block) {
    return false;  // 未找到目标块（数据不存在）
  }

  // 4. 插入延迟数据（可能触发块分裂，引用1-120）
  std::unique_ptr<DataBlock> split_block;
  const int insert_res = target_block->Insert(key, value, &split_block);
  if (insert_res == -1) {
    return false;
  }

  // 5. 若分裂，将新块插入搜索层（引用1-120）
  if (split_block && insert_res == 1) {
    return InsertDataBlockToSearchLayer(split_block.release());
  }
  return true;
}

void SBTree::CollectAllDataBlocks(SearchNode* node, std::vector<DataBlock*>* result) const {
  if (!node || !result) {
    return;
  }
  
  if (node->GetType() == SearchNode::kLeafNode) {
    // 叶子节点：收集所有数据块
    const auto& data_blocks = node->GetDataBlocks();
    for (DataBlock* db : data_blocks) {
      result->push_back(db);
    }
  } else {
    // 内部节点：递归收集所有子节点的数据块
    const auto& children = node->GetChildren();
    for (const auto& child : children) {
      CollectAllDataBlocks(child.get(), result);
    }
  }
}

bool SBTree::InsertDataBlockToSearchLayer(DataBlock* data_block) {
  if (!data_block) {
    return false;
  }
  std::lock_guard<std::mutex> lock(search_layer_write_mutex_);  // ROWEX：单写线程（引用1-92）

  uint64_t insert_key = data_block->GetMinKey();
  
  // 如果根节点是叶子节点，直接处理
  if (root_->GetType() == SearchNode::kLeafNode) {
    if (root_->InsertDataBlock(insert_key, data_block)) {
      return true;
    }
    
    // 根节点满，需要分裂
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
    
    // 重新尝试插入到新的树结构
    return InsertDataBlockToSearchLayerRecursive(root_.get(), insert_key, data_block);
  }
  
  // 内部节点：递归插入
  return InsertDataBlockToSearchLayerRecursive(root_.get(), insert_key, data_block);
}

bool SBTree::InsertDataBlockToSearchLayerRecursive(SearchNode* node, uint64_t key, DataBlock* data_block) {
  if (!node || !data_block) {
    return false;
  }
  
  // 如果是叶子节点，直接插入数据块
  if (node->GetType() == SearchNode::kLeafNode) {
    if (node->InsertDataBlock(key, data_block)) {
      return true;
    }
    // 叶子节点满了，返回false表示需要父节点处理分裂
    return false;
  }
  
  // 内部节点：找到目标子节点
  SearchNode* child = node->FindChild(key);
  if (!child) {
    // 如果找不到合适的子节点，插入到第一个子节点（最左侧）
    const auto& children = node->GetChildren();
    if (children.empty()) {
      return false;
    }
    child = children[0].get();
  }
  
  // 递归插入到子节点
  if (InsertDataBlockToSearchLayerRecursive(child, key, data_block)) {
    return true;
  }
  
  // 子节点满了，需要分裂
  std::unique_ptr<SearchNode> new_child_node;
  uint64_t split_key;
  if (!child->Split(&new_child_node, &split_key)) {
    return false;
  }
  
  // 将分裂出的新节点插入到当前节点
  if (node->InsertChild(new_child_node->GetMaxKey(), std::move(new_child_node))) {
    // 成功插入新子节点，重试原始插入
    return InsertDataBlockToSearchLayerRecursive(node, key, data_block);
  }
  
  // 当前节点也满了，返回false让上层处理
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

bool SBTree::LookupInSegmentedBlock(uint64_t key, uint64_t* out_value) const {
  SegmentedBlock* seg = current_segmented_block_.load(std::memory_order_acquire);
  if (!seg) return false;
  const size_t max_threads = seg->GetMaxThreads();
  for (size_t i = 0; i < max_threads; ++i) {
    PerThreadDataBlock* pt_block = seg->LoadPerThreadBlock(i);
    if (!pt_block) continue;
    const auto& kv_pairs = pt_block->GetAllKv();
    for (const auto& kv : kv_pairs) {
      if (kv.key == key) {
        *out_value = kv.value;
        return true;
      }
    }
  }
  return false;
}

const uint64_t* SBTree::Lookup(uint64_t key) const {
  // 1. 首先检查当前分段块中的 PerThreadBlock（最新数据可能还未转换）
  static thread_local uint64_t thread_local_value;
  if (LookupInSegmentedBlock(key, &thread_local_value)) {
    return &thread_local_value;
  }

  // 2. 遍历搜索层找数据块（引用1-124）
  SearchNode* current_node = root_.get();
  while (current_node && current_node->GetType() != SearchNode::kLeafNode) {
    current_node = current_node->FindChild(key);
  }

  // 3. 数据块内查找（引用1-124）
  DataBlock* data_block = nullptr;
  if (current_node) {
    data_block = current_node->FindDataBlock(key);
  }
  
  if (!data_block) {
    // 搜索层未找到，遍历数据层（引用1-119）
    std::vector<DataBlock*> all_data_blocks;
    CollectAllDataBlocks(root_.get(), &all_data_blocks);
    
    for (DataBlock* db : all_data_blocks) {
      if (db->GetMinKey() <= key && key <= db->GetMaxKey()) {
        data_block = db;
        break;
      }
    }
    
    if (!data_block) {
      return nullptr;
    }
  }

  // 4. 带版本一致性检查的查找（引用1-93）
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
