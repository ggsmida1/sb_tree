#include "search_node.h"
#include "block_allocator.h"
#include <algorithm>

// -----------------------------------------------------------------------------
// SearchNode 实现（论文3.2节，引用1-67、1-119）
// -----------------------------------------------------------------------------
SearchNode::SearchNode(NodeType type, size_t capacity, BlockAllocator* allocator)
    : type_(type),
      capacity_(capacity),
      size_(0),
      allocator_(allocator) {
  keys_.reserve(capacity);
  if (type == kLeafNode) {
    data_blocks_.reserve(capacity);
  } else {
    children_.reserve(capacity);
  }
}

bool SearchNode::InsertDataBlock(uint64_t min_key, DataBlock* data_block) {
  if (type_ != kLeafNode) {
    return false;
  }
  std::unique_lock<std::shared_mutex> lock(rw_mutex_);  // 独占写锁
  
  if (IsFull()) {
    return false;  // 需分裂，上层处理
  }

  // 按min_key有序插入（引用1-119）
  auto it = std::lower_bound(keys_.begin(), keys_.end(), min_key);
  const size_t pos = std::distance(keys_.begin(), it);
  keys_.insert(it, min_key);
  data_blocks_.insert(data_blocks_.begin() + pos, data_block);
  size_++;
  return true;
}

bool SearchNode::InsertChild(uint64_t max_key, std::unique_ptr<SearchNode> child) {
  if (type_ != kInternalNode) {
    return false;
  }
  std::unique_lock<std::shared_mutex> lock(rw_mutex_);  // 独占写锁
  
  if (IsFull()) {
    return false;  // 需分裂
  }

  // 按max_key有序插入（引用1-67）
  auto it = std::lower_bound(keys_.begin(), keys_.end(), max_key);
  const size_t pos = std::distance(keys_.begin(), it);
  keys_.insert(it, max_key);
  children_.insert(children_.begin() + pos, std::move(child));
  size_++;
  return true;
}

bool SearchNode::Split(std::unique_ptr<SearchNode>* new_node, uint64_t* split_key) {
  if (!IsFull() || new_node == nullptr || split_key == nullptr) {
    return false;
  }
  std::unique_lock<std::shared_mutex> lock(rw_mutex_);  // 独占写锁
  
  if (!IsFull()) {
    return false;
  }

  // 创建新节点（同类型、同容量）
  *new_node = std::make_unique<SearchNode>(type_, capacity_, allocator_);
  const size_t split_pos = size_ / 2;

  if (type_ == kLeafNode) {
    // 叶子节点分裂：拆分min_key和数据块指针（引用1-67）
    (*new_node)->keys_.assign(keys_.begin() + split_pos, keys_.end());
    (*new_node)->data_blocks_.assign(data_blocks_.begin() + split_pos, data_blocks_.end());
    (*new_node)->size_ = size_ - split_pos;
    // 分裂键：新节点的第一个min_key（父节点需插入）
    *split_key = (*new_node)->keys_[0];
  } else {
    // 内部节点分裂：拆分max_key和子节点（引用1-67）
    (*new_node)->keys_.assign(keys_.begin() + split_pos + 1, keys_.end());  // 排除分裂键
    // 移动子节点（使用move避免复制unique_ptr）
    for (size_t i = split_pos + 1; i < children_.size(); ++i) {
      (*new_node)->children_.push_back(std::move(children_[i]));
    }
    (*new_node)->size_ = size_ - (split_pos + 1);
    // 分裂键：当前节点的split_pos位置的max_key（父节点需插入）
    *split_key = keys_[split_pos];
  }

  // 收缩当前节点
  keys_.erase(keys_.begin() + split_pos, keys_.end());
  if (type_ == kLeafNode) {
    data_blocks_.erase(data_blocks_.begin() + split_pos, data_blocks_.end());
  } else {
    children_.erase(children_.begin() + split_pos + 1, children_.end());
  }
  size_ = split_pos;
  return true;
}

DataBlock* SearchNode::FindDataBlock(uint64_t key) const {
  if (type_ != kLeafNode) {
    return nullptr;
  }
  // 使用共享读锁保护（ROWEX改进：多读单写，引用1-92）
  std::shared_lock<std::shared_mutex> lock(rw_mutex_);
  
  if (keys_.empty()) {
    return nullptr;
  }
  
  auto it = std::upper_bound(keys_.begin(), keys_.end(), key);
  if (it == keys_.begin()) {
    return nullptr;
  }
  const size_t pos = std::distance(keys_.begin(), it) - 1;
  if (pos >= data_blocks_.size()) {
    return nullptr;
  }
  return data_blocks_[pos];
}

SearchNode* SearchNode::FindChild(uint64_t key) const {
  if (type_ != kInternalNode) {
    return nullptr;
  }
  // 使用共享读锁保护（ROWEX改进：多读单写，引用1-92）
  std::shared_lock<std::shared_mutex> lock(rw_mutex_);
  
  if (keys_.empty()) {
    return nullptr;
  }
  
  // 修复P0-3：右溢逻辑错误 - 右溢应落到最右子树
  auto it = std::upper_bound(keys_.begin(), keys_.end(), key);
  const size_t pos = std::distance(keys_.begin(), it);
  if (pos >= children_.size()) {
    return children_.back().get(); // 右溢：返回最右子树
  }
  return children_[pos].get();
}
