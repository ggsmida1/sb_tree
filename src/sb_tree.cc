#include "sb_tree.h"
#include "block_allocator.h"
#include "segmented_block.h"
#include "segmented_block_converter.h"
#include "search_node.h"
#include "data_block.h"
#include <thread>
#include <functional>
#include <algorithm>
#include <iostream>

// 线程局部存储初始化（如果需要的话）
// 稳定线程ID分配：0..N-1（N <= kSegmentedBlockMaxThreads）
static std::atomic<size_t> g_thread_id_counter{0};
thread_local size_t tls_thread_id = SIZE_MAX;
// 线程局部最大键发布（减少全局CAS竞争）
thread_local uint64_t tls_local_max_key = 0;
thread_local uint64_t tls_published_max_key = 0;

// 修复P1-1：全局原子时间戳，替代thread_local节流机制
static std::atomic<uint64_t> g_last_conversion_time{0};

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
  // 线程局部更新 + 分批发布到全局，降低CAS竞争
  if (key > tls_local_max_key) {
    tls_local_max_key = key;
    // 每提升一定幅度再发布一次
    constexpr uint64_t kPublishStep = 1024;
    if (tls_local_max_key - tls_published_max_key >= kPublishStep) {
      uint64_t observed = current_max_key_.load(std::memory_order_relaxed);
      while (tls_local_max_key > observed &&
             !current_max_key_.compare_exchange_weak(observed, tls_local_max_key,
                                                    std::memory_order_release,
                                                    std::memory_order_relaxed)) {
        // CAS失败时 observed 已更新为新值，循环重试直到不需要更新
      }
      tls_published_max_key = tls_local_max_key;
    }
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

    // 修复P0-2：使用SegmentedBlock内部slot分配，避免线程ID冲突
    const size_t thread_id = seg->AllocateSlot();
    
    // 调试信息：记录slot分配情况（减少输出频率）
    static thread_local int debug_count = 0;
    if (++debug_count % 1000 == 0) {
      std::cout << "Insert: thread_id=" << thread_id << ", key=" << key << std::endl;
    }

    bool inserted = false;
    bool need_convert = false;
    {
      // 在slot分配、校验以及实际插入期间持有写者计数，防止转换器提前steal
      ScopedSegmentWrite guard(seg);
      if (!guard.IsAcquired()) {
        // 转换期间无法获取写锁，重新开始
        continue;
      }
      PerThreadDataBlock* pt_block = seg->AllocatePerThreadBlock(thread_id);
      if (!pt_block) {
        return false;
      }
      // 再次确认seg仍为当前分段块且slot未被替换
      if (seg != current_segmented_block_.load(std::memory_order_acquire)) {
        // 分段块已被替换，退出当前循环，重新开始
        continue;
      } else if (seg->LoadPerThreadBlock(thread_id) != pt_block) {
        // slot已被替换，退出当前循环，重新开始
        continue;
      } else {
        // 3. 插入PerThreadBlock（引用1-103）
        if (pt_block->Insert(key, value)) {
          // 更新分段块的键范围
          seg->UpdateKeyRange(key);
          // 论文：仅当当前线程块满时才需要转换
          need_convert = pt_block->IsFull();
          inserted = true;
        } else {
          // 块满：需要切换
          need_convert = true;
        }
      }
    } // guard析构，结束写者计数

    if (inserted) {
      if (need_convert) {
        // 修复P1-1：使用全局原子时间戳进行转换节流
        const uint64_t now = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        const uint64_t min_interval = 1000; // 1ms最小间隔
        
        uint64_t last_time = g_last_conversion_time.load(std::memory_order_acquire);
        if (now - last_time < min_interval) {
          // 转换过于频繁，跳过本次转换
          return true;
        }
        
        // 尝试更新全局转换时间戳
        if (!g_last_conversion_time.compare_exchange_strong(last_time, now, 
                                                           std::memory_order_acq_rel, 
                                                           std::memory_order_acquire)) {
          // 其他线程已触发转换，跳过
          return true;
        }
        
        // 正确的转换流程：先标记旧分段块，再切换
        std::cout << "Insert: Triggering conversion for segmented block (key=" << key << ")" << std::endl;
        
        // 1. 标记旧分段块为转换中，阻止新写入
        seg->MarkConversionTriggered();
        
        // 2. 创建新分段块并原子切换
        SegmentedBlock* expected = seg;
        SegmentedBlock* new_seg = new SegmentedBlock(kSegmentedBlockMaxThreads, &allocator_);
        if (current_segmented_block_.compare_exchange_strong(expected, new_seg, std::memory_order_acq_rel, std::memory_order_acquire)) {
          // 3. 提交旧分段块的转换任务
          converter_.SubmitConversionTask(std::unique_ptr<SegmentedBlock>(seg));
          std::cout << "Insert: Conversion task submitted for old segmented block" << std::endl;
        } else {
          std::cout << "Insert: Another thread already triggered conversion" << std::endl;
          delete new_seg;
        }
      }
      return true;
    }

    // 插入失败（块满或被替换）：尝试切换分段块
    if (need_convert) {
      // 修复P1-1：同样使用全局原子时间戳进行转换节流
      const uint64_t now = std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
      const uint64_t min_interval = 1000; // 1ms最小间隔
      
      uint64_t last_time = g_last_conversion_time.load(std::memory_order_acquire);
      if (now - last_time >= min_interval) {
        // 尝试更新全局转换时间戳
        if (g_last_conversion_time.compare_exchange_strong(last_time, now, 
                                                         std::memory_order_acq_rel, 
                                                         std::memory_order_acquire)) {
          SegmentedBlock* expected = seg;
          SegmentedBlock* new_seg = new SegmentedBlock(kSegmentedBlockMaxThreads, &allocator_);
          if (current_segmented_block_.compare_exchange_strong(expected, new_seg, std::memory_order_acq_rel, std::memory_order_acquire)) {
            // 异步转换优化：不设置conversion_triggered_，允许并发写入
            converter_.SubmitConversionTask(std::unique_ptr<SegmentedBlock>(seg));
          } else {
            delete new_seg;
          }
        }
      }
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
        curr = curr->GetNextBlock();
      }
      if (target_block) {
        break;
      }
    }
  }
  
  if (!target_block) {
    // 尚无数据块可供插入：触发一次分段块转换并等待完成，再重试一次
    SegmentedBlock* seg = current_segmented_block_.load(std::memory_order_acquire);
    if (seg) {
      SegmentedBlock* expected = seg;
      SegmentedBlock* new_seg = new SegmentedBlock(kSegmentedBlockMaxThreads, &allocator_);
      if (current_segmented_block_.compare_exchange_strong(expected, new_seg, std::memory_order_acq_rel, std::memory_order_acquire)) {
        seg->MarkConversionTriggered();
        converter_.SubmitConversionTask(std::unique_ptr<SegmentedBlock>(seg));
        // 等待转换完成以确保搜索层可见
        converter_.WaitForIdle();
      } else {
        delete new_seg;
      }
    }
    // 重试定位目标块
    current_node = root_.get();
    while (current_node && current_node->GetType() != SearchNode::kLeafNode) {
      current_node = current_node->FindChild(key);
    }
    if (current_node) {
      target_block = current_node->FindDataBlock(key);
    }
    if (!target_block) {
      // 选择合适的落点：最左块或覆盖key的块
      std::vector<DataBlock*> all_blocks;
      CollectAllDataBlocks(root_.get(), &all_blocks);
      if (all_blocks.empty()) return false;
      // 默认选择最左块
      target_block = all_blocks.front();
      for (DataBlock* db : all_blocks) {
        if (key >= db->GetMinKey() && key <= db->GetMaxKey()) { target_block = db; break; }
        if (key < db->GetMinKey()) { target_block = db; break; }
        target_block = db; // 最右块
      }
    }
  }

  // 4. 插入延迟数据（可能触发块分裂，引用1-120）
  std::unique_ptr<DataBlock> split_block;
  const int insert_res = target_block->Insert(key, value, &split_block);
  if (insert_res == -1) {
    return false;
  }

  // 5. 若分裂，不在此处直接写搜索层（保持ROWEX单写者）。
  //    交由转换线程异步索引新块（仅插入锚点键）。
  if (insert_res == 1) {
    // 现在 split_block 持有新块所有权；由调用者负责挂链与提交索引
    if (split_block) {
      DataBlock* new_block_ptr = split_block.get();

      // 新块挂入链表：new.next = old.next; old.next = new
      split_block->SetNextBlockPtr(target_block->GetNextBlock());
      target_block->SetNextBlock(std::move(split_block));

      // 仅由转换线程更新搜索层，这里只提交"锚点键"索引任务
      if (new_block_ptr) {
        converter_.SubmitIndexTask(new_block_ptr);
      }
    }
    return true;
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
                                   uint64_t /*split_key*/) {
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
  
  // 修复P0-1：统一所有权模型 - 容器唯一持有，链表只存裸指针
  // 1. 先转移所有权到数据层容器
  std::vector<DataBlock*> data_block_ptrs;
  data_block_ptrs.reserve(data_blocks.size());
  
  {
    std::lock_guard<std::mutex> lock(data_layer_mutex_);
    for (auto& block : data_blocks) {
      if (block) {
        data_block_ptrs.push_back(block.get());
        data_layer_blocks_.push_back(std::move(block));
      }
    }
  }
  
  // 2. 建立链表关系（只存裸指针，不转移所有权）
  for (size_t i = 0; i < data_block_ptrs.size() - 1; ++i) {
    data_block_ptrs[i]->SetNextBlockPtr(data_block_ptrs[i + 1]);
  }
  
  // 3. 将每个块都索引到搜索层（确保所有块都可见）
  for (DataBlock* block_ptr : data_block_ptrs) {
    InsertDataBlockToSearchLayer(block_ptr);
  }
}

void SBTree::IndexDataBlockNonOwning(DataBlock* data_block) {
  if (!data_block) return;
  InsertDataBlockToSearchLayer(data_block);
}

bool SBTree::LookupInSegmentedBlock(uint64_t key, uint64_t* out_value) const {
  SegmentedBlock* seg = current_segmented_block_.load(std::memory_order_acquire);
  if (!seg) return false;
  
  // 修复P1-3：优化segmented block扫描，避免O(80)全扫描
  // 策略1：只在分段块键范围内查找
  const uint64_t seg_min = seg->GetMinKey();
  const uint64_t seg_max = seg->GetMaxKey();
  
  if (key < seg_min || key > seg_max) {
    return false; // key不在分段块范围内，直接跳过
  }
  
  // 策略2：优先检查最近活跃的线程块（基于线程ID的启发式）
  const size_t max_threads = seg->GetMaxThreads();
  
  // 首先检查当前线程的块（最可能包含新数据）
  thread_local static size_t tls_last_checked_thread = 0;
  size_t start_thread = tls_last_checked_thread % max_threads;
  
  for (size_t offset = 0; offset < max_threads; ++offset) {
    size_t i = (start_thread + offset) % max_threads;
    PerThreadDataBlock* pt_block = seg->LoadPerThreadBlock(i);
    if (!pt_block) continue;
    
    const auto& kv_pairs = pt_block->GetAllKv();
    
    // 注意：PerThreadDataBlock中的数据不是排序的，所以不能使用这个优化
    // if (!kv_pairs.empty() && key < kv_pairs.front().key) {
    //   continue;
    // }
    
    for (const auto& kv : kv_pairs) {
      if (kv.key == key) {
        *out_value = kv.value;
        tls_last_checked_thread = i; // 记录最近找到的线程
        return true;
      }
      // 注意：PerThreadDataBlock中的数据不是排序的，所以不能使用这个优化
      // if (kv.key > key) break; // 利用有序特性提前退出
    }
  }
  return false;
}

const uint64_t* SBTree::Lookup(uint64_t key) const {
  // 1. 先检查分段块（保证新写入数据的可见性；若负载较大可关闭）
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

  // 4. 可能沿链表前进（转换后的块通过next相连）
  const uint32_t max_retries = 3;  // 每个块的最大重试次数
  while (data_block) {
    const uint64_t* value = nullptr;
    for (uint32_t i = 0; i < max_retries; ++i) {
      value = data_block->Lookup(key);
      if (value) {
        return value;
      }
      std::this_thread::yield();
    }
    if (key > data_block->GetMaxKey()) {
      data_block = data_block->GetNextBlock();
    } else {
      break;
    }
  }
  return nullptr;
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
    // 搜索层未找到，先尝试数据层（引用1-119）
    if (root_->GetType() == SearchNode::kLeafNode) {
      const auto& data_blocks = root_->GetDataBlocks();
      for (DataBlock* db : data_blocks) {
        if (db->GetMaxKey() >= start_key) {
          start_block = db;
          break;
        }
      }
    }
    // 如仍未找到，回退到当前分段块的临时扫描（合并各线程块）
    if (!start_block) {
      SegmentedBlock* seg = current_segmented_block_.load(std::memory_order_acquire);
      if (!seg) return 0;
      std::vector<KeyValuePair> temp;
      temp.reserve(1024);
      const size_t max_threads = seg->GetMaxThreads();
      for (size_t i = 0; i < max_threads; ++i) {
        PerThreadDataBlock* pt_block = seg->LoadPerThreadBlock(i);
        if (!pt_block) continue;
        const auto& kv = pt_block->GetAllKv();
        temp.insert(temp.end(), kv.begin(), kv.end());
      }
      if (temp.empty()) return 0;
      std::sort(temp.begin(), temp.end(), [](const KeyValuePair& a, const KeyValuePair& b){return a.key < b.key;});
      auto it = std::lower_bound(temp.begin(), temp.end(), start_key, [](const KeyValuePair& a, uint64_t k){return a.key < k;});
      size_t taken = 0;
      for (; it != temp.end() && taken < count; ++it, ++taken) {
        result->push_back(*it);
      }
      return taken;
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
