#include "segmented_block_converter.h"
#include "sb_tree.h"
#include "block_allocator.h"
#include "segmented_block.h"
#include "data_block.h"
#include <algorithm>
#include <functional>
#include <thread>

// -----------------------------------------------------------------------------
// SegmentedBlockConverter 实现（论文4.2节，引用1-107、1-108）
// -----------------------------------------------------------------------------
SegmentedBlockConverter::SegmentedBlockConverter(SBTree* sb_tree, BlockAllocator* allocator)
    : sb_tree_(sb_tree),
      allocator_(allocator),
      stop_threads_(false),
      num_converter_threads_(std::max(2u, std::thread::hardware_concurrency() / 2)) {
  // 启动多线程转换器池（论文建议多线程处理）
  conversion_threads_.reserve(num_converter_threads_);
  for (size_t i = 0; i < num_converter_threads_; ++i) {
    conversion_threads_.emplace_back(std::bind(&SegmentedBlockConverter::ConversionThreadMain, this));
  }
}

SegmentedBlockConverter::~SegmentedBlockConverter() {
  stop_threads_ = true;
  task_cv_.notify_all();  // 唤醒所有线程退出
  for (auto& thread : conversion_threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}

void SegmentedBlockConverter::SubmitConversionTask(std::unique_ptr<SegmentedBlock> segmented_block) {
  std::lock_guard<std::mutex> lock(queue_mutex_);
  task_queue_.push(std::move(segmented_block));
  pending_tasks_.fetch_add(1, std::memory_order_relaxed);
  task_cv_.notify_one();  // 通知转换线程有新任务
}

void SegmentedBlockConverter::SubmitIndexTask(DataBlock* data_block) {
  if (!data_block) return;
  std::lock_guard<std::mutex> lock(queue_mutex_);
  index_queue_.push(data_block);
  pending_tasks_.fetch_add(1, std::memory_order_relaxed);
  task_cv_.notify_one();
}

void SegmentedBlockConverter::ConversionThreadMain() {
  while (!stop_threads_) {
    std::vector<std::unique_ptr<SegmentedBlock>> batch_tasks;
    std::vector<DataBlock*> batch_index_tasks;
    
    // 批量收集任务（论文要求：减少转换频率）
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      
      // 等待任务或批量超时
      auto timeout = std::chrono::nanoseconds(kBatchTimeoutNs);
      bool has_tasks = task_cv_.wait_for(lock, timeout, [this]() {
        return stop_threads_ || !task_queue_.empty() || !index_queue_.empty();
      });
      
      if (stop_threads_ && task_queue_.empty() && index_queue_.empty()) {
        break;
      }
      
      // 批量收集转换任务
      size_t collected = 0;
      while (!task_queue_.empty() && collected < kBatchSize) {
        batch_tasks.emplace_back(std::move(task_queue_.front()));
        task_queue_.pop();
        collected++;
      }
      
      // 批量收集索引任务
      collected = 0;
      while (!index_queue_.empty() && collected < kBatchSize) {
        batch_index_tasks.push_back(index_queue_.front());
        index_queue_.pop();
        collected++;
      }
    }

    // 批量处理索引任务
    for (DataBlock* index_task : batch_index_tasks) {
      sb_tree_->IndexDataBlockNonOwning(index_task);
      pending_tasks_.fetch_sub(1, std::memory_order_relaxed);
    }
    
    // 批量处理转换任务
    for (auto& task : batch_tasks) {
      if (!task) continue;

      // 等待旧分段块上无活跃写者，确保安全抓取
      task->WaitForQuiescent();

      // 1. 合并所有PerThreadBlock的KV对（引用1-108）
      std::vector<KeyValuePair> merged_kv;
      std::vector<std::unique_ptr<PerThreadDataBlock>> gc_list;
      gc_list.reserve(task->GetMaxThreads());
      for (size_t i = 0; i < task->GetMaxThreads(); ++i) {
        PerThreadDataBlock* pt_block = task->StealPerThreadBlock(i);
        if (!pt_block) continue;
        pt_block->CopyAllKvThreadSafe(&merged_kv);
        gc_list.emplace_back(pt_block);
      }
      if (merged_kv.empty()) continue;

      // 2. 排序KV对（按key递增，引用1-108）
      // 优化：利用时间序列键单调特性，排序开销低（引用1-108）
      std::sort(merged_kv.begin(), merged_kv.end(), 
                [](const KeyValuePair& a, const KeyValuePair& b) {
                  return a.key < b.key;
                });

      // 3. 拆分并创建DataBlock（批量填充，引用1-108）
      std::vector<std::unique_ptr<DataBlock>> data_blocks;
      size_t kv_idx = 0;
      const size_t total_kv = merged_kv.size();
      while (kv_idx < total_kv) {
        auto data_block = std::make_unique<DataBlock>(allocator_);
        size_t end_idx = std::min(kv_idx + kDataBlockCapacity, total_kv);
        data_block->BulkFill(merged_kv, kv_idx, end_idx);
        kv_idx = end_idx;
        data_blocks.push_back(std::move(data_block));
      }

      // 4. 链接DataBlock（形成链表，引用1-65）
      for (size_t i = 0; i < data_blocks.size() - 1; ++i) {
        data_blocks[i]->SetNextBlock(std::move(data_blocks[i + 1]));
      }

      // 5. 通知SBTree更新搜索层（引用1-119）
      if (!data_blocks.empty()) {
        sb_tree_->UpdateSearchLayerWithDataBlocks(std::move(data_blocks));
      }
      // 延迟回收：gc_list在作用域末尾自动释放
      
      // 6. 任务完成，减少待处理计数
      pending_tasks_.fetch_sub(1, std::memory_order_relaxed);
    }
  }
}

void SegmentedBlockConverter::WaitForIdle() {
  // 等待所有待处理任务完成
  while (pending_tasks_.load(std::memory_order_acquire) > 0) {
    std::this_thread::yield();
  }
}
