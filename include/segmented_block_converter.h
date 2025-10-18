#ifndef SEGMENTED_BLOCK_CONVERTER_H_
#define SEGMENTED_BLOCK_CONVERTER_H_

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <vector>
#include "data_block.h"

// 前向声明
class SBTree;
class BlockAllocator;
class SegmentedBlock;

/// 分段块转换任务（论文4.2节，引用1-107、1-108）
/// 功能：专用线程处理分段块到数据块的转换，避免阻塞插入
class SegmentedBlockConverter {
 public:
  SegmentedBlockConverter(SBTree* sb_tree, BlockAllocator* allocator);
  ~SegmentedBlockConverter();

  /// 提交转换任务（非阻塞）
  void SubmitConversionTask(std::unique_ptr<SegmentedBlock> segmented_block);

 private:
  /// 转换线程主函数（论文4.2节：合并排序KV、创建数据块、更新搜索层）
  void ConversionThreadMain();

  SBTree* sb_tree_;                               // 指向SBTree实例
  BlockAllocator* allocator_;                     // 块分配器
  std::thread conversion_thread_;                 // 专用转换线程
  std::queue<std::unique_ptr<SegmentedBlock>> task_queue_;  // 任务队列
  std::mutex queue_mutex_;                        // 队列互斥锁
  std::condition_variable task_cv_;               // 任务通知条件变量
  std::atomic<bool> stop_thread_;                 // 线程停止标记
};

#endif  // SEGMENTED_BLOCK_CONVERTER_H_
