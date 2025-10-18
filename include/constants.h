#ifndef CONSTANTS_H_
#define CONSTANTS_H_

#include <cstdint>

// 配置常量（严格匹配论文参数，引用1-65、1-73、1-87、1-89）
constexpr size_t kPerThreadBlockCapacity = 512;    // 每线程数据块容量（论文3.3节）
constexpr size_t kDataBlockCapacity = 1024;        // 数据块容量（4KB块内存储，引用1-73）
constexpr size_t kNAryBucketSize = 32;             // N元桶大小（论文3.4节，引用1-87）
constexpr size_t kSearchNodeCapacity = 64;         // 搜索层节点容量（论文3.2节）
constexpr size_t kSegmentedBlockMaxThreads = 80;   // 分段块最大线程数（匹配实验，引用1-142）
constexpr size_t kBlockSize = 4096;                // 固定块大小（4KB，引用1-87）
constexpr uint64_t kInvalidKey = UINT64_MAX;       // 无效键标记

#endif  // CONSTANTS_H_
