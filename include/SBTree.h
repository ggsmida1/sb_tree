#pragma once
#include <atomic>
#include <mutex> // 新增头文件
#include "KVPair.h"
#include "SegmentedBlock.h"
#include "DataBlock.h"
#include "PerThreadDataBlock.h"
#include "SearchLayer.h"

// SBTree 主类
class SBTree
{
public:
    SBTree();
    ~SBTree(); // 新增析构函数，用于释放 DataBlock 链表

    // 顺序插入（key 单增假设）
    void insert(Key key, Value value);

    // 查找
    bool lookup(Key k, Value *out) const;

    // 扫描
    size_t scan(Key l, Key r, std::vector<Value> &out) const;

    // ++ 新增：用于测试的验证方法 ++
    // 遍历整个数据层，验证总数、顺序和数据正确性
    bool verify_data_layer(size_t expected_total_keys) const;

    void flush(); // ++ 新增 flush 方法 ++

private:
    // ++ 新增私有辅助函数 ++
    void convert_and_append(SegmentedBlock *seg_to_convert);

    Key max_key_{0};
    std::atomic<SegmentedBlock *> shortcut_; // 指向当前活跃分段块

    // ++ 新增成员，用于管理 DataBlock 链表 ++
    mutable std::mutex data_layer_lock_; // ++ 改为 mutable ++
    DataBlock *data_head_;               // 数据块链表的头指针
    DataBlock *data_tail_;               // 数据块链表的尾指针

    // 搜索层
    SearchLayer search_;
};