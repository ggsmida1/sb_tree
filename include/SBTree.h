#pragma once
#include <atomic>
#include "KVPair.h"
#include "SegmentedBlock.h"
#include "DataBlock.h"
#include "PerThreadDataBlock.h"

// SBTree 主类（当前最小版本：只维护活跃的 SegmentedBlock 并顺序写入）
class SBTree
{
public:
    SBTree();

    // 顺序插入（key 单增假设）
    void insert(Key key, Value value);

private:
    Key max_key_{0};                         // 目前未使用，预留
    std::atomic<SegmentedBlock *> shortcut_; // 指向当前活跃分段块
};
