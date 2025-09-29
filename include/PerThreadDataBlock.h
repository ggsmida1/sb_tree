#pragma once
#include <cstdint>
#include <cstddef>
#include "KVPair.h"

// -----------------------------------------------------------------------------
// PerThreadDataBlock
// -----------------------------------------------------------------------------
// 作用：
// - 每线程私有的顺序写入缓冲块（此实现为 16KB 定长）。
// - 仅支持尾部追加（append），用于在段转换前暂存本线程写入的 KV。
// - 提供给转换阶段的只读视图（GetData / GetNumEntries）。
// 并发语义：
// - 按“每线程独占”使用，不做内部并发控制；不同线程各持有各自实例。
// - 段转换时，由上层协调停止追加并只读访问本块数据。
// 不变式（约定）：
// - num_entries_ ∈ [0, kCapacity]；
// - 当工作负载单调递增写入时，data_[i].key 非降序（便于后续合并/切片）。
// - max_key_ 记录到目前为止写入的最大 key（可用于范围估计/断言）。
// 注意：
// - 不负责内存回收/复用，由上层管理生命周期。
// -----------------------------------------------------------------------------
class PerThreadDataBlock
{
public:
    // ========================= 构造与容量 =========================
    PerThreadDataBlock(); // 初始化为空块（entries=0，max_key_ 置初值）

    // ========================= 追加写入接口 =========================
    // 尾部追加一条 KV；若块已满则返回 false，不修改状态。
    bool Insert(Key key, Value value);
    // 是否已满（num_entries_ == kCapacity）。
    bool IsFull() const;

    // ========================= 转换阶段只读视图 =========================
    // 当前已写入的条目数（用于收集/合并）。
    size_t GetNumEntries() const;
    // 只读数据指针（首地址）。注意：仅在外部确保“写入已停止”前提下使用。
    const KVPair *GetData() const;

private:
    // ========================= 常量与容量计算 =========================
    static constexpr size_t kBlockSize = 16384; // 固定块大小（字节）
    // 元数据开销（用于计算可用容量）
    static constexpr size_t kMetadataSize = sizeof(size_t) /*num_entries_*/ +
                                            sizeof(Key) /*max_key_*/;
    // 可存放的 KV 条目数（整除截断）
    static constexpr size_t kCapacity = (kBlockSize - kMetadataSize) / sizeof(KVPair);

    // ========================= 元数据 =========================
    size_t num_entries_ = 0; // 已写入的条目数
    Key max_key_ = Key{};    // 到目前为止的最大 key（用于断言/范围估计）

    // ========================= 实际数据区 =========================
    KVPair data_[kCapacity]; // 顺序追加的 KV 存储
};