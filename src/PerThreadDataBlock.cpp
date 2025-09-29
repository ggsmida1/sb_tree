#include "PerThreadDataBlock.h"

// ========================= 构造 =========================
PerThreadDataBlock::PerThreadDataBlock()
    : num_entries_(0), max_key_(0) {}

// ========================= 写入接口 =========================
// 尾部插入一条 KV。若已满返回 false。
bool PerThreadDataBlock::Insert(Key key, Value value)
{
    if (IsFull())
        return false;
    data_[num_entries_] = {key, value};
    max_key_ = key; // 写入是顺序追加，直接更新最大 key
    ++num_entries_;
    return true;
}

// 是否已满
bool PerThreadDataBlock::IsFull() const
{
    return num_entries_ >= kCapacity;
}

// ========================= 只读视图 =========================
// 返回已写入的条目数
size_t PerThreadDataBlock::GetNumEntries() const
{
    return num_entries_;
}

// 返回指向数据区的只读指针（外部仅在只读阶段使用）
const KVPair *PerThreadDataBlock::GetData() const
{
    return data_;
}
