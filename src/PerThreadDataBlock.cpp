#include "PerThreadDataBlock.h"

PerThreadDataBlock::PerThreadDataBlock() {}

// 尾部插入一条 KV。若已满返回 false。
bool PerThreadDataBlock::Insert(Key key, Value value)
{
    if (IsFull())
        return false;
    data_[num_entries_] = {key, value};
    ++num_entries_;
    return true;
}

// 是否已满
bool PerThreadDataBlock::IsFull() const
{
    return num_entries_ >= kCapacity;
}

// 返回已写入的条目数
size_t PerThreadDataBlock::GetNumEntries() const
{
    return num_entries_;
}

// 返回数据数组的首地址
const KVPair *PerThreadDataBlock::GetData() const
{
    return data_;
}
