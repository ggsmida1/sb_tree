#include "PerThreadDataBlock.h"

PerThreadDataBlock::PerThreadDataBlock()
    : num_entries_(0), max_key_(0) {}

bool PerThreadDataBlock::Insert(Key key, Value value)
{
    if (IsFull())
        return false;

    data_[num_entries_] = {key, value};
    max_key_ = key; // 顺序写入，直接更新
    ++num_entries_;
    return true;
}

bool PerThreadDataBlock::IsFull() const
{
    return num_entries_ >= kCapacity;
}

size_t PerThreadDataBlock::GetNumEntries() const
{
    return num_entries_;
}

const KVPair *PerThreadDataBlock::GetData() const
{
    return data_;
}
