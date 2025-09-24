#include "sb-tree.h"

// ===== DataBlock 接口实现 =====

size_t DataBlock::build_from_sorted(const KVPair *src, size_t n)
{
    const size_t take = (n > kCapacity) ? kCapacity : n;

    for (size_t i = 0; i < take; ++i)
    {
        keys_[i] = src[i].key;
        vals_[i] = src[i].value;
    }

    count_ = static_cast<uint32_t>(take);
    if (take > 0)
        min_key_ = keys_[0];

    build_nary_();
    return take; // 注意：如果 n > kCapacity，调用方需要继续分配下一个 DataBlock
}

bool DataBlock::find(Key k, Value &out) const
{
    if (count_ == 0 || k < min_key_)
        return false;

    auto [lo, hi] = bucket_range_(k);
    for (size_t i = lo; i < hi; ++i)
    {
        if (keys_[i] == k)
        {
            out = vals_[i];
            return true;
        }
        if (keys_[i] > k)
            break; // 提前结束
    }
    return false;
}

size_t DataBlock::scan_from(Key startKey, size_t count, std::vector<Value> &out) const
{
    if (count_ == 0)
        return 0;

    auto [lo, hi] = bucket_range_(startKey);
    size_t pos = lo;
    while (pos < hi && keys_[pos] < startKey)
        ++pos;

    size_t taken = 0;
    while (pos < this->count_ && taken < count)
    {
        out.push_back(vals_[pos]);
        ++pos;
        ++taken;
    }
    return taken;
}

// ===================== 辅助函数 =====================

void DataBlock::build_nary_()
{
    if (count_ == 0)
        return;

    const size_t buckets = (count_ < kBuckets) ? count_ : kBuckets;
    const size_t per = (count_ + buckets - 1) / buckets; // 向上取整

    for (size_t i = 0; i < buckets; ++i)
    {
        size_t idx = i * per;
        if (idx >= count_)
            nary_[i] = std::numeric_limits<Key>::max();
        else
            nary_[i] = keys_[idx]; // 桶代表值 = 每桶第一个 key
    }

    for (size_t i = buckets; i < kBuckets; ++i)
        nary_[i] = std::numeric_limits<Key>::max();
}

std::pair<size_t, size_t> DataBlock::bucket_range_(Key k) const
{
    size_t upper = 0;
    while (upper < kBuckets && nary_[upper] <= k)
        ++upper;

    const size_t buckets = (count_ < kBuckets) ? count_ : kBuckets;
    const size_t per = (count_ + buckets - 1) / buckets;

    if (upper == 0)
    {
        return {0, std::min<size_t>(per, count_)};
    }

    size_t lo = (upper - 1) * per;
    size_t hi = std::min<size_t>(upper * per, count_);
    return {lo, hi};
}
