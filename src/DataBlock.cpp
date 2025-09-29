#include "DataBlock.h"

// ========================= 构造 =========================
DataBlock::DataBlock()
    : status_(Status::READY),
      min_key_(std::numeric_limits<Key>::max()),
      next_(nullptr),
      lock_(0),
      count_(0)
{
    for (size_t i = 0; i < kBuckets; ++i)
    {
        nary_[i] = std::numeric_limits<Key>::max();
    }
}

// ========================= 构建 =========================
// 从已排序 KV 数组中构建 DataBlock
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
    return take; // 如果 n > kCapacity，需要调用方继续切块
}

// ========================= 查找 =========================
// 在块内查找 key，命中则返回 true 并写出 value
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

// ========================= 扫描 =========================
// 从 startKey 开始扫描最多 count 条数据
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

// 扫描 [start, end] 范围内的所有数据
size_t DataBlock::scan_range(Key start, Key end, std::vector<Value> &out) const
{
    if (start > end)
        return 0;
    size_t n = this->size();
    size_t L = 0, R = n, pos = n;
    while (L < R)
    {
        size_t mid = L + ((R - L) >> 1);
        const KVPair &e = this->get_entry(mid);
        if (e.key >= start)
        {
            pos = mid;
            R = mid;
        }
        else
            L = mid + 1;
    }
    if (pos == n)
        return 0;

    size_t taken = 0;
    for (size_t i = pos; i < n; ++i)
    {
        const KVPair &e = this->get_entry(i);
        if (e.key > end)
            break;
        out.push_back(e.value);
        ++taken;
    }
    return taken;
}

// ========================= 内部辅助 =========================
// 构建 N-ary 搜索表
void DataBlock::build_nary_()
{
    if (count_ == 0)
        return;
    const size_t buckets = (count_ < kBuckets) ? count_ : kBuckets;
    const size_t per = (count_ + buckets - 1) / buckets; // 向上取整
    for (size_t i = 0; i < buckets; ++i)
    {
        size_t idx = i * per;
        nary_[i] = (idx >= count_) ? std::numeric_limits<Key>::max() : keys_[idx];
    }
    for (size_t i = buckets; i < kBuckets; ++i)
        nary_[i] = std::numeric_limits<Key>::max();
}

// 根据 key 确定桶范围 [lo, hi)
std::pair<size_t, size_t> DataBlock::bucket_range_(Key k) const
{
    size_t upper = 0;
    while (upper < kBuckets && nary_[upper] <= k)
        ++upper;
    const size_t buckets = (count_ < kBuckets) ? count_ : kBuckets;
    const size_t per = (count_ + buckets - 1) / buckets;
    if (upper == 0)
        return {0, std::min<size_t>(per, count_)};
    size_t lo = (upper - 1) * per;
    size_t hi = std::min<size_t>(upper * per, count_);
    return {lo, hi};
}