#include "DataBlock.h"
#include <algorithm>

// ========================= 构造 =========================
DataBlock::DataBlock()
    : status_(Status::READY),
      min_key_(std::numeric_limits<Key>::max()),
      max_key_(0),
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
    {
        min_key_ = keys_[0];
        // 【修改】在构建时，同时设置 max_key_
        // 因为数据是排序的，所以最后一个元素的 key 就是最大 key
        max_key_ = keys_[take - 1];
    }
    // 【注意】如果 take == 0 (块为空)，min_key_ 和 max_key_ 会保留
    // 构造函数中的初始值，这是正确的行为。

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

// ========================= 写入（延迟数据） =========================
bool DataBlock::insert_sorted(Key k, Value v, DataBlock **outNewRight)
{
    if (outNewRight)
        *outNewRight = nullptr;

    std::lock_guard<std::mutex> g(write_mutex_);

    // 若已满，先分裂，再决定写入左或右
    if (is_full())
    {
        DataBlock *right = split_unsafe_();
        if (outNewRight)
            *outNewRight = right;
        // 决定目标块
        if (k > max_key_ && right)
        {
            return right->insert_sorted(k, v, nullptr);
        }
        // 否则插入当前块
    }

    // 在当前块内二分定位插入位置
    if (count_ == 0)
    {
        keys_[0] = k;
        vals_[0] = v;
        count_ = 1;
        min_key_ = k;
        max_key_ = k;
        build_nary_();
        return true;
    }

    size_t L = 0, R = count_;
    while (L < R)
    {
        size_t mid = L + ((R - L) >> 1);
        if (keys_[mid] < k)
            L = mid + 1;
        else
            R = mid;
    }
    // L 为插入位置，将 [L..count_-1] 后移一位
    if (count_ >= kCapacity)
        return false; // 理论上前面已处理满载
    for (size_t i = count_; i > L; --i)
    {
        keys_[i] = keys_[i - 1];
        vals_[i] = vals_[i - 1];
    }
    keys_[L] = k;
    vals_[L] = v;
    ++count_;
    if (k > max_key_)
        max_key_ = k;
    if (k < min_key_)
        min_key_ = k;
    build_nary_();
    return true;
}

// 分裂：等分后半部分到新块，维护链表与索引
DataBlock *DataBlock::split_unsafe_()
{
    if (count_ < 2)
        return nullptr;
    const size_t split_pos = count_ / 2;

    DataBlock *right = new DataBlock();
    // 将后半部分复制到右块
    size_t rcount = count_ - split_pos;
    for (size_t i = 0; i < rcount; ++i)
    {
        right->keys_[i] = keys_[split_pos + i];
        right->vals_[i] = vals_[split_pos + i];
    }
    right->count_ = static_cast<uint32_t>(rcount);
    right->min_key_ = right->keys_[0];
    right->max_key_ = right->keys_[right->count_ - 1];

    // 缩减本块为左半部分
    count_ = static_cast<uint32_t>(split_pos);
    min_key_ = keys_[0];
    max_key_ = keys_[count_ - 1];

    // 链接链表：right 继承原 next_
    right->next_ = next_;
    next_ = right;

    // 重建 n-ary
    build_nary_();
    right->build_nary_();
    return right;
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