#include "SegmentedBlock.h"
#include <algorithm>
#include <assert.h>

// ========================= 构造/析构 =========================
SegmentedBlock::SegmentedBlock()
    : status_(BlockStatus::ACTIVE),
      min_key_(UINT64_MAX),
      reserved_count_(0),
      committed_count_(0)
{
    for (size_t i = 0; i < kMaxPTBs; ++i)
    {
        ptb_pointers_[i].store(nullptr, std::memory_order_relaxed);
    }
}

// 析构时释放所有 PTB
SegmentedBlock::~SegmentedBlock()
{
    for (size_t i = 0; i < kMaxPTBs; ++i)
    {
        delete ptb_pointers_[i].load();
    }
}

// ========================= 写入接口 =========================
// 在当前分段块中顺序追加一条 KV
bool SegmentedBlock::append_ordered(Key k, Value v)
{
    // 1. 状态检查 (保持不变)
    if (status_.load(std::memory_order_acquire) != BlockStatus::ACTIVE)
        return false;

    // 2. 移除冗余的 pre-check (采纳你的建议 3)
    // if (reserved_count_.load(std::memory_order_relaxed) >= kMaxPTBs) return false;

    // 3. 获取槽位 (保持不变)
    int slot = get_or_create_slot_for_this_thread_();
    if (slot < 0)
        return false;

    // 4. 【关键修改】只获取、不创建 PTB
    // 使用显式的 acquire load (采纳你的建议 2)
    PerThreadDataBlock *ptb = ptb_pointers_[slot].load(std::memory_order_acquire);

    // 断言：此时 ptb 必须已经被 get_or_create... 初始化好了
    assert(ptb != nullptr);

    // 5. 插入数据 (逻辑基本不变)
    if (ptb->IsFull())
        return false;

    if (!ptb->Insert(k, v)) // Insert 内部会更新 max_key
        return false;

    // 6. 更新 min_key_ (保持不变)
    Key old_min = min_key_.load(std::memory_order_relaxed);
    while (k < old_min &&
           !min_key_.compare_exchange_weak(old_min, k,
                                           std::memory_order_release,
                                           std::memory_order_relaxed))
    {
    }

    // 7. 移除错误的 committed_count_ 更新，这个计数的意义在无锁下已经不明确
    // committed_count_.fetch_add(1);

    // 8. 触发 seal (保持不变)
    if (ptb->IsFull())
        should_seal_.store(true, std::memory_order_release);

    return true;
}

// ========================= 状态管理 =========================
// 将段状态从 ACTIVE → CONVERT，用于封印
void SegmentedBlock::seal()
{
    BlockStatus expected = BlockStatus::ACTIVE;
    status_.compare_exchange_strong(expected, BlockStatus::CONVERT,
                                    std::memory_order_acq_rel,
                                    std::memory_order_acquire);
}

// ========================= 数据收集 =========================
// 收集并排序本段所有 PTB 的数据
std::vector<KVPair> SegmentedBlock::collect_and_sort_data()
{
    std::lock_guard<std::mutex> g(lock_);

    if (status_.load(std::memory_order_acquire) == BlockStatus::ACTIVE)
        seal(); // 若还未封印，先封印

    std::vector<KVPair> all_data;
    size_t total_entries = 0;

    // 第一次遍历：计算总容量
    for (size_t i = 0; i < kMaxPTBs; ++i)
    {
        // 【关键修改】使用 acquire load
        PerThreadDataBlock *ptb = ptb_pointers_[i].load(std::memory_order_acquire);
        if (ptb)
        {
            total_entries += ptb->GetNumEntries();
        }
    }
    all_data.reserve(total_entries);

    // 第二次遍历：收集数据
    for (size_t i = 0; i < kMaxPTBs; ++i)
    {
        // 【关键修改】使用 acquire load
        PerThreadDataBlock *ptb = ptb_pointers_[i].load(std::memory_order_acquire);
        if (ptb)
        {
            const KVPair *ptb_data = ptb->GetData();
            size_t n = ptb->GetNumEntries();
            all_data.insert(all_data.end(), ptb_data, ptb_data + n);
        }
    }

    std::sort(all_data.begin(), all_data.end(),
              [](const KVPair &a, const KVPair &b)
              { return a.key < b.key; });

    // 别忘了析构函数也需要修改
    // for (size_t i = 0; i < kMaxPTBs; ++i) { delete ptb_pointers_[i]; }
    // 应该改为：
    // for (size_t i = 0; i < kMaxPTBs; ++i) { delete ptb_pointers_[i].load(); }

    return all_data;
}

// ========================= 内部辅助 =========================

// 定义一个结构体来持有缓存
struct ThreadSlotCache
{
    const SegmentedBlock *owner = nullptr;
    int slot = -1;
};

// 获取或为当前线程分配 PTB 槽位
int SegmentedBlock::get_or_create_slot_for_this_thread_()
{
    // Step 1: 【关键修改】使用与实例绑定的线程本地缓存
    static thread_local ThreadSlotCache tls_cache;

    if (tls_cache.owner == this && tls_cache.slot != -1)
    {
        // 确保缓存没有失效 (虽然在当前逻辑下不会，但这是好习惯)
        if (ptb_pointers_[tls_cache.slot].load(std::memory_order_relaxed) != nullptr)
        {
            return tls_cache.slot;
        }
    }

    // Step 2: 原子预定一个槽位 (不变)
    size_t slot_idx = reserved_count_.fetch_add(1, std::memory_order_relaxed);

    if (slot_idx >= kMaxPTBs)
    {
        return -1;
    }

    // Step 3: CAS 初始化，安全地发布 PTB
    // 先检查槽位是否已被其他线程初始化
    PerThreadDataBlock *ptb = ptb_pointers_[slot_idx].load(std::memory_order_acquire);
    if (ptb == nullptr)
    {
        // 我们是第一个到达此槽位的线程，尝试初始化它
        auto new_ptb = new PerThreadDataBlock();
        PerThreadDataBlock *expected_nullptr = nullptr;

        // 尝试用我们的新 PTB 去原子地替换 nullptr
        if (ptb_pointers_[slot_idx].compare_exchange_strong(expected_nullptr, new_ptb, std::memory_order_release, std::memory_order_relaxed))
        {
            // CAS 成功，我们成功初始化了该槽位
            ptb = new_ptb;
        }
        else
        {
            // CAS 失败，意味着在我们创建 new_ptb 和执行 CAS 之间，
            // 另一个线程已经抢先初始化了该槽位。
            delete new_ptb;         // 必须释放我们创建但未使用的内存，防止泄漏
            ptb = expected_nullptr; // expected 会被 CAS 更新为实际值
        }
    }

    // Step 4: 【关键修改】更新与实例绑定的缓存
    tls_cache.owner = this;
    tls_cache.slot = static_cast<int>(slot_idx);
    return tls_cache.slot;
}

bool SegmentedBlock::is_completely_empty() const noexcept
{
    for (size_t i = 0; i < kMaxPTBs; ++i)
    {
        // 指针用 acquire 读取，保证看到 PTB 构造后的状态
        PerThreadDataBlock *ptb =
            ptb_pointers_[i].load(std::memory_order_acquire);
        if (ptb && ptb->GetNumEntries() > 0)
        {
            return false;
        }
    }
    return true;
}