#include <mutex>
#include <cstdint>
#include <atomic>
#include <algorithm>
#include <iterator>
#include <vector>

using Key = uint64_t;
using Value = uint64_t;

// 定义键值对
struct KVPair
{
    Key key;
    Value value;
};

// 分段块中,定义块的状态。
enum class BlockStatus : uint8_t
{
    // 正在接受插入
    ACTIVE,
    // 正在被转换,不接受插入
    CONVERT,
    // 转换完成
    CONVERTED,
};

// 每线程数据块类
class PerThreadDataBlock
{
public:
    // 构造函数,初始化块
    PerThreadDataBlock() : num_entries_(0) {}

    //  尝试向块中追加一个键值对
    //  如果成功,返回true;如果块已满,返回false
    bool Insert(Key key, Value value)
    {
        if (IsFull())
        {
            return false;
        }
        // 直接在数组末尾追加新的键值对
        data_[num_entries_] = {key, value};

        // 每次插入时都更新最大键,因为key单增,直接更新即可
        max_key_ = key;

        num_entries_++;
        return true;
    }

    // 检查块是否已满
    bool IsFull() const
    {
        return num_entries_ >= kCapacity;
    }

    // --- 以下是为“块转换”过程提供的辅助函数 ---

    // 获取当前存储的条目数量。
    size_t GetNumEntries() const
    {
        return num_entries_;
    }

    // 获取数据数组的只读指针。
    const KVPair *GetData() const
    {
        return data_;
    }

private:
    // 设置块的大小为4KB
    static constexpr size_t kBlockSize = 4096;

    // 成员变量

    // 记录当前块中存了多少键值对
    size_t num_entries_;
    // 当前块中所有键的最大值
    Key max_key_;

    // 减去元数据大小后，计算块实际能容纳多少个KVPair。
    static constexpr size_t kMetadataSize = sizeof(num_entries_) + sizeof(max_key_);
    static constexpr size_t kCapacity = (kBlockSize - kMetadataSize) / sizeof(KVPair);

    // 键值对
    KVPair data_[kCapacity];
};

// 分段块
class SegmentedBlock
{
public:
    SegmentedBlock() : status_(BlockStatus::ACTIVE),
                       min_key_(UINT64_MAX),
                       next_block_(nullptr),
                       reserved_count_(0),
                       committed_count_(0)
    {
        std::fill(std::begin(ptb_pointers_), std::end(ptb_pointers_), nullptr);
    }

    // 顺序插入：为“当前线程”获取/创建一个 PTB，然后尝试写入。
    // 返回 true 表示写入成功；返回 false 表示本分段块已不再接受写入
    // （例如没有空槽位或该线程的 PTB 已满，外层应切换到新分段块）。
    bool append_ordered(Key k, Value v)
    {
        int slot = get_or_create_slot_for_this_thread_();
        if (slot < 0)
            return false; // 没有可用 PTB 槽位，需要切换到新分段块

        PerThreadDataBlock *ptb = ptb_pointers_[static_cast<size_t>(slot)];
        if (!ptb)
            return false; // 理论不该发生

        if (!ptb->Insert(k, v))
        {
            // 该线程的 PTB 已满，本分段块对该线程来说写不动了
            return false;
        }
        // 第一次写入或更小的 key 时更新 min_key_
        if (k < min_key_)
            min_key_ = k;
        return true;
    }

private:
    // 为“当前线程”分配一个专属 PTB 槽位（第一次调用时分配）。
    // 返回槽位下标 [0, kMaxPTBs)，失败返回 -1。
    int get_or_create_slot_for_this_thread_()
    {
        // 线程本地缓存槽位，下次直接复用
        static thread_local int tls_slot = -1;
        if (tls_slot >= 0)
            return tls_slot;

        // 线性找空槽，简单起见用互斥保护（后续需要可再做无锁/原子优化）
        std::lock_guard<std::mutex> g(lock_);
        for (size_t i = 0; i < kMaxPTBs; ++i)
        {
            if (ptb_pointers_[i] == nullptr)
            {
                ptb_pointers_[i] = new PerThreadDataBlock();
                ++reserved_count_;
                ++committed_count_; // 这里把“占用即提交”合并处理（只做顺序插入足够）
                tls_slot = static_cast<int>(i);
                return tls_slot;
            }
        }
        // 没有空位了，本分段块需要外层切换到新分段块
        return -1;
    }

    // 记录块状态
    std::atomic<BlockStatus> status_;
    // 记录块最小Key;
    Key min_key_;
    // 指向数据层下一个块的指针
    SegmentedBlock *next_block_;
    // 用于块转换过程中的锁
    std::mutex lock_;

    // 用于PTB分配的原子计数器
    std::atomic<size_t> reserved_count_;
    std::atomic<size_t> committed_count_;

    // 指向每线程数据块的指针
    static constexpr size_t kMaxPTBs = 128;
    PerThreadDataBlock *ptb_pointers_[kMaxPTBs];
};

// 数据块
class DataBlock
{
public:
    // --- 状态枚举 ---
    enum class Status : uint8_t
    {
        READY,
        SPLITTING
    };

    // 构造函数
    DataBlock()
        : status_(Status::READY),
          min_key_(std::numeric_limits<Key>::max()),
          next_(nullptr),
          lock_(0),
          count_(0)
    {
        for (size_t i = 0; i < kBuckets; ++i)
            nary_[i] = std::numeric_limits<Key>::max();
    }

    // 由“已排序”的 KV 构建；返回写入条数（<= kCapacity）
    size_t build_from_sorted(const KVPair *src, size_t n);

    // 查找：先 N-ary 桶定位，再桶内线扫
    bool find(Key k, Value &out) const;

    // 扫描：从 startKey 起，最多取 count 个，写入 out；返回实际条数
    size_t scan_from(Key startKey, size_t count, std::vector<Value> &out) const;

    // 访问器
    size_t size() const { return count_; }
    Key min_key() const { return min_key_; }
    DataBlock *next() const { return next_; }
    void set_next(DataBlock *p) { next_ = p; }

private:
    // === 固定大小常量 ===
    static constexpr size_t kBlockSize = 4096; // 整块大小 4KB
    static constexpr size_t kBuckets = 8;      // N-ary 桶数（固定）
    using LockWord = uint32_t;                 // 轻量“锁位”

    // Header 占用字节数
    static constexpr size_t kHeaderSize =
        sizeof(Status) + sizeof(Key) + sizeof(void *) +
        sizeof(LockWord) + sizeof(uint32_t);

    // N-ary 表占用
    static constexpr size_t kNarySize = kBuckets * sizeof(Key);

    // 留给 KV 的空间
    static constexpr size_t kKVBytes =
        (kBlockSize >= kHeaderSize + kNarySize)
            ? (kBlockSize - kHeaderSize - kNarySize)
            : 0;

    static constexpr size_t kOneEntryBytes = sizeof(Key) + sizeof(Value);
    static constexpr size_t kCapacity = kKVBytes / kOneEntryBytes;

    static_assert(kCapacity > 0, "DataBlock capacity must be > 0 under 4KB.");

    // 内部辅助函数
    void build_nary_();
    std::pair<size_t, size_t> bucket_range_(Key k) const;

    // === 元数据字段 ===

    Status status_;   // 状态
    Key min_key_;     // 块内最小 key
    DataBlock *next_; // 下一个数据块
    LockWord lock_;   // 轻量锁
    uint32_t count_;  // 实际条目数

    // === 数据区 ===

    // N-ary表
    Key nary_[kBuckets];
    // Key数组
    Key keys_[kCapacity];
    // Value数组
    Value vals_[kCapacity];
};

// 主类
class SBTree
{
public:
    SBTree() : shortcut_(new SegmentedBlock()) {};
    void insert(Key key, Value value)
    {
        while (true)
        {
            SegmentedBlock *seg = shortcut_.load(std::memory_order_acquire);
            if (seg && seg->append_ordered(key, value))
            {
                // 成功写入
                return;
            }
            // 失败：说明该线程的 PTB 满了或无空槽 ——> 切到新分段块
            SegmentedBlock *new_seg = new SegmentedBlock();

            // 只需要把 shortcut_ 指到新块即可；竞争下用 CAS
            SegmentedBlock *expected = seg;
            if (shortcut_.compare_exchange_weak(
                    expected, new_seg,
                    std::memory_order_acq_rel, std::memory_order_acquire))
            {
                // 切换成功后再试一次写入（应该会成功）
                (void)new_seg->append_ordered(key, value);
                return;
            }
            else
            {
                // 说明别的线程已经切换成功了；我们丢弃自己建的新块
                delete new_seg;
                // 回到 while，重新读新的 shortcut_ 再试
            }
        }
    }

private:
    // 最大键
    Key max_key;

    // 快捷指针,指向分段块
    std::atomic<SegmentedBlock *> shortcut_;
};