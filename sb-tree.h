#include <mutex>
#include <cstdint>
#include <atomic>
#include <algorithm>
#include <iterator>

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

// 主类
class SBTree
{
public:
    void insert(Key key, Value value);

private:
    Key max_key;
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

private:
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