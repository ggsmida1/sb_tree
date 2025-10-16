#include <iostream>
#include <vector>
#include <thread>
#include <cstdint>
#include <cstddef>
#include <atomic>
#include <numeric>
#include <mutex>

// =================================================================
// 1. 数据结构定义
// =================================================================

using Key = uint64_t;
using Value = uint64_t;

struct KVPair
{
    Key key;
    Value value;
};

class PerThreadDataBlock
{
public: // <--- 将 private 改为 public 或者将 kCapacity 移到 public 部分
    // ====================== 第 1 处修改 ======================
    // 将 kCapacity 设为公有，以便外部可以访问
    static constexpr size_t kCapacity = 10000;
    // =========================================================

    PerThreadDataBlock() : num_entries_(0) {}

    bool Insert(Key key, Value value)
    {
        if (IsFull())
        {
            return false;
        }
        data_[num_entries_] = {key, value};
        num_entries_++;
        return true;
    }

    bool IsFull() const
    {
        return num_entries_ >= kCapacity;
    }

    size_t GetNumEntries() const
    {
        return num_entries_;
    }

    const KVPair *GetData() const
    {
        return data_;
    }

private:
    size_t num_entries_ = 0;
    KVPair data_[kCapacity];
};

// =================================================================
// 2. 多线程测试逻辑
// =================================================================

const int NUM_THREADS = 4;
const int MAX_TIMESTAMP = 40000;

std::atomic<bool> stop_flag{false};
std::mutex mtx;

// void worker(int thread_id, PerThreadDataBlock &ptb)
// {
//     for (int ts = 1; ts <= MAX_TIMESTAMP; ++ts)
//     {

//         if (stop_flag.load(std::memory_order_relaxed))
//         {
//             break;
//         }

//         if ((ts - 1) % NUM_THREADS == thread_id)
//         {

//             std::cout << " thread id is: " << thread_id << "\n";

//             std::lock_guard<std::mutex> lock(mtx);

//             if (stop_flag.load(std::memory_order_relaxed))
//             {
//                 break;
//             }

//             if (ptb.Insert(ts, ts * 10))
//             {
//                 if (ptb.IsFull())
//                 {
//                     // ====================== 第 2 处修改 ======================
//                     // 使用 PerThreadDataBlock::kCapacity 来正确访问
//                     std::cout << "--- 线程 " << thread_id << " 的数据块已满 (达到 " << PerThreadDataBlock::kCapacity << " 条)，设置停止标志! ---\n\n";
//                     // =========================================================
//                     stop_flag.store(true, std::memory_order_relaxed);
//                 }
//             }
//         }
//     }
// }

// void worker(int thread_id, PerThreadDataBlock &ptb)
// {
//     // 让每个线程处理连续的时间戳范围
//     int start_ts = thread_id * (MAX_TIMESTAMP / NUM_THREADS) + 1;
//     int end_ts = (thread_id == NUM_THREADS - 1) ? MAX_TIMESTAMP : (thread_id + 1) * (MAX_TIMESTAMP / NUM_THREADS);

//     for (int ts = start_ts; ts <= end_ts; ++ts)
//     {
//         if (stop_flag.load(std::memory_order_relaxed))
//         {
//             break;
//         }

//         // 直接插入，不需要锁
//         if (ptb.Insert(ts, ts * 10))
//         {
//             if (ptb.IsFull())
//             {
//                 std::cout << "--- 线程 " << thread_id << " 的数据块已满 (达到 "
//                           << PerThreadDataBlock::kCapacity << " 条)，设置停止标志! ---\n";
//                 stop_flag.store(true, std::memory_order_relaxed);
//                 break;
//             }
//         }
//     }
// }

void worker(int thread_id, PerThreadDataBlock &ptb)
{
    int count = 0;
    for (int ts = 1; ts <= MAX_TIMESTAMP; ++ts)
    {
        if (stop_flag.load(std::memory_order_relaxed))
        {
            break;
        }

        if ((ts - 1) % NUM_THREADS == thread_id)
        {
            count++;
            if (ptb.Insert(ts, ts * 10))
            {
                if (ptb.IsFull())
                {
                    std::cout << "--- 线程 " << thread_id << " 的数据块已满 (达到 " << PerThreadDataBlock::kCapacity << " 条)，设置停止标志! ---\n";
                    stop_flag.store(true, std::memory_order_relaxed);
                }
            }
        }
    }
    std::cout << "线程 " << thread_id << " 处理了 " << count << " 个时间戳\n";
}

int main()
{
    std::cout << "启动 " << NUM_THREADS << " 个线程进行交错写入测试...\n";
    // (可选的优化) 这里也使用 PerThreadDataBlock::kCapacity
    std::cout << "每个线程的数据块容量为 " << PerThreadDataBlock::kCapacity << " 条\n";
    std::cout << "当任一数据块写满时，所有线程将停止写入。\n\n";

    std::vector<PerThreadDataBlock> all_blocks(NUM_THREADS);
    std::vector<std::thread> threads;

    for (int i = 0; i < NUM_THREADS; ++i)
    {
        threads.emplace_back(worker, i, std::ref(all_blocks[i]));
    }

    for (auto &t : threads)
    {
        t.join();
    }

    std::cout << "所有线程已停止。开始检查每个线程写入的数据：\n";
    std::cout << "==============================================\n";

    for (int i = 0; i < NUM_THREADS; ++i)
    {
        const auto &ptb = all_blocks[i];
        std::cout << "\n线程 " << i << " (共写入 " << ptb.GetNumEntries() << " 条数据)\n";
        if (ptb.GetNumEntries() > 0)
        {
            const KVPair *data = ptb.GetData();
            std::cout << "  (示例 Key: " << data[0].key
                      << ", ..., " << data[ptb.GetNumEntries() - 1].key << ")\n";
        }
    }

    std::cout << "\n==============================================\n";
    std::cout << "检查完毕。\n";

    return 0;
}