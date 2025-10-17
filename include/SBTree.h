// SBTree.h  (modified: add advance_to_next_window declaration)
#pragma once

#include <vector>
#include <atomic>
#include <mutex>
#include <memory>
#include <thread>
#include <condition_variable>
#include <deque>
#include <unordered_map>

#include "KVPair.h"
#include "DataBlock.h"
#include "PerThreadDataBlock.h"
#include "SearchLayer.h"
#include "SearchNode.h"
#include "SegmentedBlock.h"

// SBTree with windowed segmented blocks and delayed-data handling
class SBTree
{
public:
    SBTree();
    ~SBTree();

    // basic API
    void insert(Key k, Value v);
    bool find(Key k, Value &out) const;
    size_t scan(Key l, Key r, std::vector<Value> &out) const;

    // compatibility
    bool lookup(Key k, Value *out);
    void flush();

    std::size_t index_levels() const;

private:
    // helpers
    DataBlock *find_candidate_(Key k) const;

    // delayed insert: locate target DataBlock and insert in-order; may split
    bool insert_delayed_(Key k, Value v);

    // --- SearchNode helpers (new index path) ---
    bool sn_insert_block_(DataBlock *block);
    bool sn_handle_split_(std::vector<SearchNode *> &path,
                          std::size_t child_idx,
                          std::unique_ptr<SearchNode> new_child,
                          Key split_key);

    // conversion helpers: collect pairs from a SegmentedBlock
    std::vector<KVPair> collect_pairs_from_segment(SegmentedBlock *seg);

    // build DataBlocks from merged KV pairs
    std::vector<DataBlock *> build_blocks_from_pairs(std::vector<KVPair> &pairs);

    // publish (attach and append_run)
    void publish_blocks(uint64_t window_id, std::vector<DataBlock *> &blocks);

    // enqueue for background conversion
    void enqueue_segment_for_conversion(SegmentedBlock *seg);

    // background worker
    void background_loop();

    // mapping key -> window
    uint64_t key_to_window(Key k) const { return static_cast<uint64_t>(k) / WINDOW_SIZE; }

    // *** MODIFIED: helper to advance to next window keeping CAS semantics
    void advance_to_next_window(SegmentedBlock *old_seg);

    // members
    Key max_key_{0};

    std::atomic<SegmentedBlock *> shortcut_{nullptr};

    // published data chain
    DataBlock *data_head_;
    DataBlock *data_tail_;
    std::mutex data_tail_mutex_;
    std::mutex data_layer_mutex_; // protect delayed insert/split on data layer

    // search layer
    // 过渡期同时保留：老的 SearchLayer 与新的 SearchNode 根
    SearchLayer search_;
    std::unique_ptr<SearchNode> sn_root_;
    mutable std::mutex search_mutex_;

    // conversion queue (seg + window_id)
    struct PendingSeg
    {
        SegmentedBlock *seg;
        uint64_t window_id;
    };
    std::deque<PendingSeg> convert_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;

    // delayed map: window_id -> vector<KVPair>
    std::unordered_map<uint64_t, std::vector<KVPair>> delayed_map_;
    std::mutex delayed_mutex_;

    // background thread
    std::atomic<bool> stop_writer_{false};
    std::thread bg_thread_;

    // last published window (monotonic)
    std::atomic<uint64_t> last_published_window_{0};

    // parameters
    static constexpr uint64_t WINDOW_SIZE = 1000000ULL;
    size_t writer_batch_wait_ms_ = 20;

    // --- 新索引参数 ---
    static constexpr std::size_t SN_FANOUT = 64; // 与 SearchLayer 一致的容量
};
