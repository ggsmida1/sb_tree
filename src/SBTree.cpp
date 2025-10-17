#include "SBTree.h"

#include <algorithm>
#include <iostream>
#include <chrono>
#include <cassert>

// create initial segmented block for given window
static SegmentedBlock *create_segmented_block_for_window(uint64_t wid, size_t capacity = 128)
{
    return new SegmentedBlock(capacity, wid);
}

SBTree::SBTree()
    : max_key_{0},
      shortcut_(nullptr),
      data_head_(nullptr),
      data_tail_(nullptr),
      search_()
{
    // start with window 0
    SegmentedBlock *init = create_segmented_block_for_window(0);
    shortcut_.store(init, std::memory_order_release);
    last_published_window_.store((uint64_t)0);

    stop_writer_.store(false);
    bg_thread_ = std::thread(&SBTree::background_loop, this);
}

SBTree::~SBTree()
{
    // stop background thread
    stop_writer_.store(true);
    queue_cv_.notify_one();
    if (bg_thread_.joinable())
        bg_thread_.join();

    // drain remaining queued segments synchronously
    {
        std::lock_guard<std::mutex> lk(queue_mutex_);
        while (!convert_queue_.empty())
        {
            PendingSeg ps = convert_queue_.front();
            convert_queue_.pop_front();
            auto pairs = collect_pairs_from_segment(ps.seg);
            // merge with delayed for that window
            {
                std::lock_guard<std::mutex> dl(delayed_mutex_);
                auto it = delayed_map_.find(ps.window_id);
                if (it != delayed_map_.end())
                {
                    // move delayed entries into pairs
                    pairs.insert(pairs.end(), it->second.begin(), it->second.end());
                    delayed_map_.erase(it);
                }
            }
            if (!pairs.empty())
            {
                std::sort(pairs.begin(), pairs.end(), [](const KVPair &a, const KVPair &b)
                          { return a.key < b.key; });
                auto blocks = build_blocks_from_pairs(pairs);
                publish_blocks(ps.window_id, blocks);
            }
            delete ps.seg;
        }
    }

    // delete current shortcut
    SegmentedBlock *cur = shortcut_.load(std::memory_order_acquire);
    if (cur)
        delete cur;

    // delete DataBlock chain
    DataBlock *p = data_head_;
    while (p)
    {
        DataBlock *nxt = p->next();
        delete p;
        p = nxt;
    }
}

void SBTree::insert(Key k, Value v)
{
    uint64_t target_wid = key_to_window(k);

    while (true)
    {
        SegmentedBlock *seg = shortcut_.load(std::memory_order_acquire);
        uint64_t seg_wid = seg ? seg->window_id() : 0;

        if (target_wid < seg_wid)
        {
            // delayed: belongs to an already-advancing window -> buffer it
            std::lock_guard<std::mutex> lk(delayed_mutex_);
            delayed_map_[target_wid].push_back(KVPair{k, v});
            return;
        }
        else if (target_wid > seg_wid)
        {
            // key belongs to future window: try to install a seg for target_wid
            SegmentedBlock *newseg = create_segmented_block_for_window(target_wid, seg ? seg->capacity() : 128);
            if (shortcut_.compare_exchange_strong(seg, newseg, std::memory_order_acq_rel))
            {
                // succeeded: enqueue old seg for conversion
                if (seg)
                    enqueue_segment_for_conversion(seg);
                // continue to insert (loop will reload current seg)
            }
            else
            {
                // failed: someone else installed; delete ours and retry
                delete newseg;
            }
            continue;
        }
        else
        {
            // target_wid == seg_wid -> normal fast path
            PerThreadDataBlock *block = seg->get_or_install_block_for_current_thread();
            if (!block)
            {
                // seg full: create next window seg (advance 1 window)
                uint64_t new_wid = seg_wid + 1;
                SegmentedBlock *newseg = create_segmented_block_for_window(new_wid, seg->capacity());
                if (shortcut_.compare_exchange_strong(seg, newseg, std::memory_order_acq_rel))
                {
                    enqueue_segment_for_conversion(seg);
                    // inserted into new seg? we'll loop and try again
                }
                else
                {
                    delete newseg;
                }
                continue;
            }

            // Try insert into per-thread buffer
            if (block->Insert(k, v))
            {
                if (k > max_key_)
                    max_key_ = k;
                return;
            }
            else
            {
                // buffer full -> advance window by 1
                uint64_t new_wid = seg_wid + 1;
                SegmentedBlock *newseg = create_segmented_block_for_window(new_wid, seg->capacity());
                if (shortcut_.compare_exchange_strong(seg, newseg, std::memory_order_acq_rel))
                {
                    enqueue_segment_for_conversion(seg);
                    return; // we enqueued old seg; caller's insert considered done
                }
                else
                {
                    delete newseg;
                    continue;
                }
            }
        }
    } // end while
}

bool SBTree::find(Key k, Value &out) const
{
    DataBlock *cand = find_candidate_(k);
    if (!cand)
        return false;
    return cand->find(k, out);
}

size_t SBTree::scan(Key l, Key r, std::vector<Value> &out) const
{
    if (l > r)
        return 0;
    DataBlock *cur = find_candidate_(l);
    size_t added = 0;
    while (cur)
    {
        size_t n = cur->size();
        for (size_t i = 0; i < n; ++i)
        {
            KVPair kv = cur->get_entry(i);
            if (kv.key >= l && kv.key <= r)
            {
                out.push_back(kv.value);
                ++added;
            }
            if (kv.key > r)
                break;
        }
        if (cur->max_key() > r)
            break;
        cur = cur->next();
    }
    return added;
}

bool SBTree::lookup(Key k, Value *out) { return find(k, *out); }

void SBTree::flush()
{
    SegmentedBlock *seg = shortcut_.load(std::memory_order_acquire);
    if (seg)
    {
        // try to install next window so seg gets flushed
        uint64_t seg_wid = seg->window_id();
        SegmentedBlock *newseg = create_segmented_block_for_window(seg_wid + 1, seg->capacity());
        if (shortcut_.compare_exchange_strong(seg, newseg, std::memory_order_acq_rel))
        {
            enqueue_segment_for_conversion(seg);
        }
        else
        {
            delete newseg;
        }
    }
}

DataBlock *SBTree::find_candidate_(Key k) const
{
    return search_.find_candidate(k);
}

// collect KV pairs from a SegmentedBlock (takes and deletes per-thread buffers)
std::vector<KVPair> SBTree::collect_pairs_from_segment(SegmentedBlock *seg)
{
    std::vector<KVPair> out;
    out.reserve(1024);
    seg->for_each_registered_ptr([&out](PerThreadDataBlock *p)
                                 {
        if (!p) return;
        size_t n = p->GetNumEntries();
        const KVPair* src = p->GetData();
        for (size_t i = 0; i < n; ++i) out.push_back(src[i]);
        // free per-thread buffer (ownership transferred)
        delete p; });
    return out;
}

std::vector<DataBlock *> SBTree::build_blocks_from_pairs(std::vector<KVPair> &pairs)
{
    std::vector<DataBlock *> blocks;
    if (pairs.empty())
        return blocks;
    std::sort(pairs.begin(), pairs.end(), [](const KVPair &a, const KVPair &b)
              { return a.key < b.key; });
    size_t idx = 0;
    while (idx < pairs.size())
    {
        DataBlock *b = new DataBlock();
        size_t taken = b->build_from_sorted(pairs.data() + idx, pairs.size() - idx);
        blocks.push_back(b);
        idx += taken;
    }
    return blocks;
}

void SBTree::publish_blocks(uint64_t window_id, std::vector<DataBlock *> &blocks)
{
    if (blocks.empty())
        return;

    // attach to data list
    {
        std::lock_guard<std::mutex> lk(data_tail_mutex_);
        if (!data_head_)
        {
            data_head_ = blocks.front();
            data_tail_ = blocks.back();
        }
        else
        {
            data_tail_->set_next(blocks.front());
            data_tail_ = blocks.back();
        }
    }

    // append to search layer
    {
        std::lock_guard<std::mutex> lk(search_mutex_);
        // sanity: ensure window monotonicity (for debug)
        uint64_t last = last_published_window_.load();
        if (window_id < last)
        {
            // This shouldn't happen if the background loop orders by window_id and merges delayed data.
            // We still allow it but print a warning for debugging.
            std::cerr << "Warning: publishing window " << window_id << " after last " << last << std::endl;
        }
        search_.append_run(blocks);
        last_published_window_.store(window_id);
    }
}

void SBTree::enqueue_segment_for_conversion(SegmentedBlock *seg)
{
    uint64_t wid = seg->window_id();
    {
        std::lock_guard<std::mutex> lk(queue_mutex_);
        convert_queue_.push_back(PendingSeg{seg, wid});
    }
    queue_cv_.notify_one();
}

// background loop: drain queue, group by window_id, merge with delayed_map_, build blocks and publish in window order
void SBTree::background_loop()
{
    while (!stop_writer_.load())
    {
        std::vector<PendingSeg> batch;
        {
            std::unique_lock<std::mutex> lk(queue_mutex_);
            if (convert_queue_.empty())
            {
                queue_cv_.wait_for(lk, std::chrono::milliseconds(writer_batch_wait_ms_));
            }
            while (!convert_queue_.empty())
            {
                batch.push_back(convert_queue_.front());
                convert_queue_.pop_front();
            }
        }
        if (batch.empty())
            continue;

        // group pairs by window_id
        std::unordered_map<uint64_t, std::vector<KVPair>> grouped;
        grouped.reserve(batch.size());

        for (auto &ps : batch)
        {
            // collect
            std::vector<KVPair> pairs = collect_pairs_from_segment(ps.seg);
            if (!pairs.empty())
            {
                auto &vec = grouped[ps.window_id];
                // move pairs into grouped
                vec.insert(vec.end(), pairs.begin(), pairs.end());
            }
            delete ps.seg;
        }

        // incorporate delayed_map_ entries for these windows (and optionally adjacent ones)
        {
            std::lock_guard<std::mutex> dl(delayed_mutex_);
            for (auto &kv : delayed_map_)
            {
                uint64_t wid = kv.first;
                if (!kv.second.empty())
                {
                    auto &vec = grouped[wid];
                    vec.insert(vec.end(), kv.second.begin(), kv.second.end());
                }
            }
            // clear delayed_map_ entirely because we merged all delayed entries into grouped.
            // This is conservative: if delayed_map_ has windows that were not in grouped,
            // we still merged them so they'll be published now.
            delayed_map_.clear();
        }

        // sort window ids and publish in order
        std::vector<uint64_t> wids;
        wids.reserve(grouped.size());
        for (auto &e : grouped)
            wids.push_back(e.first);
        std::sort(wids.begin(), wids.end());

        for (uint64_t wid : wids)
        {
            auto &pairs = grouped[wid];
            if (pairs.empty())
                continue;
            // build data blocks for this window
            std::vector<DataBlock *> blocks = build_blocks_from_pairs(pairs);
            if (!blocks.empty())
            {
                publish_blocks(wid, blocks);
            }
            else
            {
                // nothing to publish
            }
        }
    }

    // On shutdown, drain any remaining items similarly
    std::vector<PendingSeg> remaining;
    {
        std::lock_guard<std::mutex> lk(queue_mutex_);
        while (!convert_queue_.empty())
        {
            remaining.push_back(convert_queue_.front());
            convert_queue_.pop_front();
        }
    }
    if (!remaining.empty())
    {
        std::unordered_map<uint64_t, std::vector<KVPair>> grouped;
        for (auto &ps : remaining)
        {
            auto pairs = collect_pairs_from_segment(ps.seg);
            if (!pairs.empty())
                grouped[ps.window_id].insert(grouped[ps.window_id].end(), pairs.begin(), pairs.end());
            delete ps.seg;
        }
        {
            std::lock_guard<std::mutex> dl(delayed_mutex_);
            for (auto &kv : delayed_map_)
            {
                grouped[kv.first].insert(grouped[kv.first].end(), kv.second.begin(), kv.second.end());
            }
            delayed_map_.clear();
        }
        std::vector<uint64_t> wids;
        for (auto &e : grouped)
            wids.push_back(e.first);
        std::sort(wids.begin(), wids.end());
        for (uint64_t wid : wids)
        {
            auto &pairs = grouped[wid];
            if (pairs.empty())
                continue;
            auto blocks = build_blocks_from_pairs(pairs);
            if (!blocks.empty())
                publish_blocks(wid, blocks);
        }
    }
}
