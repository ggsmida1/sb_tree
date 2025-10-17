// SBTree.cpp (modified)
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

    // 初始化新的 SearchNode 根（叶子）
    sn_root_ = std::make_unique<SearchNode>(SearchNode::NodeType::Leaf, SN_FANOUT);
}
bool SBTree::IsDelayedData(Key k) const
{
    // 判断：k 小于当前段的最小键 或 小于全局最大键
    SegmentedBlock *seg = shortcut_.load(std::memory_order_acquire);
    Key seg_min = std::numeric_limits<Key>::max();
    if (seg)
    {
        // 估算段最小键：遍历所有注册 PTB 的最小键（需要 PerThreadDataBlock::GetMinKey）
        seg->for_each_registered_ptr([&](PerThreadDataBlock *p) {
            if (!p) return;
            Key mk = p->GetMinKey();
            if (mk != 0 && mk < seg_min)
                seg_min = mk;
        });
    }
    Key global_max = max_key_;
    bool seg_min_known = (seg_min != std::numeric_limits<Key>::max());
    if (seg_min_known && k < seg_min)
        return true;
    if (k < global_max)
        return true;
    return false;
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

// *** MODIFIED: helper - encapsulate advance + enqueue CAS semantics
void SBTree::advance_to_next_window(SegmentedBlock *old_seg)
{
    if (!old_seg)
        return;
    uint64_t new_wid = old_seg->window_id() + 1;
    SegmentedBlock *newseg = create_segmented_block_for_window(new_wid, old_seg->capacity());
    // try to atomically install new seg as shortcut; if succeed, enqueue old for conversion
    SegmentedBlock *expected = old_seg;
    if (shortcut_.compare_exchange_strong(expected, newseg, std::memory_order_acq_rel))
    {
        enqueue_segment_for_conversion(old_seg);
    }
    else
    {
        // someone else advanced; drop ours
        delete newseg;
    }
}

void SBTree::insert(Key k, Value v)
{
    uint64_t target_wid = key_to_window(k);

    while (true)
    {
        // 优先：如果是延迟数据，直接走延迟路径，避免进入段写缓冲
        if (IsDelayedData(k))
        {
            insert_delayed_(k, v);
            return;
        }

        SegmentedBlock *seg = shortcut_.load(std::memory_order_acquire);
        uint64_t seg_wid = seg ? seg->window_id() : 0;

        if (target_wid < seg_wid)
        {
            // 历史窗口：改为直接落地到数据层（有序插入，必要时分裂）
            insert_delayed_(k, v);
            return;
        }
        else if (target_wid > seg_wid)
        {
            // key belongs to future window: try to install a seg for target_wid
            // *** MODIFIED: do NOT allow skipping many windows eagerly.
            // If target_wid is beyond seg_wid+1, buffer into delayed_map_ instead of creating far-future seg.
            if (target_wid > seg_wid + 1)
            {
                // 远未来窗口：直接落地到数据层，避免长时间滞留
                insert_delayed_(k, v);
                return;
            }

            // otherwise attempt to advance by one (to seg_wid+1) or to target_wid (if equals)
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
            // If segment already marked converting, retry to pick up new shortcut
            if (seg->is_converting())
            {
                // queued for conversion, try again to get the new shortcut
                std::this_thread::yield();
                continue;
            }

            PerThreadDataBlock *block = seg->get_or_install_block_for_current_thread();
            if (!block)
            {
                // seg has no available slot: mark converting (first-wins) and advance window
                // *** MODIFIED: use try_mark_converting() to ensure only one thread triggers conversion
                if (seg->try_mark_converting())
                {
                    advance_to_next_window(seg);
                }
                // whether we won or lost, retry loop to get new seg
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
                // buffer full -> instead of immediately creating many small segments,
                // we only mark segment as converting; the first thread to succeed will advance the window.
                // *** MODIFIED: use try_mark_converting() first (first-wins)
                if (seg->try_mark_converting())
                {
                    // this thread is responsible for installing next window and triggering conversion
                    advance_to_next_window(seg);
                }
                // whether we won or not, retry to insert into newly installed segment
                continue;
            }
        }
    } // end while
}

// delayed insert: locate target DataBlock via search layer and data-layer correction,
// then insert in-order; split if full and publish new right block into search layer
bool SBTree::insert_delayed_(Key k, Value v)
{
    // 通过搜索层找候选块
    DataBlock *cur = find_candidate_(k);
    // 若无候选，从数据头开始
    if (!cur)
    {
        // 步骤1：若完全找不到候选，临时缓冲进 delayed_map_ 对应窗口，等待后台发布
        uint64_t wid = key_to_window(k);
        {
            std::lock_guard<std::mutex> lk(delayed_mutex_);
            delayed_map_[wid].push_back(KVPair{k, v});
        }
        return true;
    }

    // 沿链表前进到第一个可能覆盖 k 的块（max_key >= k）
    while (cur && cur->max_key() < k)
        cur = cur->next();

    if (!cur)
    {
        uint64_t wid = key_to_window(k);
        std::lock_guard<std::mutex> lk(delayed_mutex_);
        delayed_map_[wid].push_back(KVPair{k, v});
        return true;
    }

    // 在当前块尝试插入；若失败且下一块存在，尝试下一块
    for (int attempt = 0; attempt < 2 && cur; ++attempt)
    {
        DataBlock *newRight = nullptr;
        bool ok = false;
        {
            std::lock_guard<std::mutex> lk(data_layer_mutex_);
            ok = cur->insert_sorted(k, v, &newRight);
            if (ok)
            {
                if (newRight)
                {
                    // 分裂新块已链接到链表；仅更新新的索引，避免破坏 SearchLayer 的追加顺序
                    sn_insert_block_(newRight);
                }
                return true;
            }
        }
        cur = cur->next();
    }
    return false;
}

bool SBTree::find(Key k, Value &out) const
{
    DataBlock *cur = find_candidate_(k);
    if (!cur)
        return false;

    // 尝试在候选块查找；若未命中且 key 超过当前块范围，则沿链表纠偏到覆盖该 key 的块
    for (;;)
    {
        if (cur->find(k, out))
            return true;

        // 若目标 key 小于当前块最小值，则不存在
        if (k < cur->min_key())
            return false;

        // 若目标 key 大于当前块最大值，沿 next() 前进；否则未命中即不存在
        if (k > cur->max_key())
        {
            cur = cur->next();
            if (!cur)
                return false;
            continue;
        }
        return false;
    }
}

size_t SBTree::scan(Key l, Key r, std::vector<Value> &out) const
{
    if (l > r)
        return 0;
    DataBlock *cur = find_candidate_(l);
    if (!cur)
        cur = data_head_;
    while (cur && cur->max_key() < l)
        cur = cur->next();
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
            // 步骤3：测试期同步等待后台线程处理当前队列
            // 简单等待一小段时间，给 background_loop 充足时间构建与发布
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        else
        {
            delete newseg;
        }
    }
}

DataBlock *SBTree::find_candidate_(Key k) const
{
    // 优先使用成熟的 SearchLayer，确保稳定性
    if (!search_.empty())
    {
        DataBlock *cand = search_.find_candidate(k);
        if (cand)
            return cand;
    }
    // 回退到新的 SearchNode（叶/单层有效），再由链表纠偏兜底
    if (sn_root_)
    {
        DataBlock *cand = sn_root_->FindDataBlock(k);
        if (cand)
            return cand;
    }
    return nullptr;
}

// collect KV pairs from a SegmentedBlock (takes and deletes per-thread buffers)
// *** MODIFIED: Before copying we Freeze each per-thread buffer to avoid races with in-flight writers.
std::vector<KVPair> SBTree::collect_pairs_from_segment(SegmentedBlock *seg)
{
    std::vector<KVPair> out;
    out.reserve(1024);

    // First, freeze all per-thread buffers so writers stop (Freeze sets frozen_ flag)
    seg->for_each_registered_ptr([](PerThreadDataBlock *p)
                                 {
        if (p)
            p->Freeze(); });

    // 步骤4：临时小延迟，降低 Freeze 与 writer 并发窗口风险（测试期）
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

    // Now safely collect data (writers will have observed frozen_ and stopped)
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

        // 同步将 run 的首块插入新的 SearchNode 根（渐进迁移，至少保证可定位）
        if (sn_root_ && !blocks.empty())
        {
            // 将 run 的每个块都插入新索引，保证更好的可定位性
            for (DataBlock *b : blocks)
                sn_insert_block_(b);
        }
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
// *** MODIFIED: Only merge delayed_map_ entries corresponding to windows present in grouped,
// and remove merged items from delayed_map_ instead of clearing entire map.
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
        {
            // 无段转换任务时，主动冲刷 delayed_map_，避免延迟数据饥饿
            // 仅当存在延迟窗口时进行；一次处理有限数量窗口以控制抖动
            std::unordered_map<uint64_t, std::vector<KVPair>> delayed_take;
            std::vector<uint64_t> wid_list;
            {
                std::lock_guard<std::mutex> dl(delayed_mutex_);
                if (!delayed_map_.empty())
                {
                    // 取前若干窗口（按窗口 id 升序）
                    wid_list.reserve(delayed_map_.size());
                    for (auto &e : delayed_map_)
                        wid_list.push_back(e.first);
                    std::sort(wid_list.begin(), wid_list.end());
                    const size_t kMaxWindowsPerTick = 4;
                    if (wid_list.size() > kMaxWindowsPerTick)
                        wid_list.resize(kMaxWindowsPerTick);
                    for (uint64_t wid : wid_list)
                    {
                        auto it = delayed_map_.find(wid);
                        if (it != delayed_map_.end() && !it->second.empty())
                        {
                            delayed_take.emplace(wid, std::move(it->second));
                            delayed_map_.erase(it);
                        }
                    }
                }
            }

            if (!delayed_take.empty())
            {
                // 将抽取的延迟窗口构建并发布
                std::vector<uint64_t> wids;
                wids.reserve(delayed_take.size());
                for (auto &e : delayed_take)
                    wids.push_back(e.first);
                std::sort(wids.begin(), wids.end());

                for (uint64_t wid : wids)
                {
                    auto &pairs = delayed_take[wid];
                    if (pairs.empty())
                        continue;
                    std::sort(pairs.begin(), pairs.end(), [](const KVPair &a, const KVPair &b)
                              { return a.key < b.key; });
                    std::vector<DataBlock *> blocks = build_blocks_from_pairs(pairs);
                    if (!blocks.empty())
                        publish_blocks(wid, blocks);
                }
            }
            continue;
        }

        // group pairs by window_id
        std::unordered_map<uint64_t, std::vector<KVPair>> grouped;
        grouped.reserve(batch.size());

        std::vector<uint64_t> grouped_wids;
        grouped_wids.reserve(batch.size());

        for (auto &ps : batch)
        {
            // collect
            std::vector<KVPair> pairs = collect_pairs_from_segment(ps.seg);
            if (!pairs.empty())
            {
                auto &vec = grouped[ps.window_id];
                vec.insert(vec.end(), pairs.begin(), pairs.end());
            }
            grouped_wids.push_back(ps.window_id);
            delete ps.seg;
        }

        // dedupe grouped_wids
        std::sort(grouped_wids.begin(), grouped_wids.end());
        grouped_wids.erase(std::unique(grouped_wids.begin(), grouped_wids.end()), grouped_wids.end());

        // incorporate delayed_map_ entries for these windows only
        {
            std::lock_guard<std::mutex> dl(delayed_mutex_);
            for (uint64_t wid : grouped_wids)
            {
                auto it = delayed_map_.find(wid);
                if (it != delayed_map_.end() && !it->second.empty())
                {
                    auto &vec = grouped[wid];
                    vec.insert(vec.end(), it->second.begin(), it->second.end());
                    // remove merged entries
                    delayed_map_.erase(it);
                }
            }
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

// --- SearchNode helpers ---
bool SBTree::sn_insert_block_(DataBlock *block)
{
    if (!block || !sn_root_)
        return false;
    // 简化：仅插入叶层，必要时在叶满时执行一次分裂并创建新根
    if (sn_root_->InsertDataBlock(block->min_key(), block))
        return true;

    // 叶子满：分裂叶子并创建新根（简化版本）
    std::unique_ptr<SearchNode> new_node;
    Key split_key;
    if (!sn_root_->Split(new_node, split_key))
        return false;
    // 创建新根并挂接两个子叶
    auto new_root = std::make_unique<SearchNode>(SearchNode::NodeType::Internal, SN_FANOUT);
    // 左子放在 children_[0]，右子放在 children_[1]，keys_[0] 为分裂键
    new_root->InsertChild(split_key, std::move(new_node));
    // 将原叶作为左子（需要将 sn_root_ 移到 new_root 的 children_[0]）
    // 为简化，重新构造 children：先把当前旧叶作为第0子
    // 这里用一种简单方式：先保存旧根指针，再重置 sn_root_
    std::unique_ptr<SearchNode> old_leaf = std::move(sn_root_);
    sn_root_ = std::move(new_root);
    // children_: 插入顺序为 [old_leaf, right_leaf]，对应 keys_ 中 split_key
    sn_root_->InsertChild(split_key, std::move(old_leaf));
    // 再尝试把 block 插入（叶容量已扩展）
    return sn_root_->InsertDataBlock(block->min_key(), block);
}

bool SBTree::sn_handle_split_(std::vector<SearchNode *> &path,
                              std::size_t child_idx,
                              std::unique_ptr<SearchNode> new_child,
                              Key split_key)
{
    // 预留：当前简化实现暂不使用 path 回溯；后续扩展时填充。
    (void)path;
    (void)child_idx;
    (void)new_child;
    (void)split_key;
    return false;
}
