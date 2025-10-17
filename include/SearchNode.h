#pragma once
#include <vector>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include "KVPair.h"

class DataBlock;

// -----------------------------------------------------------------------------
// SearchNode: 自底向上插入/分裂的简化索引节点
// -----------------------------------------------------------------------------
class SearchNode
{
public:
    enum class NodeType
    {
        Leaf,
        Internal
    };

    explicit SearchNode(NodeType type, std::size_t capacity)
        : type_(type), capacity_(capacity), size_(0)
    {
        keys_.reserve(capacity);
        if (type_ == NodeType::Leaf)
            data_blocks_.reserve(capacity);
        else
            children_.reserve(capacity + 1);
    }

    // 叶节点插入 {min_key, DataBlock*}，按 min_key 有序
    bool InsertDataBlock(Key min_key, DataBlock *ptr)
    {
        if (type_ != NodeType::Leaf)
            return false;
        std::lock_guard<std::mutex> lk(write_mutex_);
        if (IsFull())
            return false;
        auto it = lower_bound_(keys_, min_key);
        std::size_t pos = static_cast<std::size_t>(it - keys_.begin());
        keys_.insert(it, min_key);
        data_blocks_.insert(data_blocks_.begin() + pos, ptr);
        ++size_;
        return true;
    }

    // 内部节点插入子节点（按 max_key 有序）
    bool InsertChild(Key max_key, std::unique_ptr<SearchNode> child)
    {
        if (type_ != NodeType::Internal || !child)
            return false;
        std::lock_guard<std::mutex> lk(write_mutex_);
        if (IsFull())
            return false;
        auto it = lower_bound_(keys_, max_key);
        std::size_t pos = static_cast<std::size_t>(it - keys_.begin());
        keys_.insert(it, max_key);
        children_.insert(children_.begin() + pos + 1, std::move(child));
        ++size_;
        return true;
    }

    // 节点分裂：返回新节点与分裂键
    bool Split(std::unique_ptr<SearchNode> &new_node_out, Key &split_key)
    {
        std::lock_guard<std::mutex> lk(write_mutex_);
        if (!IsFull())
            return false;
        const std::size_t mid = size_ / 2;
        new_node_out = std::make_unique<SearchNode>(type_, capacity_);
        if (type_ == NodeType::Leaf)
        {
            new_node_out->keys_.assign(keys_.begin() + mid, keys_.end());
            new_node_out->data_blocks_.assign(data_blocks_.begin() + mid, data_blocks_.end());
            new_node_out->size_ = new_node_out->keys_.size();
            split_key = new_node_out->keys_.front();

            keys_.erase(keys_.begin() + mid, keys_.end());
            data_blocks_.erase(data_blocks_.begin() + mid, data_blocks_.end());
            size_ = keys_.size();
        }
        else
        {
            // Internal: 提取中间键作为分裂键
            split_key = keys_[mid];
            new_node_out->keys_.assign(keys_.begin() + mid + 1, keys_.end());
            new_node_out->children_.assign(children_.begin() + mid + 1, children_.end());
            new_node_out->size_ = new_node_out->keys_.size();

            keys_.erase(keys_.begin() + mid, keys_.end());
            children_.erase(children_.begin() + mid + 1, children_.end());
            size_ = keys_.size();
        }
        return true;
    }

    // 查找
    DataBlock *FindDataBlock(Key k) const
    {
        if (type_ != NodeType::Leaf)
            return nullptr;
        auto it = upper_bound_(keys_, k);
        if (it == keys_.begin())
            return nullptr;
        std::size_t pos = static_cast<std::size_t>((it - keys_.begin()) - 1);
        return data_blocks_[pos];
    }

    SearchNode *FindChild(Key k) const
    {
        if (type_ != NodeType::Internal)
            return nullptr;
        auto it = upper_bound_(keys_, k);
        std::size_t pos = static_cast<std::size_t>(it - keys_.begin());
        if (pos >= children_.size())
            return nullptr;
        return children_[pos].get();
    }

    // 只读信息
    bool IsFull() const { return size_ >= capacity_; }
    NodeType Type() const { return type_; }

private:
    static std::vector<Key>::iterator lower_bound_(std::vector<Key> &arr, Key k)
    {
        return std::lower_bound(arr.begin(), arr.end(), k);
    }
    static std::vector<Key>::const_iterator upper_bound_(const std::vector<Key> &arr, Key k)
    {
        return std::upper_bound(arr.begin(), arr.end(), k);
    }

private:
    NodeType type_;
    std::size_t capacity_;
    std::size_t size_;
    std::vector<Key> keys_;
    std::vector<DataBlock *> data_blocks_;                         // 叶子
    std::vector<std::unique_ptr<SearchNode>> children_;           // 内部
    mutable std::mutex write_mutex_;
};


