//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

// LRU node

// LRUKNode::LRUKNode(frame_id_t frame_id, size_t timestamp) : fid_(frame_id) {
////  k_ = 1;
//  history_.push_front(timestamp);
//}

LRUKNode::LRUKNode(frame_id_t fid, size_t timestamp) {
  history_.emplace_front(timestamp);
  fid_ = fid;
}

// LRU replacer

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  if (curr_size_ <= 0) {
    return false;
  }
  //  frame_id_t frame_id_chosen = -1;
  auto now_timestamp = current_timestamp_;
  bool is_found = false;
  size_t max_k_distance = 0;
  size_t min_k_timestamp = 0;

  //  std::vector<frame_id_t> evict_candidates;

  for (auto &i : node_store_) {
    if (!i.second.is_evictable_) {
      continue;
    }
    is_found = true;
    size_t k_distance = 0;
    if (i.second.history_.size() < k_) {
      //      evict_candidates.push_back(i.second.fid_);
      k_distance = UINT32_MAX;
    } else {
      k_distance = now_timestamp - i.second.history_.back();
    }
    if (k_distance > max_k_distance) {
      max_k_distance = k_distance;
      min_k_timestamp = i.second.history_.back();
      *frame_id = i.second.fid_;
    } else if (k_distance == UINT32_MAX) {
      if (i.second.history_.back() < min_k_timestamp) {
        min_k_timestamp = i.second.history_.back();
        *frame_id = i.second.fid_;
      }
    }
  }
  if (!is_found) {
    return false;
  }
  node_store_.erase(*frame_id);
  curr_size_ -= 1;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // 先查看map中是否含有该frame_id的Node
  if (static_cast<size_t>(frame_id) >= replacer_size_ || frame_id < 0) {
    return;
  }
  std::unique_lock<std::mutex> lock(latch_);
  auto node_iter = node_store_.find(frame_id);
  if (node_iter == node_store_.end()) {
    // 不存在，则创建一个
    node_store_.insert(std::make_pair(frame_id, LRUKNode(frame_id, current_timestamp_++)));
  } else {
    if (node_iter->second.history_.size() == k_) {
      node_iter->second.history_.pop_back();
    }
    // 存在，在history中增加一个时间戳
    node_iter->second.history_.push_front(current_timestamp_++);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (static_cast<size_t>(frame_id) >= replacer_size_ || frame_id < 0) {
    return;
  }
  std::unique_lock<std::mutex> lock(latch_);
  auto node_iter = node_store_.find(frame_id);
  if (node_iter != node_store_.end()) {
    if (set_evictable && !node_iter->second.is_evictable_) {
      curr_size_ += 1;
    } else if (!set_evictable && node_iter->second.is_evictable_) {
      curr_size_ -= 1;
    }
    node_iter->second.is_evictable_ = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (static_cast<size_t>(frame_id) >= replacer_size_ || frame_id < 0) {
    return;
  }
  std::unique_lock<std::mutex> lock(latch_);
  auto node_iter = node_store_.find(frame_id);
  if (node_iter != node_store_.end()) {
    if (!node_iter->second.is_evictable_) {
      throw Exception("frame is not evictable");
    }
    //    BUSTUB_ASSERT(node_iter->second.is_evictable_, "frame is not evictable");
    node_store_.erase(node_iter);
    curr_size_ -= 1;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::unique_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
