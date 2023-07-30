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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t delete_frame_id = 0;
  bool is_found = false;
  size_t distance = 0;
  size_t last_visit_time = 0;
  for (const auto &iter : node_store_) {
    auto &node = iter.second;
    if (node.is_evictable_) {
      size_t tmp_distance = 0;
      size_t tmp_last_visit_time = node.history_.back();
      if (node.history_.size() == k_) {
        tmp_distance = current_timestamp_ - node.history_.back();
      } else {
        tmp_distance = static_cast<size_t>(-1);
      }
      if (tmp_distance > distance || (tmp_distance == distance && tmp_last_visit_time < last_visit_time)) {
        distance = tmp_distance;
        last_visit_time = tmp_last_visit_time;
        delete_frame_id = iter.first;
        is_found = true;
      }
    }
  }
  if (!is_found) {
    return false;
  }
  *frame_id = delete_frame_id;
  node_store_.erase(delete_frame_id);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> guard(latch_);
  if (node_store_.find(frame_id) != node_store_.end() && node_store_[frame_id].fid_ == frame_id) {
    auto &node = node_store_[frame_id];
    node.history_.push_front(current_timestamp_++);
    if (node.history_.size() > k_) {
      node.history_.pop_back();
    }
  } else {
    LRUKNode node(frame_id);
    node.history_.push_front(current_timestamp_++);
    node_store_[frame_id] = std::move(node);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  if (node_store_.find(frame_id) != node_store_.end() && node_store_[frame_id].fid_ == frame_id) {
    auto &node = node_store_[frame_id];
    if (set_evictable == node.is_evictable_) {
      return;
    }
    node.is_evictable_ = set_evictable;
    if (set_evictable) {
      curr_size_++;
    } else {
      curr_size_--;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  auto &node = node_store_[frame_id];
  if (!node.is_evictable_) {
    // throw
    return;
  }
  node_store_.erase(frame_id);
  curr_size_--;
}

// Get evictable frame nums
auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
