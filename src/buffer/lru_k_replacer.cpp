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

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::priority_queue<LRUKNode, std::vector<LRUKNode>, Compare> node_queue;
  for (auto it = node_store_.begin(); it != node_store_.end(); it++) {
    LRUKNode node = it->second;
    if (node.GetEvictable()) {
      node_queue.push(node);
    }
  }
  if (node_queue.empty()) {
    return std::nullopt;
  }
  // TODO(namcvh) : memory leaks ? should destroy evicted node
  LRUKNode candidate_evict_node = node_queue.top();
  node_queue.pop();
  frame_id_t frame_id = candidate_evict_node.GetFrameId();
  node_store_.erase(frame_id);
  curr_size_--;
  return frame_id;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_ && frame_id > 0,
                "Invalid: frame_id larger than replacer_size_");
  current_timestamp_++;
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    // Create a new entry for access history if frame id has not been seen before.
    node_store_.emplace(frame_id, LRUKNode(frame_id, k_, current_timestamp_));
  } else {
    // Record the event that the given frame id is accessed at current timestamp.
    node_store_[frame_id].UpdateHistory(current_timestamp_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) <= replacer_size_ && frame_id > 0,
                "Invalid: frame_id larger than replacer_size_");
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  LRUKNode node = node_store_[frame_id];
  if (node.GetEvictable() != set_evictable) {
    int i = (node.GetEvictable() == true) ? -1 : 1;
    if (curr_size_ + i >= 0) curr_size_ += i;
  }
  node_store_[frame_id].SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (frame_id < 0 || node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  LRUKNode node = node_store_[frame_id];
  if (node.GetEvictable() == false) {
    throw std::logic_error("Cannot remove a non-evictable frame: " + std::to_string(frame_id));
  }
  node_store_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
