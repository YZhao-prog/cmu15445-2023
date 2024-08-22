
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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  is_accessible_.resize(num_frames + 1);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  const std::lock_guard<std::mutex> guard(latch_);

  *frame_id = 0;

  if (history_map_.empty() && cache_map_.empty()) {
    return false;
  }

  // first find frame which need evict in history list
  auto it = history_list_.end();
  while (it != history_list_.begin()) {
    it--;
    if (!is_accessible_[*it]) {
      continue;
    }
    // id of frame that is evicted.
    *frame_id = *it;
    // update state
    curr_size_--;
    is_accessible_[*it] = false;
    use_count_[*it] = 0;
    // evict
    history_map_.erase(*it);
    history_list_.erase(it);
    return true;
  }

  it = cache_list_.end();
  while (it != cache_list_.begin()) {
    it--;
    if (!is_accessible_[*it]) {
      continue;
    }
    *frame_id = *it;
    // update state
    curr_size_--;
    is_accessible_[*it] = false;
    use_count_[*it] = 0;
    // evict
    cache_map_.erase(*it);
    cache_list_.erase(it);
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  const std::lock_guard<std::mutex> guard(latch_);
  // check valid
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }

  use_count_[frame_id]++;

  if (use_count_[frame_id] == k_) {
    // take frame from history to cache
    if (history_map_.count(frame_id) != 0) {
      auto it = history_map_[frame_id];
      history_list_.erase(it);
      history_map_.erase(frame_id);
    }
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } else if (use_count_[frame_id] > k_) {
    // resort the cache
    if (cache_map_.count(frame_id) != 0) {
      auto it = cache_map_[frame_id];
      cache_list_.erase(it);
    }
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } else {
    // use count < k, change history
    if (history_map_.count(frame_id) == 0) {
      history_list_.push_front(frame_id);
      history_map_[frame_id] = history_list_.begin();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  const std::lock_guard<std::mutex> guard(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }

  if (use_count_[frame_id] == 0) {
    return;
  }

  // If a frame was previously evictable and is to be set to non-evictable, then size should decrement.
  if (is_accessible_[frame_id] && !set_evictable) {
    curr_size_--;
  }
  // If a frame was previously non-evictable and is to be set to evictable, then size should increment.
  if (!is_accessible_[frame_id] && set_evictable) {
    curr_size_++;
  }

  is_accessible_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  const std::lock_guard<std::mutex> guard(latch_);
  // not found
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  // cannot remove
  if (!is_accessible_[frame_id]) {
    return;
  }

  auto removal = use_count_[frame_id];
  if (removal < k_) {
    auto it = history_map_[frame_id];
    history_list_.erase(it);
    history_map_.erase(frame_id);
  } else {
    auto it = cache_map_[frame_id];
    cache_list_.erase(it);
    cache_map_.erase(frame_id);
  }
  // update state
  use_count_[frame_id] = 0;
  is_accessible_[frame_id] = false;
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
