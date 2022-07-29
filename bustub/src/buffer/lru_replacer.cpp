//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lck(latch_);
  if (frames_.empty()) {
    frame_id = nullptr;
    return false;
  }
  frame_id_t lru = frames_.front();
  frames_.pop_front();
  *frame_id = lru;
  frame_map_.erase(lru);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lck(latch_);
  if (frame_map_.find(frame_id) != frame_map_.end()) {
    std::list<frame_id_t>::iterator frame_it = frame_map_[frame_id];
    frame_map_.erase(frame_id);
    frames_.erase(frame_it);
  } else {
    return;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lck(latch_);
  if (frame_map_.find(frame_id) == frame_map_.end()) {
    while (frame_map_.size() >= capacity_) {
      frame_id_t lru = frames_.front();
      frames_.pop_front();
      frame_map_.erase(lru);
    }
    frames_.emplace_back(frame_id);
    frame_map_[frame_id] = --frames_.end();
  } else {
    return;
  }
}

size_t LRUReplacer::Size() { return frame_map_.size(); }

}  // namespace bustub
