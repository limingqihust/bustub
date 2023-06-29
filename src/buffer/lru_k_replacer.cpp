
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
  //  LOG_INFO("# lru-k raplacer:%ld %ld", num_frames, k);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  for (auto it = less_k_frame_.rbegin(); it != less_k_frame_.rend(); it++) {
    auto node = *it;
    if (it->is_evictable_) {  // find page can be evicted in less_k_frame
      *frame_id = it->fid_;
      //      LOG_INFO("# Evict frame : %d", *frame_id);
      curr_size_--;
      node_store_.erase(it->fid_);
      it = std::list<LRUKNode>::reverse_iterator(less_k_frame_.erase(std::next(it).base()));
      return true;
    }
  }

  for (auto it = cache_frame_.begin(); it != cache_frame_.end(); it++) {
    if (it->is_evictable_) {  // find page can be evicted in cache_frame_
      *frame_id = it->fid_;
      //      LOG_INFO("# Evict frame : %d", *frame_id);
      curr_size_--;
      node_store_.erase(it->fid_);
      cache_frame_.erase(it);
      return true;
    }
  }
  //  LOG_INFO("# Evict fail");
  return false;  // do not find page can be evicted
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  //  LOG_INFO("# RecordAccess frame: %d", frame_id);
  std::lock_guard<std::mutex> lock(latch_);
  current_timestamp_++;
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  if (node_store_.count(frame_id) == 0) {  // 第一次被访问
    auto new_node = LRUKNode();
    new_node.history_.push_front(current_timestamp_);
    new_node.k_timestamp_ = INF;
    new_node.cnt_ = 1;
    new_node.fid_ = frame_id;
    if (curr_size_ == replacer_size_) {  // 已满 驱逐一个
      frame_id_t evict_frame_id;
      Evict(&evict_frame_id);
      auto evict_node = node_store_[evict_frame_id];
      if (evict_node->k_timestamp_ == INF) {
        less_k_frame_.erase(evict_node);
      } else {
        cache_frame_.erase(evict_node);
      }
      node_store_.erase(evict_frame_id);
    }
    less_k_frame_.push_front(new_node);
    node_store_[frame_id] = less_k_frame_.begin();
  } else {
    auto cur_node = node_store_[frame_id];
    cur_node->cnt_++;
    cur_node->history_.push_front(current_timestamp_);
    if (cur_node->cnt_ == k_) {  // 访问次数超过k 从less_k_frame_迁移到cache_frame_
      cur_node->k_timestamp_ = cur_node->history_.back();
      auto value = cur_node->k_timestamp_;
      // find proper position
      auto iter_dst = std::find_if(cache_frame_.begin(), cache_frame_.end(),
                                   [value](const LRUKNode &i) { return i.k_timestamp_ > value; });
      cache_frame_.splice(iter_dst, less_k_frame_, cur_node);
    } else if (cur_node->cnt_ > k_) {  // 已经在cache_frame_ 调整位置
      cur_node->history_.pop_back();
      cur_node->k_timestamp_ = cur_node->history_.back();

      //      cache_frame_.splice(cache_frame_.begin(), less_k_frame_, cur_node);
      auto value = cur_node->k_timestamp_;
      auto iter_dst = std::find_if(cache_frame_.begin(), cache_frame_.end(),
                                   [value](const LRUKNode &i) { return i.k_timestamp_ > value; });
      cache_frame_.splice(iter_dst, cache_frame_, cur_node);
    }
    //    else {  // 在less_k_frame_中 调整位置
    //      less_k_frame_.splice(less_k_frame_.begin(), less_k_frame_, cur_node);
    //    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  //  if (set_evictable) {
  //    LOG_INFO("# set frame %d evictable", frame_id);
  //  } else {
  //    LOG_INFO("# set frame %d unevictable", frame_id);
  //  }

  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::runtime_error("the frame_id is more than replacer_size_");
  }
  auto cur_node = node_store_[frame_id];
  if (cur_node->is_evictable_ && !set_evictable) {  // 之前是可驱逐的 现在设为不可驱逐
    cur_node->is_evictable_ = false;
    curr_size_--;
  } else if (!cur_node->is_evictable_ && set_evictable) {  // 之前是不可驱逐的 现在设为可驱逐
    cur_node->is_evictable_ = true;
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  //  LOG_INFO("# Remove frame %d", frame_id);
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) == 0) {  // the frame is not found
    return;
  }
  if (!node_store_[frame_id]->is_evictable_) {  //  it is a non-evictable frame,
    throw std::runtime_error("attemp to remove a frame which is non-evictable");
  }
  auto cur_node = node_store_[frame_id];
  curr_size_--;
  node_store_.erase(frame_id);
  if (cur_node->k_timestamp_ != INF) {
    cache_frame_.erase(cur_node);
  } else {
    less_k_frame_.erase(cur_node);
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
