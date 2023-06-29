//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"
namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  LOG_INFO("# new BufferPoolManager : pool_size :%ld replacer_k : %ld", pool_size, replacer_k);
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  LOG_INFO("# NewPage : ");
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t new_frame_id;
  if (!free_list_.empty()) {  // pick frame from free list
    new_frame_id = free_list_.front();
    LOG_INFO("# NewPage : pick free frame %d", new_frame_id);
    free_list_.pop_front();
  } else {  // pick frame from lru-k replacer
    if (!replacer_->Evict(&new_frame_id)) {
      LOG_INFO("# NewPage : fail,evict error");
      return nullptr;
    }
    LOG_INFO("# NewPage : evict frame %d", new_frame_id);
    page_table_.erase(pages_[new_frame_id].GetPageId());
  }

  auto new_page_id = AllocatePage();  // alloc a new page id

  // the frame has a dirty page,write it back and reset the page
  if (pages_[new_frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[new_frame_id].GetPageId(),
                             pages_[new_frame_id].GetData());  // write old dirty page in the new frame to disk
    pages_[new_frame_id].ResetMemory();                        // reset data
    pages_[new_frame_id].pin_count_ = 0;
    pages_[new_frame_id].is_dirty_ = false;
  }
  pages_[new_frame_id].page_id_ = new_page_id;
  pages_[new_frame_id].pin_count_ = 1;

  replacer_->RecordAccess(new_frame_id);         // record access of the frame
  replacer_->SetEvictable(new_frame_id, false);  // pin the new frame

  page_table_[new_page_id] = new_frame_id;
  *page_id = new_page_id;
  LOG_INFO("# NewPage : page_id %d in frame_id %d", new_page_id, new_frame_id);
  return pages_ + new_frame_id;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  LOG_INFO("# FetchPage : page_id %d", page_id);
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_.count(page_id) != 0) {  // 该page已经在buffer pool中
    frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id);
    LOG_INFO("# FetchPage : page %d is found in buffer pool frame %d", page_id, frame_id);
    return pages_ + frame_id;
  }
  // 该page不在buffer pool中 需要将其从磁盘中取出来
  if (!free_list_.empty()) {  // 首先看有没有空闲的frame
    frame_id = free_list_.front();
    free_list_.pop_front();
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ = 0;
    pages_[frame_id].is_dirty_ = false;
    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    LOG_INFO("# FetchPage : page %d is not in buffer pool,pick frame %d from free list", page_id, frame_id);
    return pages_ + frame_id;
  }
  // 没有空闲的frame 需要驱逐一个
  if (!replacer_->Evict(&frame_id)) {
    LOG_INFO("# FetchPage : page %d is not in buffer pool,but can not evict a frame", page_id);
    return nullptr;
  }

  LOG_INFO("# FetchPage : page %d is not in buffer pool,evict page in frame %d", page_id, frame_id);
  if (pages_[frame_id].IsDirty()) {  // if the frame has dirty page,write it back
    disk_manager_->WritePage(pages_[frame_id].GetPageId(),
                             pages_[frame_id].GetData());  // write old dirty page in the new frame to disk
    pages_[frame_id].ResetMemory();                        // reset data
    pages_[frame_id].pin_count_ = 0;
  }
  page_table_.erase(pages_[frame_id].GetPageId());
  page_table_[page_id] = frame_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return pages_ + frame_id;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  LOG_INFO("# UnpinPage : page_id %d is dirty %d", page_id, (int)is_dirty);
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {  // the page is not in buffer pool
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ <= 0) {
    return false;
  }
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }

  pages_[frame_id].pin_count_--;
  LOG_INFO("# UnPinPage : after UnPinPage pin_count_ of page_id %d is %d", page_id, pages_[frame_id].pin_count_);
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  LOG_INFO("# FlushPage : page_id %d", page_id);
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  LOG_INFO("# FlushAllPages:");
  for (auto iter : page_table_) {
    FlushPage(iter.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  LOG_INFO("# DeletePage : page_id %d", page_id);
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {  // the page is not in buffer pool
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() != 0) {
    return false;
  }

  pages_[frame_id].ResetMemory();
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  LOG_INFO("# AlloctePage : new_page_id %d", (page_id_t)next_page_id_);
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  LOG_INFO("# FetchPageBasic : page_id %d", page_id);
  auto page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  LOG_INFO("# FetchPageRead : page_id %d", page_id);
  auto page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  LOG_INFO("# FetchPageWrite : page_id %d", page_id);
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  LOG_INFO("# NewPageGuarded : ");
  auto page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
