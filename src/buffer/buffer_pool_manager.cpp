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
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page *
{
  frame_id_t new_frame_id;
  if(!free_list_.empty())                   // pick frame from free list
  {
    new_frame_id=free_list_.front();
    free_list_.pop_front();
  }
  else                                      // pick frame from lru-k replacer
  {
    if(!replacer_->Evict(&new_frame_id)) {
      return nullptr;
    }
    page_table_.erase(pages_[new_frame_id].GetPageId());
  }

  auto new_page_id=AllocatePage();  // alloc a new page id
  if(pages_[new_frame_id].IsDirty())         // the frame has a dirty page,write it back and reset the page
  {
    disk_manager_->WritePage(pages_[new_frame_id].GetPageId(),pages_[new_frame_id].GetData()); // write old dirty page in the new frame to disk
    pages_[new_frame_id].ResetMemory();       // reset data
    pages_[new_frame_id].pin_count_=0;
  }
  pages_[new_frame_id].page_id_=new_page_id;
  pages_[new_frame_id].pin_count_++;
  replacer_->RecordAccess(new_frame_id);                       // record access of the frame
  replacer_->SetEvictable(new_frame_id,false);      // pin the new frame

  page_table_[new_page_id]=new_frame_id;
  *page_id=new_page_id;
  return pages_+new_frame_id;
  
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  frame_id_t frame_id;


  if(page_table_.count(page_id)!=0)             // the page is found in buffer pool
  {
    frame_id=page_table_[page_id];
    replacer_->SetEvictable(frame_id,false);
    replacer_->RecordAccess(frame_id);
    return pages_+frame_id;
  }
  // the page is not in buffer pool
  if(!free_list_.empty())                         // pick frame from free list
  {
    frame_id=free_list_.front();
    free_list_.pop_front();
    disk_manager_->ReadPage(page_id,pages_[frame_id].data_);
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id,false);
    return pages_+frame_id;
  }


  if(!replacer_->Evict(&frame_id)) {
    return nullptr;
  }
  // remove old page
  page_table_.erase(pages_[frame_id].GetPageId());
  page_table_[page_id]=frame_id;
  if(pages_[frame_id].IsDirty())            // if the frame has dirty page,write it back
  {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(),pages_[frame_id].GetData()); // write old dirty page in the new frame to disk
    pages_[frame_id].ResetMemory();     // reset data
    pages_[frame_id].pin_count_=0;
  }
  pages_[frame_id].pin_count_++;
  pages_[frame_id].page_id_=page_id;
  disk_manager_->ReadPage(page_id,pages_[frame_id].data_);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id,false);
  return pages_+frame_id;

}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool
{
  if(page_table_.count(page_id)==0)     // the page is not in buffer pool
  {
    return false;
  }
  frame_id_t frame_id=page_table_[page_id];
  if(pages_[frame_id].pin_count_<=0)
  {
    return false;
  }
  pages_[frame_id].is_dirty_=is_dirty;
  pages_[frame_id].pin_count_--;
  if(pages_[frame_id].pin_count_==0)
  {
    replacer_->SetEvictable(frame_id,true);
  }
  return true;

}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool
{

  if(page_table_.count(page_id)==0)
  {
    return false;
  }
  frame_id_t frame_id=page_table_[page_id];
  disk_manager_->WritePage(page_id,pages_[frame_id].data_);
  pages_[frame_id].is_dirty_=false;
  return true;

}

void BufferPoolManager::FlushAllPages()
{
  for(auto iter:page_table_)
  {
    FlushPage(iter.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool
{
  if(page_table_.count(page_id)==0)   // the page is not in buffer pool
  {
    return true;
  }

  frame_id_t frame_id=page_table_[page_id];
  if(pages_[frame_id].GetPinCount()!=0)
  {
    return false;
  }

  pages_[frame_id].ResetMemory();
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  return true;

}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
