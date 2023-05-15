#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
{
  bpm_=that.bpm_;
  page_=that.page_;
  is_dirty_=that.is_dirty_;
  that.bpm_=nullptr;
  that.page_=nullptr;
}

void BasicPageGuard::Drop()
{
  bpm_->UnpinPage(page_->GetPageId(),is_dirty_);
  bpm_=nullptr;
  page_=nullptr;
  is_dirty_=false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard &
{
  if(this!=&that) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_=false;
  }
  return *this;

}

BasicPageGuard::~BasicPageGuard()
{
  if(page_!=nullptr) {
    Drop();
  }
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard &
{
  if(this != &that)
  {
    guard_=std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop()
{
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard()
{
  guard_.Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {};

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard &
{
  if(this!=&that)
  {
    guard_=std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop()
{
  guard_.Drop();
}

WritePageGuard::~WritePageGuard()
{
  guard_.Drop();
}  // NOLINT

}  // namespace bustub
