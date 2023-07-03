#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
//  LOG_INFO("# BasicPageGuard::move : page_id %d", that.page_->GetPageId());
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
//  LOG_INFO("# BasicPageGuard::Drop : ");
  if (page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    bpm_ = nullptr;
    page_ = nullptr;
    is_dirty_ = false;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
//  LOG_INFO("# BasicPageGuard::operator= : page_id %d", that.page_->GetPageId());
  if (this != &that) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
//  LOG_INFO("# BasicPageGuard::~ : ");
  Drop();
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {
//  LOG_INFO("# ReadPageGuard::move : ");
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
//  LOG_INFO("# ReadPageGuard::operator= : ");
  if (this != &that) {
    guard_.page_->RUnlatch();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
//  LOG_INFO("# ReadPageGuard::Drop : ");
  guard_.page_->RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
//  LOG_INFO("# ReadPageGuard::~ : ");
  if (guard_.page_ != nullptr) {
    Drop();
  }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {
//  LOG_INFO("# WritePageGuard::move : ");
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
//  LOG_INFO("# WritePageGuard::operator= : ");
  if (this != &that) {
    guard_.page_->WUnlatch();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
//  LOG_INFO("# WritePageGuard::Drop : ");
  guard_.page_->WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
//  LOG_INFO("# WritePageGuard::~ : ");
  if (guard_.page_ != nullptr) {
    Drop();
  }
}  // NOLINT

}  // namespace bustub
